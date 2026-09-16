//! Real adapter acceptance test, without Spark or ldk-server processes.
//! Run with LDK_TEST_BITCOIND / LDK_TEST_ELECTRS pointing to the native tools.
use super::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
struct Chain {
    rpc: String,
    client: reqwest::Client,
}
impl Chain {
    async fn rpc(&self, method: &str, params: Value) -> Result<Value, String> {
        let response: Value = self.client.post(&self.rpc).basic_auth("testutil", Some("testutilpassword"))
            .json(&json!({"jsonrpc": "1.0", "id": "embedded-test", "method": method, "params": params}))
            .send().await.map_err(|e| e.to_string())?.json().await.map_err(|e| e.to_string())?;
        if !response["error"].is_null() {
            return Err(response["error"].to_string());
        }
        Ok(response["result"].clone())
    }
    async fn mine(&self, count: u32) {
        let address = self.rpc("getnewaddress", json!([])).await.unwrap();
        self.rpc("generatetoaddress", json!([count, address]))
            .await
            .unwrap();
    }
}
fn embedded(client: &LdkClient) -> &Arc<EmbeddedNode> {
    match client {
        LdkClient::Embedded(node) => node,
        _ => panic!("expected embedded node"),
    }
}
async fn next_payment_event(client: &LdkClient) -> ldk_node::Event {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let node = embedded(client);
            let event = node.node.next_event_async().await;
            if matches!(
                event,
                ldk_node::Event::PaymentClaimable { .. }
                    | ldk_node::Event::PaymentReceived { .. }
                    | ldk_node::Event::PaymentFailed { .. }
                    | ldk_node::Event::PaymentSuccessful { .. }
            ) {
                return event;
            }
            node.call(|n| n.event_handled().map_err(|e| e.to_string()))
                .await
                .unwrap();
        }
    })
    .await
    .expect("payment event timed out")
}
async fn acknowledge(client: &LdkClient) {
    embedded(client)
        .call(|n| n.event_handled().map_err(|e| e.to_string()))
        .await
        .unwrap();
}
async fn wait_status(
    client: &LdkClient,
    send: &LightningSend,
    status: types::PaymentStatus,
) -> types::Payment {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if let Some(payment) = client.lookup(send).await.unwrap() {
                if payment.status == status as i32 {
                    return payment;
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("payment status timed out")
}
fn send(invoice: String, kind: SendKind, expected_id: String) -> LightningSend {
    LightningSend {
        request_id: uuid::Uuid::new_v4().to_string(),
        owner: "test".into(),
        outbound_transfer_id: "test".into(),
        invoice,
        amount_sats: 1_000,
        amount_override: (kind == SendKind::Bolt12).then_some(1_000),
        kind,
        expected_id,
        payment_id: None,
        status: SendStatus::Submitting,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires native Bitcoin Core and Esplora electrs binaries"]
async fn embedded_regtest_payments_and_restart_esplora() {
    payments_and_restart(LdkChainSource::Esplora).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a native Bitcoin Core binary"]
async fn embedded_regtest_payments_and_restart_bitcoind() {
    payments_and_restart(LdkChainSource::Bitcoind).await;
}

async fn payments_and_restart(source: LdkChainSource) {
    let chain_dir = TestDir::new();
    let bitcoin_dir = chain_dir.0.join("bitcoin");
    std::fs::create_dir(&bitcoin_dir).unwrap();
    let rpc_port = port();
    let bitcoin_log = File::create(chain_dir.0.join("bitcoin.log")).unwrap();
    let bitcoind = std::env::var("LDK_TEST_BITCOIND")
        .unwrap_or_else(|_| ".regtest/native-tools/bitcoin-29.0/bin/bitcoind".into());
    let _bitcoin = Process(
        Command::new(bitcoind)
            .args([
                "-regtest",
                "-server",
                "-listen=0",
                "-txindex=1",
                "-fallbackfee=0.0002",
                "-rpcuser=testutil",
                "-rpcpassword=testutilpassword",
                "-rpcbind=127.0.0.1",
                "-rpcallowip=127.0.0.1",
                &format!("-rpcport={rpc_port}"),
                &format!("-datadir={}", bitcoin_dir.display()),
            ])
            .stdout(Stdio::from(bitcoin_log.try_clone().unwrap()))
            .stderr(Stdio::from(bitcoin_log))
            .spawn()
            .unwrap(),
    );
    let chain = Chain {
        rpc: format!("http://127.0.0.1:{rpc_port}"),
        client: reqwest::Client::new(),
    };
    tokio::time::timeout(Duration::from_secs(30), async {
        while chain.rpc("getblockchaininfo", json!([])).await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    chain.rpc("createwallet", json!(["miner"])).await.unwrap();
    chain.mine(101).await;

    let (_electrs, esplora) = if source == LdkChainSource::Esplora {
        let http_port = port();
        let electrs_log = File::create(chain_dir.0.join("electrs.log")).unwrap();
        let electrs = std::env::var("LDK_TEST_ELECTRS")
            .unwrap_or_else(|_| ".regtest/native-tools/electrs".into());
        let electrs = Process(
            Command::new(electrs)
                .args([
                    "--network=regtest",
                    "--jsonrpc-import",
                    "--cookie=testutil:testutilpassword",
                    &format!("--daemon-rpc-addr=127.0.0.1:{rpc_port}"),
                    &format!("--daemon-dir={}", bitcoin_dir.display()),
                    &format!("--db-dir={}", chain_dir.0.join("electrs").display()),
                    &format!("--http-addr=127.0.0.1:{http_port}"),
                    &format!("--electrum-rpc-addr=127.0.0.1:{}", port()),
                    &format!("--monitoring-addr=127.0.0.1:{}", port()),
                ])
                .stdout(Stdio::from(electrs_log.try_clone().unwrap()))
                .stderr(Stdio::from(electrs_log))
                .spawn()
                .unwrap(),
        );
        let esplora = format!("http://127.0.0.1:{http_port}");
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if let Ok(response) = chain
                    .client
                    .get(format!("{esplora}/blocks/tip/height"))
                    .send()
                    .await
                {
                    if response.text().await.unwrap_or_default() == "101" {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .unwrap();
        (Some(electrs), esplora)
    } else {
        (None, String::new())
    };

    let sender_dir = TestDir::new();
    let receiver_dir = TestDir::new();
    let mut sender_config = config(&sender_dir);
    sender_config.ldk_node_esplora_url = esplora.clone();
    sender_config.ldk_node_listen_addr = format!("127.0.0.1:{}", port());
    let mut receiver_config = config(&receiver_dir);
    receiver_config.ldk_node_esplora_url = esplora;
    receiver_config.ldk_node_listen_addr = format!("127.0.0.1:{}", port());
    for config in [&mut sender_config, &mut receiver_config] {
        config.ldk_node_chain_source = source;
        config.ldk_node_bitcoind_rpc_host = "127.0.0.1".into();
        config.ldk_node_bitcoind_rpc_port = rpc_port;
        config.ldk_node_bitcoind_rpc_user = "testutil".into();
        config.ldk_node_bitcoind_rpc_password = "testutilpassword".into();
    }
    // Exercise file precedence (including a trailing newline) on real RPC calls.
    let password_file = receiver_dir.0.join("rpc-password");
    std::fs::write(&password_file, "testutilpassword\n").unwrap();
    receiver_config.ldk_node_bitcoind_rpc_password_file = password_file.to_str().unwrap().into();
    receiver_config.ldk_node_bitcoind_rpc_password = "wrong-password".into();
    let (sender, _) = LdkClient::connect(&sender_config, bitcoin::Network::Regtest)
        .await
        .unwrap();
    let (receiver, receiver_id) = LdkClient::connect(&receiver_config, bitcoin::Network::Regtest)
        .await
        .unwrap();
    for client in [&sender, &receiver] {
        let address = embedded(client)
            .call(|node| {
                node.onchain_payment()
                    .new_address()
                    .map(|a| a.to_string())
                    .map_err(|e| e.to_string())
            })
            .await
            .unwrap();
        chain
            .rpc("sendtoaddress", json!([address, 1]))
            .await
            .unwrap();
    }
    chain.mine(6).await;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            for client in [&sender, &receiver] {
                embedded(client)
                    .call(|n| n.sync_wallets().map_err(|e| e.to_string()))
                    .await
                    .unwrap();
            }
            if [&sender, &receiver].iter().all(|c| {
                embedded(c)
                    .node
                    .list_balances()
                    .spendable_onchain_balance_sats
                    >= 100_000_000
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .unwrap();
    let peer_id = receiver_id.parse().unwrap();
    let peer_addr = receiver_config.ldk_node_listen_addr.parse().unwrap();
    embedded(&sender)
        .call(move |node| {
            node.open_channel(peer_id, peer_addr, 1_000_000, Some(400_000_000), None)
                .map(|_| ())
                .map_err(|e| e.to_string())
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), async {
        while chain
            .rpc("getrawmempool", json!([]))
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .unwrap();
    chain.mine(6).await;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            for client in [&sender, &receiver] {
                embedded(client)
                    .call(|n| n.sync_wallets().map_err(|e| e.to_string()))
                    .await
                    .unwrap();
            }
            if [&sender, &receiver]
                .iter()
                .all(|c| embedded(c).node.list_channels().iter().any(|c| c.is_usable))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    })
    .await
    .unwrap();

    // Restart with an unacknowledged hold. Replay must preserve its claim ID.
    let preimage = [42u8; 32];
    let hash = hex::encode(Sha256::digest(preimage));
    let invoice = receiver
        .bolt11_receive_for_hash(Bolt11ReceiveForHashRequest {
            amount_msat: Some(1_000_000),
            description: super::super::super::description_of("embedded restart"),
            expiry_secs: 3600,
            payment_hash: hash.clone(),
        })
        .await
        .unwrap();
    let intent = send(invoice.invoice, SendKind::Bolt11, hash.clone());
    let payment_id = sender.submit(&intent).await.unwrap();
    assert_eq!(payment_id, hash);
    let event = next_payment_event(&receiver).await;
    let claim_id = match &event {
        ldk_node::Event::PaymentClaimable {
            payment_id,
            payment_hash,
            ..
        } => {
            assert_eq!(payment_hash.to_string(), hash);
            payment_id.to_string()
        }
        _ => panic!("expected claimable: {event:?}"),
    };
    receiver.stop().await.unwrap();
    drop(receiver);
    receiver_config.ldk_node_seed_required = true;
    let (receiver, restarted_id) = LdkClient::connect(&receiver_config, bitcoin::Network::Regtest)
        .await
        .unwrap();
    assert_eq!(restarted_id, receiver_id);
    // Reconnect explicitly instead of waiting for LDK's 60-second peer timer.
    let peer_id = receiver_id.parse().unwrap();
    let peer_addr = receiver_config.ldk_node_listen_addr.parse().unwrap();
    embedded(&sender)
        .call(move |node| {
            node.connect(peer_id, peer_addr, true)
                .map_err(|e| e.to_string())
        })
        .await
        .unwrap();
    assert_eq!(next_payment_event(&receiver).await, event);
    assert_eq!(
        receiver
            .get_payment_details(GetPaymentDetailsRequest {
                payment_id: claim_id.clone()
            })
            .await
            .unwrap()
            .payment
            .unwrap()
            .status,
        types::PaymentStatus::Pending as i32
    );
    receiver
        .claim_receive(&claim_id, 1_000_000, &hex::encode(preimage))
        .await
        .unwrap();
    acknowledge(&receiver).await;
    let succeeded = wait_status(&sender, &intent, types::PaymentStatus::Succeeded).await;
    assert!(
        matches!(succeeded.kind.unwrap().kind.unwrap(), types::payment_kind::Kind::Bolt11(p) if p.preimage == Some(hex::encode(preimage)))
    );
    assert!(matches!(
        next_payment_event(&receiver).await,
        ldk_node::Event::PaymentReceived { .. }
    ));
    acknowledge(&receiver).await;

    // Failing a hold by backend ID gives the sender an authoritative failure.
    let hash = hex::encode(Sha256::digest([43u8; 32]));
    let invoice = receiver
        .bolt11_receive_for_hash(Bolt11ReceiveForHashRequest {
            amount_msat: Some(1_000_000),
            description: None,
            expiry_secs: 3600,
            payment_hash: hash.clone(),
        })
        .await
        .unwrap();
    let intent = send(invoice.invoice, SendKind::Bolt11, hash);
    sender.submit(&intent).await.unwrap();
    let event = next_payment_event(&receiver).await;
    let id = match event {
        ldk_node::Event::PaymentClaimable { payment_id, .. } => payment_id.to_string(),
        _ => panic!("expected claimable"),
    };
    receiver.fail_receive(&id).await.unwrap();
    acknowledge(&receiver).await;
    wait_status(&sender, &intent, types::PaymentStatus::Failed).await;

    // Offer recovery has no stored payment ID: it must use offer ID + payer note.
    let offer = receiver
        .bolt12_receive(Bolt12ReceiveRequest {
            amount_msat: Some(1_000_000),
            description: "embedded offer".into(),
            expiry_secs: Some(3600),
            quantity: None,
        })
        .await
        .unwrap();
    assert_eq!(
        receiver.offer_id(&offer.offer).await.unwrap(),
        offer.offer_id
    );
    let intent = send(offer.offer, SendKind::Bolt12, offer.offer_id.clone());
    let id = sender.submit(&intent).await.unwrap();
    let payment = wait_status(&sender, &intent, types::PaymentStatus::Succeeded).await;
    assert_eq!(payment.payment_id, id);
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let event = next_payment_event(&receiver).await;
            let mapped = embedded(&receiver)
                .call(move |node| map_event(event, |id| event_payment(node, id)))
                .await
                .unwrap()
                .unwrap();
            acknowledge(&receiver).await;
            if let LnEvent::InboundBolt12Received {
                offer_id,
                preimage,
                amount_msat,
                ..
            } = mapped
            {
                assert_eq!(offer_id, offer.offer_id);
                assert!(preimage.is_some());
                // BOLT12's blinded route can deliver more than the requested
                // amount. Settlement requires at least the quoted amount.
                assert!(amount_msat.is_some_and(|amount| amount >= 1_000_000));
                break;
            }
            // A restarted node can replay earlier terminal events. The SSP's
            // durable handlers accept those duplicates before the offer event.
        }
    })
    .await
    .expect("BOLT12 receive event timed out");
    receiver.stop().await.unwrap();
    sender.stop().await.unwrap();
}
