//! Single-use deposits and repeated leaf swaps through Breez payments.
use super::*;
pub async fn run(client: &Client, config: &TestConfig, ldk: &LdkClient) -> Result<()> {
    let a = connect_wallet(
        client,
        config,
        "swap-a",
        "http://127.0.0.1:5000",
        ldk.clone(),
        0x0d,
    )
    .await?;
    let b = connect_wallet(
        client,
        config,
        "swap-b",
        "http://127.0.0.1:5000",
        ldk.clone(),
        0x0e,
    )
    .await?;
    let result = exercise(client, config, &a, &b).await;
    let da = a.sdk.disconnect().await;
    let db = b.sdk.disconnect().await;
    result?;
    da?;
    db?;
    Ok(())
}
async fn send(sender: &Wallet, receiver: &Wallet, amount: u64) -> Result<()> {
    let address = receiver
        .sdk
        .receive_payment(ReceivePaymentRequest {
            payment_method: ReceivePaymentMethod::SparkAddress,
        })
        .await?
        .payment_request;
    let prepared = sender
        .sdk
        .prepare_send_payment(PrepareSendPaymentRequest {
            payment_request: PaymentRequest::Input { input: address },
            amount: Some(amount.into()),
            token_identifier: None,
            conversion_options: None,
            fee_policy: None,
        })
        .await?;
    sender
        .sdk
        .send_payment(SendPaymentRequest {
            prepare_response: prepared,
            options: None,
            idempotency_key: None,
        })
        .await?;
    Ok(())
}
async fn exercise(client: &Client, config: &TestConfig, a: &Wallet, b: &Wallet) -> Result<()> {
    fund_ssp(client, config, a.ssp_url, 114_000).await?;
    let ssp_before = ssp_available_balance(client, config, a.ssp_url).await?;
    let address = a.sdk.generate_single_use_deposit_address().await?;
    let txid = send_regtest_deposit(client, config, &address, 100_000).await?;
    let miner = bitcoin_rpc(client, config, "getnewaddress", json!([])).await?;
    bitcoin_rpc(client, config, "generatetoaddress", json!([3, miner])).await?;
    let transaction = bitcoin_rpc(client, config, "getrawtransaction", json!([txid, true])).await?;
    let vout = transaction["vout"]
        .as_array()
        .context("missing outputs")?
        .iter()
        .find(|v| v["scriptPubKey"]["address"] == address)
        .context("deposit output missing")?["n"]
        .as_u64()
        .context("missing vout")? as u32;
    poll(
        "single-use operator deposit claim",
        config.timeout,
        || async {
            a.sdk
                .claim_single_use_deposit(
                    transaction["hex"]
                        .as_str()
                        .context("missing raw transaction")?,
                    vout,
                )
                .await?;
            Ok(())
        },
    )
    .await?;
    poll("Breez single-use deposit", config.timeout, || {
        exact_balance(a, 100_000)
    })
    .await?;
    send(a, b, 50_000)
        .await
        .context("first partial Spark payment")?;
    poll("first swap receiver", config.timeout, || {
        exact_balance(b, 50_000)
    })
    .await?;
    poll("first swap sender", config.timeout, || {
        exact_balance(a, 50_000)
    })
    .await?;
    poll("first swap SSP reclaim", config.timeout, || async {
        ensure!(
            ssp_available_balance(client, config, a.ssp_url).await? == ssp_before,
            "swap input not reclaimed"
        );
        Ok(())
    })
    .await?;
    restart_ssp(client, config, a.ssp_url).await?;
    send(b, a, 13_000)
        .await
        .context("partial Spark payment after restart")?;
    poll("repeated swap receiver", config.timeout, || {
        exact_balance(a, 63_000)
    })
    .await?;
    poll("repeated swap sender", config.timeout, || {
        exact_balance(b, 37_000)
    })
    .await?;
    poll("repeated swap SSP reclaim", config.timeout, || async {
        ensure!(
            ssp_available_balance(client, config, a.ssp_url).await? == ssp_before,
            "repeated swap input not reclaimed"
        );
        Ok(())
    })
    .await?;
    for wallet in [a, b] {
        let page = wallet
            .sdk
            .service_provider()
            .list_request_history(breez_sdk_spark::RequestHistoryFilter {
                first: 100,
                types: Some(vec!["LEAVES_SWAP".into()]),
                ..Default::default()
            })
            .await?;
        ensure!(
            !page.entities.is_empty(),
            "partial transfer did not exercise SSP swap"
        );
        for record in page.entities {
            let details = wallet
                .sdk
                .service_provider()
                .get_leaves_swap_request(&record.id)
                .await?
                .context("missing swap")?;
            let leaves = details.swap_leaves.context("missing swap refund leaves")?;
            ensure!(!leaves.is_empty(), "swap has no refund transactions");
            for leaf in leaves {
                let tx: bitcoin::Transaction = bitcoin::consensus::deserialize(&hex::decode(
                    leaf.raw_unsigned_refund_transaction,
                )?)?;
                ensure!(
                    !tx.input.is_empty() && !tx.output.is_empty(),
                    "empty refund transaction"
                );
                ensure!(
                    hex::decode(leaf.adaptor_signed_signature)?.len() == 64,
                    "invalid swap adaptor signature length"
                );
            }
        }
    }
    println!(
        "PASS Breez single-use deposit, partial swaps, restart, repeated split, real refund data"
    );
    Ok(())
}
