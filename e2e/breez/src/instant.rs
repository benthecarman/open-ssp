//! Exercise the instant-deposit wire contract with an unmodified Breez wallet.
//! The pinned Breez SDK lacks this high-level method, so only this test derives
//! its deterministic test keys to sign the Spark instant-deposit statement.
use super::*;
use bitcoin::{
    bip32::{DerivationPath, Xpriv},
    secp256k1::{Message, Secp256k1},
};

fn claim_input(wallet: &Wallet, quote: &Value, address: &str) -> Result<Value> {
    let secp = Secp256k1::new();
    let master = Xpriv::new_master(bitcoin::Network::Regtest, &[wallet.seed_byte; 32])?;
    let identity = master
        .derive_priv(&secp, &"m/8797555'/0'/0'".parse::<DerivationPath>()?)?
        .private_key;
    let deposit = master
        .derive_priv(&secp, &"m/8797555'/0'/3'/0'".parse::<DerivationPath>()?)?
        .private_key;
    let credit = quote["credit_amount"]["original_value"]
        .as_u64()
        .context("quote has no credit")?;
    let value = quote["deposit_amount"]["original_value"]
        .as_u64()
        .context("quote has no deposit amount")?;
    let signature = hex::decode(
        quote["quote_signature"]
            .as_str()
            .context("quote has no signature")?,
    )?;
    let encode = |values: &[&[u8]]| {
        values
            .iter()
            .flat_map(|v| {
                (v.len() as u64)
                    .to_be_bytes()
                    .into_iter()
                    .chain(v.iter().copied())
            })
            .collect::<Vec<_>>()
    };
    let tag = Sha256::digest(encode(&[b"spark", b"claim_instant_static_deposit"]));
    let mut h = Sha256::new();
    h.update(tag);
    h.update(tag);
    h.update(encode(&[
        b"regtest",
        &3u64.to_be_bytes(),
        &credit.to_be_bytes(),
        &0u64.to_be_bytes(),
        address.as_bytes(),
        &value.to_be_bytes(),
        &signature,
    ]));
    let signature = secp.sign_ecdsa(&Message::from_digest(h.finalize().into()), &identity);
    Ok(json!({"static_deposit_quote_id":quote["id"],
        "static_deposit_address_private_key_share":hex::encode(deposit.secret_bytes()),
        "signature":hex::encode(signature.serialize_der())}))
}

pub async fn run(
    client: &Client,
    config: &TestConfig,
    wallet: &Wallet,
    other: &Wallet,
) -> Result<()> {
    let project = command_output(
        "docker",
        &[
            "inspect",
            "--format",
            "{{ index .Config.Labels \"com.docker.compose.project\" }}",
            &config.ssp_container,
        ],
    )
    .await?;
    let project_filter = format!("label=com.docker.compose.project={}", project.trim());
    let miner = command_output(
        "docker",
        &[
            "ps",
            "-q",
            "--filter",
            &project_filter,
            "--filter",
            "label=com.docker.compose.service=bitcoin-miner",
        ],
    )
    .await?;
    ensure!(
        miner.lines().count() == 1,
        "instant test needs exactly one miner"
    );
    command_output("docker", &["stop", miner.trim()]).await?;
    let session = authenticate_wallet(client, wallet).await?;
    let other_session = authenticate_wallet(client, other).await?;
    let mining_address = bitcoin_rpc(client, config, "getnewaddress", json!([])).await?;
    let address = wallet
        .sdk
        .receive_payment(ReceivePaymentRequest {
            payment_method: ReceivePaymentMethod::BitcoinAddress {
                new_address: Some(false),
            },
        })
        .await?
        .payment_request;
    for replace in [false, true] {
        let before = wallet_balance(wallet).await?;
        let ssp_before = ssp_available_balance(client, config, wallet.ssp_url).await?;
        let txid = bitcoin_rpc(
            client,
            config,
            "sendtoaddress",
            json!([address, 0.00002, "", "", false, true]),
        )
        .await?;
        let tx = bitcoin_rpc(client, config, "getrawtransaction", json!([txid, true])).await?;
        let vout = tx["vout"]
            .as_array()
            .context("deposit outputs missing")?
            .iter()
            .find(|v| v["scriptPubKey"]["address"] == address)
            .context("deposit output missing")?["n"]
            .clone();
        let quote = graphql_json(
            client,
            wallet,
            Some(&session),
            "CreateInstantStaticDepositQuote",
            json!({"transaction_id":txid,"output_index":vout,"network":"REGTEST"}),
        )
        .await?;
        let output = &quote["create_instant_static_deposit_quote"];
        ensure!(
            output["fulfillment_plans"][0]["confirmations"] == 0,
            "missing zero-confirmation plan"
        );
        let input = claim_input(wallet, &output["quote"], &address)?;
        reject_request(
            client,
            wallet,
            Some(&other_session),
            "ClaimInstantStaticDeposit",
            input.clone(),
            "no rows",
        )
        .await?;
        let mut forged = input.clone();
        forged["signature"] = json!("00".repeat(64));
        reject_request(
            client,
            wallet,
            Some(&session),
            "ClaimInstantStaticDeposit",
            forged,
            "authorization",
        )
        .await?;
        let claim = graphql_json(
            client,
            wallet,
            Some(&session),
            "ClaimInstantStaticDeposit",
            input.clone(),
        )
        .await?;
        let id = claim["create_claim_instant_static_deposit"]["claim_id"].clone();
        ensure!(id.as_str().is_some(), "instant claim has no ID");
        poll("instant Spark advance", config.timeout, || {
            exact_balance(wallet, before + 1901)
        })
        .await?;
        let coin = bitcoin_rpc(client, config, "gettxout", json!([txid, vout, true])).await?;
        ensure!(
            coin["confirmations"] == 0,
            "advance waited for confirmation"
        );
        let replay = graphql_json(
            client,
            wallet,
            Some(&session),
            "ClaimInstantStaticDeposit",
            input.clone(),
        )
        .await?;
        ensure!(claim == replay, "instant replay changed the claim ID");
        if replace {
            let replacement =
                bitcoin_rpc(client, config, "bumpfee", json!([txid,{"fee_rate":10}])).await?;
            ensure!(
                replacement["txid"] != txid,
                "RBF did not replace the deposit"
            );
        }
        command_output("docker", &["restart", &config.ssp_container]).await?;
        poll(
            "SSP after instant advance restart",
            config.timeout,
            || async {
                let response = client
                    .get(format!("{}/health", wallet.ssp_url))
                    .send()
                    .await?;
                ensure!(response.status().is_success(), "SSP restart pending");
                Ok(())
            },
        )
        .await?;
        let replay = graphql_json(
            client,
            wallet,
            Some(&session),
            "ClaimInstantStaticDeposit",
            input,
        )
        .await?;
        ensure!(claim == replay, "restart created a different instant claim");
        exact_balance(wallet, before + 1901).await?;
        let history = graphql_json(
            client,
            wallet,
            Some(&session),
            "UserRequest",
            json!({"request_id":id}),
        )
        .await?;
        ensure!(
            history["user_request"]["status"] == "TRANSFER_COMPLETED",
            "instant payout has wrong state: {history}"
        );
        bitcoin_rpc(
            client,
            config,
            "generatetoaddress",
            json!([1, mining_address]),
        )
        .await?;
        poll("instant recovery broadcast", config.timeout, || async {
            let history = graphql_json(
                client,
                wallet,
                Some(&session),
                "UserRequest",
                json!({"request_id":id}),
            )
            .await?;
            ensure!(
                history["user_request"]["status"] == "SPEND_TX_BROADCAST",
                "instant recovery pending: {history}"
            );
            Ok(())
        })
        .await?;
        bitcoin_rpc(
            client,
            config,
            "generatetoaddress",
            json!([3, mining_address]),
        )
        .await?;
        poll("instant recovery confirmation", config.timeout, || async {
            let history = graphql_json(
                client,
                wallet,
                Some(&session),
                "UserRequest",
                json!({"request_id":id}),
            )
            .await?;
            ensure!(
                history["user_request"]["status"] == "SPEND_TX_CONFIRMED",
                "instant recovery not confirmed: {history}"
            );
            Ok(())
        })
        .await?;
        poll("instant SSP debit", config.timeout, || async {
            ensure!(
                ssp_available_balance(client, config, wallet.ssp_url).await? == ssp_before - 1901,
                "instant payout was duplicated or incomplete"
            );
            Ok(())
        })
        .await?;
        exact_balance(wallet, before + 1901).await?;
        println!(
            "PASS instant deposit: 1901 sats advanced at zero confirmations, restart replay, recovery confirmed; replacement={replace}"
        );
    }
    command_output("docker", &["start", miner.trim()]).await?;
    Ok(())
}
