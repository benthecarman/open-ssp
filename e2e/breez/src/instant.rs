//! Exercise instant deposits through the Breez SDK public methods.
//! The SDK owns authentication, key derivation, and claim signing.
use super::*;
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
        let transaction_hex = tx["hex"].as_str().context("missing transaction hex")?;
        let quote = wallet
            .sdk
            .get_instant_deposit_quote(
                transaction_hex,
                vout.as_u64().context("missing vout")? as u32,
            )
            .await?;
        ensure!(
            quote.fulfillment_plans[0].confirmations == 0,
            "missing zero-confirmation plan"
        );
        ensure!(
            other
                .sdk
                .claim_instant_deposit(
                    transaction_hex,
                    quote.quote.clone(),
                    quote.fulfillment_plans[0].clone()
                )
                .await
                .is_err(),
            "another wallet claimed the quote"
        );
        let mut changed = quote.quote.clone();
        changed.credit_amount.original_value += 1;
        ensure!(
            wallet
                .sdk
                .claim_instant_deposit(transaction_hex, changed, quote.fulfillment_plans[0].clone())
                .await
                .is_err(),
            "changed quote was accepted"
        );
        let display = poll("upstream instant deposit quote", config.timeout, || async {
            Ok(wallet
                .sdk
                .fetch_claim_deposit_quote(breez_sdk_spark::FetchClaimDepositQuoteRequest {
                    txid: txid.as_str().context("missing txid")?.to_owned(),
                    vout: vout.as_u64().context("missing vout")? as u32,
                })
                .await?)
        })
        .await?;
        ensure!(
            display.confirmations == 0
                && display
                    .instant
                    .as_ref()
                    .is_some_and(|q| q.credit_amount_sats == 1901),
            "upstream quote missing zero-confirmation credit"
        );
        let submitted = wallet
            .sdk
            .claim_deposit(breez_sdk_spark::ClaimDepositRequest {
                txid: txid.as_str().context("missing txid")?.to_owned(),
                vout: vout.as_u64().context("missing vout")? as u32,
                max_fee: Some(breez_sdk_spark::MaxFee::Fixed { amount: 99 }),
            })
            .await?;
        ensure!(
            submitted.payment.is_none(),
            "instant claim should settle asynchronously"
        );
        let claim = wallet
            .sdk
            .claim_instant_deposit(
                transaction_hex,
                quote.quote.clone(),
                quote.fulfillment_plans[0].clone(),
            )
            .await?;
        let id = claim.clone();
        poll("instant Spark advance", config.timeout, || {
            exact_balance(wallet, before + 1901)
        })
        .await?;
        let coin = bitcoin_rpc(client, config, "gettxout", json!([txid, vout, true])).await?;
        ensure!(
            coin["confirmations"] == 0,
            "advance waited for confirmation"
        );
        let replay = wallet
            .sdk
            .claim_instant_deposit(
                transaction_hex,
                quote.quote.clone(),
                quote.fulfillment_plans[0].clone(),
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
        let replay = wallet
            .sdk
            .claim_instant_deposit(
                transaction_hex,
                quote.quote.clone(),
                quote.fulfillment_plans[0].clone(),
            )
            .await?;
        ensure!(claim == replay, "restart created a different instant claim");
        exact_balance(wallet, before + 1901).await?;
        let history = sdk_request(wallet, "UserRequest", json!({"request_id":id})).await?;
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
            let history = sdk_request(wallet, "UserRequest", json!({"request_id":id})).await?;
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
            let history = sdk_request(wallet, "UserRequest", json!({"request_id":id})).await?;
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
