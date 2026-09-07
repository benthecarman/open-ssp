use axum::http::HeaderMap;
use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde_json::{json, Value};
use std::str::FromStr;
use uuid::Uuid;

use crate::{auth, AppState, GraphqlRequest};

/// Dispatch a GraphQL document to the matching SSP resolver.
/// Operation names mirror spark-sdk `SspClient` methods (client.ts) and the
/// `ssp_rc_schema.graphql` schema (Query/Mutation sections).
///
/// Response shapes must use the RAW schema field names: the SDK sends
/// aliased fragments (`foo_bar: foo`) and `*FromJson` reads the alias keys.
/// Missing inner fields surface as `undefined`, so every resolver below
/// returns the full field set its fragment requests.
pub async fn dispatch(
    state: AppState,
    headers: &HeaderMap,
    op: &str,
    req: &GraphqlRequest,
) -> Result<Value, String> {
    let v = &req.variables;
    // Most mutations nest under `input`.
    let input = v.get("input").cloned().unwrap_or_else(|| v.clone());
    let now = chrono::Utc::now().to_rfc3339();

    match op {
        // ---- auth (no session needed) ----
        "GetChallenge" | "get_challenge" => {
            let pk = input
                .get("public_key")
                .and_then(|x| x.as_str())
                .or_else(|| v.get("public_key").and_then(|x| x.as_str()))
                .unwrap_or("");
            let protected = auth::get_challenge(&state, pk).await?;
            Ok(json!({ "get_challenge": {
                "__typename": "GetChallengeOutput",
                "protected_challenge": protected,
            }}))
        }
        "VerifyChallenge" | "verify_challenge" => {
            let obj = input;
            let pk = str_of(&obj, "identity_public_key");
            let chal = str_of(&obj, "protected_challenge");
            let sig = str_of(&obj, "signature");
            let (token, valid_until) = auth::verify_challenge(&state, &pk, &chal, &sig).await?;
            Ok(json!({ "verify_challenge": {
                "__typename": "VerifyChallengeOutput",
                "session_token": token,
                "valid_until": valid_until.to_rfc3339(),
            }}))
        }
        // ---- fee estimates ----
        "LeavesSwapFeeEstimate" | "leaves_swap_fee_estimate" => {
            let fee = state.config.fee_flat_sats_swap;
            Ok(json!({ "leaves_swap_fee_estimate": {
                "fee_estimate": {
                    "original_value": fee,
                    "original_unit": "SATOSHI",
                    "preferred_currency_unit": "SATOSHI",
                    "preferred_currency_value_rounded": fee,
                }
            }}))
        }
        "LightningSendFeeEstimate" | "lightning_send_fee_estimate" => {
            let inv = str_of(&input, "encoded_invoice");
            validate_internal_send_support(&state.db, &inv).await?;
            let amt = opt_num(&input, "amount_sats");
            let msat = state.ldk.fee_estimate_msat(&inv, amt).await;
            Ok(json!({ "lightning_send_fee_estimate": {
                "fee_estimate": {
                    "original_value": msat / 1000,
                    "original_unit": "SATOSHI",
                    "preferred_currency_unit": "SATOSHI",
                    "preferred_currency_value_rounded": msat / 1000,
                }
            }}))
        }
        "CoopExitFeeEstimate" | "CoopExitFeeEstimates" | "coop_exit_fee_estimates" => {
            let owner = auth::require_session(&state, headers).await?;
            let service = state
                .coop_exit
                .as_ref()
                .ok_or("cooperative withdrawals are not configured")?;
            let quote = service.quote(&owner, &input).await?;
            Ok(json!({"coop_exit_fee_estimates": {
                "speed_fast": {"user_fee": currency_amount(quote.user_fee), "l1_broadcast_fee": currency_amount(quote.fees[0])},
                "speed_medium": {"user_fee": currency_amount(quote.user_fee), "l1_broadcast_fee": currency_amount(quote.fees[1])},
                "speed_slow": {"user_fee": currency_amount(quote.user_fee), "l1_broadcast_fee": currency_amount(quote.fees[2])},
            }}))
        }
        "CoopExitFeeQuote" | "coop_exit_fee_quote" => {
            let owner = auth::require_session(&state, headers).await?;
            let service = state
                .coop_exit
                .as_ref()
                .ok_or("cooperative withdrawals are not configured")?;
            let quote = service.quote(&owner, &input).await?;
            Ok(json!({"coop_exit_fee_quote": {"quote": service.quote_response(&quote)}}))
        }
        // Paginated user-request history for the session wallet.
        "FetchCurrentUserToUserRequestsConnection"
        | "fetch_current_user_to_user_requests_connection" => {
            let _ = auth::require_session(&state, headers).await?;
            Ok(json!({ "current_user": {
                "user_requests": {
                    "__typename": "SparkWalletUserToUserRequestsConnection",
                    "count": 0,
                    "page_info": {
                        "__typename": "PageInfo",
                        "has_next_page": false,
                        "has_previous_page": false,
                        "start_cursor": null,
                        "end_cursor": null,
                    },
                    "entities": [],
                }
            }}))
        }
        // ---- lightning receive (quote is stateless+signed; receive persists request) ----
        "LightningReceiveQuote" | "lightning_receive_quote" => {
            let _ = auth::require_session(&state, headers).await?;
            let amount = num_of(&input, "amount_sats");
            validate_sats(amount)?;
            let network = str_of(&input, "network");
            let network = if network.is_empty() {
                state.config.network.clone()
            } else {
                network
            };
            validate_network(&state, &network)?;
            let transfer_id = Uuid::new_v4().to_string();
            // The SDK quote flow needs a protobuf TransferManifest. This JSON
            // manifest keeps the GraphQL response shape but is not accepted as
            // a production receive quote.
            let manifest = json!({
                "transfer_id": transfer_id,
                "amount_sats": amount,
                "network": network,
                "ssp_identity_pubkey": crate::ssp_identity(&state).await?,
            });
            let serialized = B64.encode(serde_json::to_vec(&manifest).unwrap());
            let sig = sign_with_ssp(&state, &serialized).await?;
            Ok(json!({ "lightning_receive_quote": {
                "issued_quote": {
                    "serialized_manifest": serialized,
                    "issuer_signature": sig,
                },
                "attribution_status": "NO_PARTNER_JWT",
            }}))
        }
        "RequestBolt12Receive" | "request_bolt12_receive" => {
            let owner = auth::require_session(&state, headers).await?;
            let amount = num_of(&input, "amount_sats");
            validate_sats(amount)?;
            let requested_network = str_of(&input, "network");
            if !requested_network.is_empty() {
                validate_network(&state, &requested_network)?;
            }
            let requested_receiver = str_of(&input, "receiver_identity_pubkey");
            let receiver = if requested_receiver.is_empty() {
                owner.clone()
            } else {
                secp256k1::PublicKey::from_str(&requested_receiver)
                    .map_err(|_| {
                        "receiver_identity_pubkey must be a compressed public key".to_string()
                    })?
                    .to_string()
            };
            let memo = str_of(&input, "memo");
            let expiry = u32::try_from(opt_num(&input, "expiry_secs").unwrap_or(86_400))
                .map_err(|_| "expiry_secs is out of range".to_string())?;
            if expiry == 0 {
                return Err("expiry_secs must be positive".to_string());
            }
            let invoice_expires_at =
                (chrono::Utc::now() + chrono::Duration::seconds(expiry as i64)).to_rfc3339();
            let offer = state.ldk.create_bolt12_offer(amount, &memo, expiry).await?;
            let rec = store_request(
                &state,
                "LIGHTNING_RECEIVE",
                &owner,
                &now,
                json!({"amount_sats": amount, "payment_hash": offer.offer_id,
                       "offer_id": offer.offer_id, "payment_kind": "BOLT12",
                       "invoice": offer.offer, "network": state.config.network,
                       "expiry_secs": expiry,
                       "receiver_identity_pubkey": receiver.clone(),
                       "invoice_expires_at": invoice_expires_at}),
                None,
            )
            .await?;
            state
                .db
                .set_receive_status(&offer.offer_id, "INVOICE_CREATED")
                .await?;
            Ok(json!({ "request_lightning_receive": {
                "request": {
                    "__typename": "LightningReceiveRequest",
                    "id": rec["id"],
                    "created_at": now,
                    "updated_at": now,
                    "network": state.config.network,
                    "invoice": {
                        "__typename": "Invoice",
                        "encoded_invoice": offer.offer,
                        "bitcoin_network": state.config.network,
                        "payment_hash": offer.offer_id,
                        "amount": currency_amount(amount),
                        "created_at": now,
                        "expires_at": invoice_expires_at,
                        "memo": memo,
                    },
                    "status": "INVOICE_CREATED",
                    "transfer": null,
                    "receiver_identity_public_key": receiver,
                }
            }}))
        }
        "RequestLightningReceive" | "request_lightning_receive" => {
            let owner = auth::require_session(&state, headers).await?;
            let amount = num_of(&input, "amount_sats");
            validate_sats(amount)?;
            let requested_network = str_of(&input, "network");
            if !requested_network.is_empty() {
                validate_network(&state, &requested_network)?;
            }
            let hash = str_of(&input, "payment_hash").to_lowercase();
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("payment_hash must be 32 bytes hex".to_string());
            }
            if state.db.lightning_receive_for_hash(&hash).await?.is_some() {
                return Err("payment_hash already has a Lightning receive request".to_string());
            }
            let requested_receiver = str_of(&input, "receiver_identity_pubkey");
            let receiver = if requested_receiver.is_empty() {
                owner.clone()
            } else {
                secp256k1::PublicKey::from_str(&requested_receiver)
                    .map_err(|_| {
                        "receiver_identity_pubkey must be a compressed public key".to_string()
                    })?
                    .to_string()
            };
            let memo = str_of(&input, "memo");
            let expiry = u32::try_from(opt_num(&input, "expiry_secs").unwrap_or(86_400))
                .map_err(|_| "expiry_secs is out of range".to_string())?;
            if expiry == 0 {
                return Err("expiry_secs must be positive".to_string());
            }
            let invoice_expires_at =
                (chrono::Utc::now() + chrono::Duration::seconds(expiry as i64)).to_rfc3339();
            let inv = state
                .ldk
                .create_invoice(amount, &hash, &memo, expiry)
                .await
                .map_err(|e| format!("ldk create_invoice: {e}"))?;
            let rec = store_request(
                &state,
                "LIGHTNING_RECEIVE",
                &owner,
                &now,
                json!({"amount_sats": amount, "payment_hash": hash,
                       "invoice": inv.invoice, "network": state.config.network,
                       "expiry_secs": expiry,
                       "receiver_identity_pubkey": receiver.clone(),
                       "invoice_expires_at": invoice_expires_at}),
                None,
            )
            .await?;
            state
                .db
                .set_receive_status(&hash, "INVOICE_CREATED")
                .await?;
            let req_id = rec["id"].as_str().unwrap_or("").to_string();
            Ok(json!({ "request_lightning_receive": {
                "request": {
                    "__typename": "LightningReceiveRequest",
                    "id": req_id,
                    "created_at": now,
                    "updated_at": now,
                    "network": state.config.network,
                    "invoice": {
                        "__typename": "Invoice",
                        "encoded_invoice": inv.invoice,
                        "bitcoin_network": state.config.network,
                        "payment_hash": hash,
                        "amount": currency_amount(amount),
                        "created_at": now,
                        "expires_at": invoice_expires_at,
                        "memo": memo,
                    },
                    "status": "INVOICE_CREATED",
                    "transfer": null,
                    "receiver_identity_public_key": receiver,
                }
            }}))
        }
        // ---- lightning send ----
        "RequestLightningSend" | "request_lightning_send" => {
            let owner = auth::require_session(&state, headers).await?;
            let inv = str_of(&input, "encoded_invoice");
            let amt = opt_num(&input, "amount_sats");
            let ext_id = str_of(&input, "user_outbound_transfer_external_id");
            if ext_id.is_empty() {
                return Err("user_outbound_transfer_external_id is required".to_string());
            }
            let explicit_idem = str_of(&input, "idempotency_key");
            let idem = if explicit_idem.is_empty() {
                ext_id.clone()
            } else {
                explicit_idem
            };
            let _send_guard = state.send_lock.lock().await;
            if let Some(rec) = state.db.find_by_idempotency(&owner, &idem).await? {
                let payload = &rec["payload"];
                if payload["encoded_invoice"].as_str() != Some(inv.as_str())
                    || payload["user_outbound_transfer_external_id"].as_str()
                        != Some(ext_id.as_str())
                    || payload["amount_sats"].as_u64() != amt
                {
                    return Err("idempotency key was already used for another payment".into());
                }
                if let Some(send) = state
                    .db
                    .lightning_send_for_payment(rec["id"].as_str().unwrap_or(""))
                    .await?
                {
                    state.ldk.submit_send(&send).await?;
                }
                return send_response_from_record(&state, &rec, &now).await;
            }
            validate_internal_send_support(&state.db, &inv).await?;
            let send = state.ldk.prepare_send(&owner, &ext_id, &inv, amt).await?;
            let rec = state
                .db
                .prepare_lightning_send(&send, &idem, &state.config.network)
                .await?;
            state.ldk.submit_send(&send).await?;
            return send_response_from_record(&state, &rec, &now).await;
        }
        // ---- swaps (SDK mutation name is RequestSwap / field request_swap) ----
        "RequestSwap" | "request_swap" => {
            let owner = auth::require_session(&state, headers).await?;
            let total = num_of(&input, "total_amount_sats");
            if total == 0 {
                return Err("swap total must be positive".to_string());
            }
            // The embedded wallet serves fills from exact leaves only. If it
            // needs a swap, its ladder is depleted. Fail before a recursive
            // swap can lock SSP leaves on the operators.
            if let Ok(resolved) = crate::ssp_identity(&state).await {
                if !resolved.is_empty() && owner == resolved {
                    return Err("NEEDS_TOPUP: SSP ladder depleted, top up liquidity".to_string());
                }
            }
            if state.config.max_swap_total_sats > 0 && total > state.config.max_swap_total_sats {
                return Err(format!(
                    "swap total {total} exceeds operator cap {}",
                    state.config.max_swap_total_sats
                ));
            }
            // Fee is server-side (what leaves_swap_fee_estimate quotes);
            // client input is ignored so a forged fee changes nothing.
            let fee = state.config.fee_flat_sats_swap;
            let ext_id = str_of(&input, "user_outbound_transfer_external_id");
            if ext_id.is_empty() {
                return Err("user_outbound_transfer_external_id is required".to_string());
            }
            let adaptor_pubkey = str_of(&input, "adaptor_pubkey");
            if adaptor_pubkey.len() != 66
                || !adaptor_pubkey.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err("adaptor_pubkey must be a compressed public key".to_string());
            }
            let network = state.config.network.clone();
            // Target list (rc schema) or scalar (dated schema).
            let targets: Vec<u64> = match input.get("target_amount_sats") {
                Some(Value::Array(a)) => a.iter().filter_map(|e| e.as_u64()).collect(),
                Some(v) => v.as_u64().map(|t| vec![t]).unwrap_or_default(),
                None => vec![],
            };
            let target = targets.iter().try_fold(0u64, |sum, value| {
                sum.checked_add(*value)
                    .ok_or_else(|| "target amount overflow".to_string())
            })?;
            let payout_total = total
                .checked_sub(fee)
                .ok_or_else(|| "swap fee exceeds total".to_string())?;
            if target > payout_total {
                return Err("target amounts plus fee exceed swap total".to_string());
            }
            let fill = state
                .spark
                .fill_swap(
                    &owner,
                    &ext_id,
                    &adaptor_pubkey,
                    &targets,
                    total,
                    payout_total,
                )
                .await?;
            let inbound_id = fill.transfer_id;
            let swap_leaves = fill
                .leaf_ids
                .into_iter()
                .map(|leaf_id| {
                    json!({
                        "leaf_id": leaf_id,
                        "raw_unsigned_refund_transaction": "",
                        "adaptor_signed_signature": "",
                        "direct_raw_unsigned_refund_transaction": "",
                        "direct_adaptor_signed_signature": "",
                        "direct_from_cpfp_raw_unsigned_refund_transaction": "",
                        "direct_from_cpfp_adaptor_signed_signature": "",
                    })
                })
                .collect::<Vec<_>>();
            let rec = store_request(
                &state,
                "LEAVES_SWAP",
                &owner,
                &now,
                json!({"total_amount_sats": total, "target_amount_sats": target,
                       "fee_sats": fee,
                       "inbound_transfer_spark_id": inbound_id}),
                None,
            )
            .await?;
            let rid = rec["id"].as_str().unwrap_or("").to_string();
            state
                .db
                .insert_transfer(&inbound_id, &rid, "COUNTER_SWAP", "CREATED", &owner)
                .await?;
            if !ext_id.is_empty() {
                state
                    .db
                    .insert_transfer(&ext_id, &rid, "TRANSFER", "CREATED", &owner)
                    .await?;
            }
            Ok(json!({ "request_swap": {
                "request": {
                    "__typename": "LeavesSwapRequest",
                    "id": rec["id"],
                    "created_at": now,
                    "updated_at": now,
                    "network": network,
                    "status": "CREATED",
                    "total_amount": currency_amount(total),
                    "target_amount": currency_amount(target),
                    "fee": currency_amount(fee),
                    "inbound_transfer": {
                        "__typename": "Transfer",
                        "total_amount": currency_amount(total),
                        "spark_id": inbound_id,
                        "user_request": {"__typename": "LeavesSwapRequest", "id": rec["id"]},
                    },
                    "swap_leaves": swap_leaves,
                    "expires_at": null,
                }
            }}))
        }
        // ---- static deposits (SDK uses static_deposit_quote only) ----
        "StaticDepositQuote" | "static_deposit_quote" => {
            let _ = auth::require_session(&state, headers).await?;
            // Quote signing without a UTXO lookup is only acceptable where
            // coins are worthless. Refuse elsewhere rather than signing blind.
            let network = str_of(&input, "network");
            let network = if network.is_empty() {
                state.config.network.clone()
            } else {
                network
            };
            validate_network(&state, &network)?;
            if state.config.network != "REGTEST" && state.config.network != "LOCAL" {
                return Err("static deposit quotes are regtest-only".to_string());
            }
            let txid = str_of(&input, "transaction_id").to_lowercase();
            if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("transaction_id must be 64 hex chars".to_string());
            }
            let vout = num_of(&input, "output_index");
            if vout > u32::MAX as u64 {
                return Err("output_index out of range".to_string());
            }
            // FAKE credit: fixed amount, must stay <= the real UTXO value or
            // the SO rejects the claim (totalAmount > utxo.Amount check).
            // TODO(live): look up the UTXO via bitcoind/esplora and apply fees.
            let credit: u64 = 100_000;
            let payload = format!("{txid}:{vout}:{credit}");
            let proposed_signature = sign_with_ssp(&state, &payload).await?;
            let sig = state
                .db
                .record_static_quote(&txid, vout as u32, credit, &proposed_signature, &now)
                .await?
                .ok_or_else(|| "static deposit output was already claimed".to_string())?;
            let quote = json!({
                "__typename": "StaticDepositQuoteOutput",
                "transaction_id": txid, "output_index": vout,
                "network": network,
                "credit_amount_sats": credit, "signature": sig,
            });
            Ok(json!({ "static_deposit_quote": quote }))
        }
        // SDK ClaimStaticDeposit mutation only (no fixed-amount variant in SspClient).
        "ClaimStaticDeposit" | "claim_static_deposit" => {
            let owner = auth::require_session(&state, headers).await?;
            let txid = str_of(&input, "transaction_id").to_lowercase();
            let vout = num_of(&input, "output_index");
            let quote_signature = str_of(&input, "quote_signature");
            if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err("transaction_id must be 64 hex chars".to_string());
            }
            if vout > u32::MAX as u64 {
                return Err("output_index out of range".to_string());
            }
            if !state
                .db
                .consume_static_quote(&txid, vout as u32, &quote_signature)
                .await?
            {
                return Err("unknown, mismatched, or already claimed quote".to_string());
            }
            enforce_compat_quota(&state, &owner).await?;
            store_request(
                &state,
                "CLAIM_STATIC_DEPOSIT",
                &owner,
                &now,
                static_deposit_payload(&input),
                None,
            )
            .await?;
            // ClaimStaticDepositOutputFragment selects only transfer_id.
            Ok(json!({ "claim_static_deposit": {
                "__typename": "ClaimStaticDepositOutput",
                "transfer_id": Uuid::new_v4().to_string(),
            }}))
        }
        "CreateInstantStaticDepositQuote" | "create_instant_static_deposit_quote" => {
            let _ = auth::require_session(&state, headers).await?;
            Ok(json!({ "create_instant_static_deposit_quote": {
                "quote": {"id": Uuid::new_v4().to_string(), "status": "CREATED"},
            }}))
        }
        "CreateClaimInstantStaticDeposit" | "create_claim_instant_static_deposit" => {
            let owner = auth::require_session(&state, headers).await?;
            enforce_compat_quota(&state, &owner).await?;
            let rec = store_request(
                &state,
                "CLAIM_INSTANT_STATIC_DEPOSIT",
                &owner,
                &now,
                static_deposit_payload(&input),
                None,
            )
            .await?;
            Ok(json!({ "create_claim_instant_static_deposit": {
                "claim": {"id": rec["id"], "status": "CREATED"},
            }}))
        }
        // ---- cooperative withdrawals ----
        "RequestCoopExit" | "request_coop_exit" => {
            let owner = auth::require_session(&state, headers).await?;
            let service = state
                .coop_exit
                .as_ref()
                .ok_or("cooperative withdrawals are not configured")?;
            Ok(json!({"request_coop_exit": {"request": service.request(&owner, &input).await?}}))
        }
        "CompleteCoopExit" | "complete_coop_exit" => {
            let owner = auth::require_session(&state, headers).await?;
            let service = state
                .coop_exit
                .as_ref()
                .ok_or("cooperative withdrawals are not configured")?;
            Ok(json!({"complete_coop_exit": {"request": service.complete(&owner, &input).await?}}))
        }
        // ---- reads ----
        // SDK Transfers query only. All rows here were created by this SSP, so
        // keep the transfer-to-request join intact.
        "Transfers" | "transfers" => {
            let owner = auth::require_session(&state, headers).await?;
            let ids = ids_of(&input, v);
            let rows = state.db.transfers_for(&ids, &owner).await?;
            let mut list = Vec::with_capacity(rows.len());
            for row in &rows {
                let request_id = row
                    .get("user_request_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| "transfer has no user request id".to_string())?;
                let request = state
                    .db
                    .get_request(request_id, &owner)
                    .await?
                    .ok_or_else(|| format!("transfer request {request_id} was not found"))?;
                let user_request = user_request_union(&state, &request).await;
                list.push(transfer_response(row, user_request));
            }
            Ok(json!({ "transfers": list }))
        }
        "UserRequest" | "user_request" => {
            let owner = auth::require_session(&state, headers).await?;
            let rid = str_of(&input, "request_id");
            let found = state.db.get_request(&rid, &owner).await?;
            match found {
                Some(rec) => Ok(json!({ "user_request": user_request_union(&state, &rec).await })),
                None => Ok(json!({ "user_request": null })),
            }
        }
        "WalletWebhooks" | "wallet_webhooks" | "ListSparkWalletWebhooks" => {
            Ok(json!({ "wallet_webhooks": { "webhooks": [] } }))
        }
        "RegisterWalletWebhook" | "register_wallet_webhook" => Ok(json!({
            "register_wallet_webhook": { "webhook_id": Uuid::new_v4().to_string() }
        })),
        "DeleteWalletWebhook" | "delete_wallet_webhook" => Ok(json!({
            "delete_wallet_webhook": { "success": true }
        })),
        _ => Err(format!("unsupported SSP operation: {op}")),
    }
}

/// Rewrite canonical (schema-named) response keys to the aliased names the
/// SDK's generated documents request (`alias: field`).
///
/// Real GraphQL servers return data keyed by alias; our resolvers return raw
/// schema names. This pass collects `alias: field` pairs from the query text
/// (argument lists stripped first so `name: $var` pairs don't pollute the map)
/// and copies `field` -> `alias` on every object that has `field`.
/// Extra keys are harmless: each `*FromJson` reads only its own aliases.
pub fn apply_query_aliases(data: &mut Value, query: &str) {
    let aliases = collect_aliases(query);
    apply_aliases_to_value(data, &aliases);
}

fn collect_aliases(query: &str) -> Vec<(String, String)> {
    const MAX_ALIASES: usize = 2000;
    // Strip balanced (...) argument lists (they contain `name: value` pairs
    // that are NOT selection aliases). Track string literals and `#`
    // comments so their contents never contribute pairs.
    let mut stripped = String::with_capacity(query.len());
    let mut depth = 0usize;
    let mut in_string = false;
    let mut in_comment = false;
    let mut prev_backslash = false;
    for ch in query.chars() {
        if in_comment {
            if ch == '\n' {
                in_comment = false;
                stripped.push(ch);
            }
            continue;
        }
        if in_string {
            if ch == '"' && !prev_backslash {
                in_string = false;
            }
            prev_backslash = ch == '\\' && !prev_backslash;
            continue;
        }
        match ch {
            '#' if depth == 0 => in_comment = true,
            '"' if depth == 0 => in_string = true,
            '(' => depth += 1,
            ')' if depth > 0 => depth -= 1,
            _ if depth == 0 => stripped.push(ch),
            _ => {}
        }
        if ch != '\\' {
            prev_backslash = false;
        }
    }
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let bytes = stripped.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // match ident : ident
        if bytes[i].is_ascii_alphabetic() || bytes[i] == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let name = &stripped[start..i];
            let mut j = i;
            while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b':' {
                j += 1;
                while j < bytes.len() && bytes[j].is_ascii_whitespace() {
                    j += 1;
                }
                let fstart = j;
                if j < bytes.len() && (bytes[j].is_ascii_alphabetic() || bytes[j] == b'_') {
                    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_')
                    {
                        j += 1;
                    }
                    let field = &stripped[fstart..j];
                    // skip `__typename` (bare, no alias) and fragment spreads
                    if name != "__typename"
                        && name != field
                        && !name.starts_with("...")
                        && seen.insert((name.to_string(), field.to_string()))
                    {
                        out.push((name.to_string(), field.to_string()));
                        if out.len() >= MAX_ALIASES {
                            break;
                        }
                    }
                    i = j;
                    continue;
                }
            }
            continue;
        }
        i += 1;
    }
    out
}

fn apply_aliases_to_value(v: &mut Value, aliases: &[(String, String)]) {
    match v {
        Value::Object(map) => {
            for (alias, field) in aliases {
                if let Some(val) = map.get(field).cloned() {
                    if !map.contains_key(alias) {
                        map.insert(alias.clone(), val);
                    }
                }
            }
            for val in map.values_mut() {
                apply_aliases_to_value(val, aliases);
            }
        }
        Value::Array(arr) => {
            for val in arr.iter_mut() {
                apply_aliases_to_value(val, aliases);
            }
        }
        _ => {}
    }
}

/// Build the exact UserRequest union member for a stored request record.
/// Kinds map to GraphQL types: LIGHTNING_SEND->LightningSendRequest,
/// LIGHTNING_RECEIVE->LightningReceiveRequest, LEAVES_SWAP->LeavesSwapRequest,
/// COOP_EXIT->CoopExitRequest, CLAIM_STATIC_DEPOSIT->ClaimStaticDeposit.
/// Send status is refreshed from the payment tracker (event-driven).
async fn user_request_union(state: &AppState, rec: &Value) -> Value {
    let kind = rec.get("type").and_then(|v| v.as_str()).unwrap_or("");
    let id = rec.get("id").cloned().unwrap_or(Value::Null);
    let created = rec
        .get("created_at")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let p = rec.get("payload").cloned().unwrap_or(json!({}));
    let net = p
        .get("network")
        .and_then(|v| v.as_str())
        .unwrap_or(&state.config.network)
        .to_string();
    let sats = currency_amount;
    match kind {
        "COOP_EXIT_V2" => match &state.coop_exit {
            Some(service) => service
                .get(
                    rec["id"].as_str().unwrap_or(""),
                    rec["owner_identity_pubkey"].as_str().unwrap_or(""),
                )
                .await
                .ok()
                .flatten()
                .unwrap_or(Value::Null),
            None => Value::Null,
        },

        "LIGHTNING_SEND" => {
            let pid = p.get("payment_id").and_then(|v| v.as_str()).unwrap_or("");
            let status = match state.ldk.payment_status(pid).await.as_str() {
                "SUCCEEDED" => "LIGHTNING_PAYMENT_SUCCEEDED",
                "FAILED" => "LIGHTNING_PAYMENT_FAILED",
                _ => "LIGHTNING_PAYMENT_INITIATED",
            };
            json!({
                "__typename": "LightningSendRequest",
                "id": id, "created_at": created, "updated_at": created,
                "network": net,
                "encoded_invoice": p.get("encoded_invoice").cloned().unwrap_or(Value::Null),
                "fee": sats(0),
                "idempotency_key": p.get("idempotency_key").cloned().unwrap_or(Value::Null),
                "status": status,
            })
        }
        "LIGHTNING_RECEIVE" => {
            let amount = p.get("amount_sats").and_then(|v| v.as_u64()).unwrap_or(0);
            let payment_hash = p.get("payment_hash").and_then(|v| v.as_str()).unwrap_or("");
            let request_id = rec.get("id").and_then(Value::as_str).unwrap_or("");
            let owner = rec
                .get("owner_identity_pubkey")
                .and_then(Value::as_str)
                .unwrap_or("");
            let status = state
                .db
                .receive_status(payment_hash)
                .await
                .unwrap_or_else(|_| "INVOICE_CREATED".to_string());
            let preimage = state
                .db
                .lightning_receive_for_hash(payment_hash)
                .await
                .ok()
                .flatten()
                .and_then(|receive| receive.preimage);
            let transfer = state
                .db
                .transfer_for_request(request_id, owner)
                .await
                .ok()
                .flatten()
                .map(|spark_id| {
                    json!({
                        "__typename": "Transfer",
                        "total_amount": sats(amount),
                        "spark_id": spark_id,
                        "user_request": {
                            "__typename": "LightningReceiveRequest",
                            "id": request_id,
                        },
                    })
                });
            json!({
                "__typename": "LightningReceiveRequest",
                "id": id, "created_at": created, "updated_at": created,
                "network": net,
                "invoice": {
                    "__typename": "Invoice",
                    "encoded_invoice": p.get("invoice").cloned().unwrap_or(Value::Null),
                    "bitcoin_network": net,
                    "payment_hash": p.get("payment_hash").cloned().unwrap_or(Value::Null),
                    "amount": sats(amount),
                    "created_at": created,
                    "expires_at": p
                        .get("invoice_expires_at")
                        .cloned()
                        .unwrap_or_else(|| Value::String(created.clone())),
                    "memo": null,
                },
                "status": status,
                "transfer": transfer,
                "payment_preimage": preimage,
                "receiver_identity_public_key": p
                    .get("receiver_identity_pubkey")
                    .cloned()
                    .unwrap_or_else(|| Value::String(owner.to_string())),
            })
        }
        "LEAVES_SWAP" => {
            let total = p
                .get("total_amount_sats")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let target = p
                .get("target_amount_sats")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let fee = p.get("fee_sats").and_then(|v| v.as_u64()).unwrap_or(0);
            let inbound = p
                .get("inbound_transfer_spark_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            json!({
                "__typename": "LeavesSwapRequest",
                "id": id, "created_at": created, "updated_at": created,
                "network": net, "status": "CREATED",
                "total_amount": sats(total), "target_amount": sats(target),
                "fee": sats(fee),
                "inbound_transfer": {
                    "__typename": "Transfer",
                    "total_amount": sats(total),
                    "spark_id": inbound,
                    "user_request": {"id": id},
                },
                "swap_leaves": [], "expires_at": null,
            })
        }
        "COOP_EXIT" => json!({
            "__typename": "CoopExitRequest",
            "id": id, "created_at": created, "updated_at": created,
            "network": net,
            "fee": sats(1000), "l1_broadcast_fee": sats(500),
            "fee_quote": null,
            "exit_speed": p.get("exit_speed").and_then(Value::as_str).unwrap_or("MEDIUM"),
            "status": p.get("status").and_then(Value::as_str).unwrap_or("CREATED"),
            "expires_at": null,
            "raw_connector_transaction": "", "raw_coop_exit_transaction": "",
            "coop_exit_txid": p.get("coop_exit_txid").cloned().unwrap_or(Value::Null),
        }),
        "CLAIM_STATIC_DEPOSIT" => json!({
            "__typename": "ClaimStaticDeposit",
            "id": id, "created_at": created, "updated_at": created,
            "network": net,
            "credit_amount": sats(p.get("credit_amount_sats").and_then(|v| v.as_u64()).unwrap_or(0)),
            "max_fee": null, "status": "CREATED",
            "transaction_id": p.get("transaction_id").cloned().unwrap_or(Value::Null),
            "output_index": p.get("output_index").cloned().unwrap_or(Value::Null),
            "bitcoin_network": net, "transfer_spark_id": null,
        }),
        _ => Value::Null,
    }
}

fn transfer_response(row: &Value, user_request: Value) -> Value {
    json!({
        "__typename": "Transfer",
        "total_amount": currency_amount(
            row.get("total_amount_sats")
                .and_then(Value::as_u64)
                .unwrap_or(0)
        ),
        "spark_id": row.get("spark_id").cloned().unwrap_or(Value::Null),
        "user_request": user_request,
    })
}

/// Reject unsupported local invoices before the wallet locks sender funding.
async fn validate_internal_send_support(db: &crate::db::Db, invoice: &str) -> Result<(), String> {
    if db
        .lightning_receive_hash_for_invoice(invoice)
        .await?
        .is_some()
    {
        return Err(
            "this invoice cannot be paid internally; use an external Lightning wallet".into(),
        );
    }
    Ok(())
}

/// Build the request_lightning_send response from a stored LIGHTNING_SEND
/// record, refreshing status from the payment tracker (M4 idempotent replay
/// shares this with the fresh-send path).
async fn send_response_from_record(
    state: &AppState,
    rec: &Value,
    now: &str,
) -> Result<Value, String> {
    // Events and reconciliation both update the durable send status.
    let p = rec.get("payload").cloned().unwrap_or(Value::Null);
    let pid = p.get("payment_id").and_then(|v| v.as_str()).unwrap_or("");
    let live = state.ldk.payment_status(pid).await;
    let status = match live.as_str() {
        "SUCCEEDED" => "LIGHTNING_PAYMENT_SUCCEEDED",
        "FAILED" => "LIGHTNING_PAYMENT_FAILED",
        _ => "LIGHTNING_PAYMENT_INITIATED",
    };
    Ok(json!({ "request_lightning_send": {
        "request": {
            "__typename": "LightningSendRequest",
            "id": rec["id"],
            "created_at": rec.get("created_at").cloned().unwrap_or(Value::Null),
            "updated_at": now,
            "network": state.config.network,
            "encoded_invoice": p.get("encoded_invoice").cloned().unwrap_or(Value::Null),
            "fee": currency_amount(0),
            "idempotency_key": p.get("idempotency_key").cloned().unwrap_or(Value::Null),
            "status": status,
        }
    }}))
}
/// Compatibility-only request kinds: stubbed operations with no settlement
/// lifecycle. Their durable footprint is bounded by a payload allowlist, a
/// per-owner rolling quota, and TTL pruning (see `Db::prune_compat_requests`).
const COMPAT_QUOTA_WINDOW_HOURS: i64 = 24;
const MAX_COMPAT_REQUESTS_PER_OWNER: i64 = 1_000;
/// Hard ceiling for any persisted request payload. Real financial payloads
/// (invoices, offers) are a few KiB; anything larger is client padding.
const MAX_REQUEST_PAYLOAD_BYTES: usize = 16 * 1024;

/// Reject compatibility mutations once the owner already holds the quota of
/// compat rows inside the rolling window.
async fn enforce_compat_quota(state: &AppState, owner: &str) -> Result<(), String> {
    let since =
        (chrono::Utc::now() - chrono::Duration::hours(COMPAT_QUOTA_WINDOW_HOURS)).to_rfc3339();
    let count = state.db.compat_request_count(owner, &since).await?;
    if count >= MAX_COMPAT_REQUESTS_PER_OWNER {
        return Err("compatibility request quota reached; try again later".to_string());
    }
    Ok(())
}

/// Copy only the small known static-deposit fields. Unknown client JSON
/// (which may be megabytes of padding) must never reach durable storage.
fn static_deposit_payload(input: &Value) -> Value {
    let mut payload = serde_json::Map::new();
    for key in ["transaction_id", "output_index", "quote_signature"] {
        if let Some(value) = input.get(key) {
            payload.insert(key.to_string(), value.clone());
        }
    }
    Value::Object(payload)
}

/// Insert a user-request row into sqlite and return the record shape that
/// `user_request_union` reads: {id, type, created_at, payload}.
async fn store_request(
    state: &AppState,
    kind: &str,
    owner: &str,
    now: &str,
    payload: Value,
    idempotency_key: Option<&str>,
) -> Result<Value, String> {
    let serialized = serde_json::to_string(&payload).map_err(|e| e.to_string())?;
    if serialized.len() > MAX_REQUEST_PAYLOAD_BYTES {
        return Err("request payload is too large".to_string());
    }
    let id = Uuid::new_v4().to_string();
    state
        .db
        .insert_request(&id, kind, owner, now, &payload, idempotency_key)
        .await?;
    Ok(json!({
        "id": id, "type": kind,
        "owner_identity_pubkey": owner, "created_at": now,
        "payload": payload,
    }))
}

fn str_of(v: &Value, k: &str) -> String {
    match v.get(k) {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Number(value)) => value.to_string(),
        Some(Value::Bool(value)) => value.to_string(),
        _ => String::new(),
    }
}

fn validate_sats(amount: u64) -> Result<(), String> {
    if amount == 0 {
        return Err("amount_sats must be positive".to_string());
    }
    amount
        .checked_mul(1000)
        .map(|_| ())
        .ok_or_else(|| "amount_sats is too large".to_string())
}

fn validate_network(state: &AppState, requested: &str) -> Result<(), String> {
    validate_network_name(&state.config.network, requested)
}

fn validate_network_name(configured: &str, requested: &str) -> Result<(), String> {
    if requested == configured {
        Ok(())
    } else {
        Err(format!(
            "network mismatch: configured {configured}, requested {requested}"
        ))
    }
}
fn num_of(v: &Value, k: &str) -> u64 {
    v.get(k)
        .and_then(|x| x.as_u64().or_else(|| x.as_str()?.parse().ok()))
        .unwrap_or(0)
}
fn opt_num(v: &Value, k: &str) -> Option<u64> {
    v.get(k)
        .and_then(|x| x.as_u64().or_else(|| x.as_str()?.parse().ok()))
}
fn currency_amount(value: u64) -> Value {
    json!({
        "original_value": value,
        "original_unit": "SATOSHI",
        "preferred_currency_unit": "SATOSHI",
        "preferred_currency_value_rounded": value,
    })
}
fn ids_of(input: &Value, root: &Value) -> Vec<String> {
    for v in [input, root] {
        if let Some(a) = v.get("transfer_spark_ids").and_then(|x| x.as_array()) {
            return a
                .iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(a) = v.get("transferSparkIds").and_then(|x| x.as_array()) {
            return a
                .iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(s) = v.get("transfer_spark_id").and_then(|x| x.as_str()) {
            return vec![s.to_string()];
        }
    }
    vec![]
}

/// SSP signature with the identity key of the embedded Spark wallet.
async fn sign_with_ssp(state: &AppState, message: &str) -> Result<String, String> {
    state.spark.sign_message(message).await
}

#[cfg(test)]
mod tests {
    use super::{
        apply_query_aliases, currency_amount, static_deposit_payload, str_of, transfer_response,
        validate_network_name, validate_sats,
    };
    use serde_json::json;

    #[test]
    fn string_input_does_not_turn_null_into_text() {
        let input = json!({
            "missing": null,
            "object": {"unexpected": true},
            "string": "value",
            "number": 42,
        });

        assert_eq!(str_of(&input, "missing"), "");
        assert_eq!(str_of(&input, "object"), "");
        assert_eq!(str_of(&input, "string"), "value");
        assert_eq!(str_of(&input, "number"), "42");
    }

    #[test]
    fn currency_amount_includes_required_preferred_fields() {
        let amount = currency_amount(100);

        assert_eq!(amount["original_value"], 100);
        assert_eq!(amount["original_unit"], "SATOSHI");
        assert_eq!(amount["preferred_currency_unit"], "SATOSHI");
        assert_eq!(amount["preferred_currency_value_rounded"], 100);
    }

    #[test]
    fn receive_network_and_amount_are_validated() {
        assert!(validate_network_name("REGTEST", "REGTEST").is_ok());
        assert!(validate_network_name("REGTEST", "MAINNET").is_err());
        assert!(validate_sats(5_000).is_ok());
        assert!(validate_sats(0).is_err());
        assert!(validate_sats(u64::MAX).is_err());
    }

    #[test]
    fn transfers_include_the_full_receive_request() {
        let row = json!({
            "spark_id": "transfer-id",
            "total_amount_sats": 5000,
        });
        let request = json!({
            "__typename": "LightningReceiveRequest",
            "id": "request-id",
            "status": "TRANSFER_COMPLETED",
            "payment_preimage": "01".repeat(32),
            "invoice": {"encoded_invoice": "lnbcrt..."},
        });
        let mut transfer = transfer_response(&row, request);
        apply_query_aliases(
            &mut transfer,
            "lightning_request_status: status\nlightning_receive_payment_preimage: payment_preimage",
        );

        assert_eq!(transfer["spark_id"], "transfer-id");
        assert_eq!(
            transfer["user_request"]["lightning_request_status"],
            "TRANSFER_COMPLETED"
        );
        assert_eq!(
            transfer["user_request"]["lightning_receive_payment_preimage"],
            "01".repeat(32)
        );
        assert_eq!(
            transfer["user_request"]["invoice"]["encoded_invoice"],
            "lnbcrt..."
        );
    }

    #[test]
    fn static_deposit_payload_drops_unknown_client_fields() {
        let input = json!({
            "transaction_id": "00".repeat(32),
            "output_index": 0,
            "quote_signature": "sig",
            "padding": "x".repeat(64),
            "nested": {"deep": ["padding"]},
        });

        let payload = static_deposit_payload(&input);
        assert_eq!(payload["transaction_id"], json!("00".repeat(32)));
        assert_eq!(payload["output_index"], json!(0));
        assert_eq!(payload["quote_signature"], json!("sig"));
        assert!(payload.get("padding").is_none());
        assert!(payload.get("nested").is_none());
    }
}
