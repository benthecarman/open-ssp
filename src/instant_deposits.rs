//! Advance Spark credit before confirmation, then recover the reserved UTXO.
//! An uncertain operator reply never releases the exposure or changes the plan.
use crate::static_deposits::{
    outpoint, recovery_transaction, row, DepositQuote, StaticDepositService, StaticPlan,
};
use bitcoin::{
    consensus::{deserialize, serialize},
    Address, Amount, OutPoint, ScriptBuf, Transaction, TxOut,
};
use prost::Message;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};

#[derive(Clone, Serialize, Deserialize)]
struct InstantQuote {
    id: String,
    deposit: DepositQuote,
    height: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct InstantClaim {
    id: String,
    quote: InstantQuote,
    transfer_id: String,
    plan: StaticPlan,
    phase: String,
    signed_spend: Option<String>,
    scan_height: u64,
}

pub fn migrate(c: &rusqlite::Connection) -> rusqlite::Result<()> {
    c.execute_batch("CREATE TABLE IF NOT EXISTS instant_quotes(id TEXT PRIMARY KEY,txid TEXT NOT NULL,vout INTEGER NOT NULL,owner TEXT NOT NULL,expires INTEGER NOT NULL,data TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS instant_quote_outpoint ON instant_quotes(txid,vout,owner);
        CREATE TABLE IF NOT EXISTS instant_claims(id TEXT PRIMARY KEY,quote_id TEXT NOT NULL UNIQUE,txid TEXT NOT NULL,vout INTEGER NOT NULL,owner TEXT NOT NULL,address TEXT NOT NULL,value INTEGER NOT NULL,credit INTEGER NOT NULL,phase TEXT NOT NULL,data TEXT NOT NULL,last_error TEXT,UNIQUE(txid,vout));
        CREATE UNIQUE INDEX IF NOT EXISTS instant_pending_address_value ON instant_claims(address,value) WHERE phase!='SPEND_TX_CONFIRMED';
        CREATE UNIQUE INDEX IF NOT EXISTS instant_pending_owner ON instant_claims(owner) WHERE phase!='SPEND_TX_CONFIRMED';
        CREATE TRIGGER IF NOT EXISTS fixed_deposit_reservation BEFORE INSERT ON deposit_claims WHEN EXISTS(
          SELECT 1 FROM instant_claims WHERE (txid=NEW.txid AND vout=NEW.vout) OR (address=json_extract(NEW.data,'$.quote.address') AND value=json_extract(NEW.data,'$.quote.credit')+json_extract(NEW.data,'$.quote.fee') AND phase!='SPEND_TX_CONFIRMED')) BEGIN SELECT RAISE(ABORT,'deposit already reserved for an instant claim'); END;
        CREATE TRIGGER IF NOT EXISTS instant_deposit_reservation BEFORE INSERT ON instant_claims WHEN EXISTS(
          SELECT 1 FROM deposit_claims WHERE (txid=NEW.txid AND vout=NEW.vout) OR (json_extract(data,'$.quote.address')=NEW.address AND json_extract(data,'$.quote.credit')+json_extract(data,'$.quote.fee')=NEW.value AND status!='SUCCEEDED')) BEGIN SELECT RAISE(ABORT,'deposit already reserved for a confirmed claim'); END;")
}

// Spark's tagged hasher length-prefixes every value. Numeric values are u64 BE,
// including the Instant enum, which its AddUint8 method widens to u64.
pub(crate) fn authorization_digest(q: &DepositQuote) -> Result<[u8; 32], String> {
    fn add(h: &mut Sha256, bytes: &[u8]) {
        h.update((bytes.len() as u64).to_be_bytes());
        h.update(bytes);
    }
    let mut tag = Sha256::new();
    add(&mut tag, b"spark");
    add(&mut tag, b"claim_instant_static_deposit");
    let tag = tag.finalize();
    let mut h = Sha256::new();
    h.update(tag);
    h.update(tag);
    add(&mut h, q.network.to_lowercase().as_bytes());
    add(&mut h, &3u64.to_be_bytes());
    add(&mut h, &q.credit.to_be_bytes());
    add(&mut h, &0u64.to_be_bytes());
    add(&mut h, q.address.as_bytes());
    add(&mut h, &(q.credit + q.fee).to_be_bytes());
    add(
        &mut h,
        &hex::decode(&q.signature).map_err(|_| "invalid quote signature")?,
    );
    Ok(h.finalize().into())
}

fn sats(value: u64) -> Value {
    json!({"__typename":"CurrencyAmount","original_value":value,"original_unit":"SATOSHI","preferred_currency_unit":"SATOSHI","preferred_currency_value_rounded":value})
}

fn quote_response(q: &InstantQuote, claim: Option<&InstantClaim>) -> Value {
    json!({"__typename":"CreateInstantStaticDepositQuoteOutput",
        "quote":{"__typename":"StaticDepositQuote","id":q.id,"network":q.deposit.network,
            "transaction_id":q.deposit.txid,"output_index":q.deposit.vout,
            "deposit_amount":sats(q.deposit.credit+q.deposit.fee),"credit_amount":sats(q.deposit.credit),
            "quote_signature":q.deposit.signature,"expires_at":chrono::DateTime::from_timestamp(q.deposit.expires,0).map(|d|d.to_rfc3339())},
        "fulfillment_plans":[{"__typename":"StaticDepositPlan","id":q.id,"amount":sats(q.deposit.credit),
            "confirmations":0,"status":claim.map_or("CREATED",|r|if r.phase=="CREATED" {"CREATED"} else {"COMPLETED"}),
            "transfer_spark_id":claim.filter(|r|r.phase!="CREATED").map(|r|&r.transfer_id)}]})
}

fn check_exposure(
    value: u64,
    credit: u64,
    outstanding: u64,
    max_deposit: u64,
    limit: u64,
) -> Result<(), String> {
    if limit == 0 || max_deposit == 0 {
        return Err("instant deposits are disabled; configure SSP_INSTANT_MAX_OUTSTANDING_SATS and SSP_INSTANT_MAX_DEPOSIT_SATS".into());
    }
    if value > max_deposit {
        return Err("instant deposit exceeds the per-deposit limit".into());
    }
    if outstanding.checked_add(credit).is_none_or(|v| v > limit) {
        return Err("instant deposit exposure limit reached".into());
    }
    Ok(())
}

impl StaticDepositService {
    pub async fn instant_quote(&self, owner: &str, input: &Value) -> Result<Value, String> {
        self.validate_network(input)?;
        let (txid, vout) = outpoint(input)?;
        let _guard = self.lock.lock().await;
        let existing: Option<InstantClaim> = self
            .db
            .with(|c| {
                c.query_row(
                    "SELECT data FROM instant_claims WHERE txid=?1 AND vout=?2 AND owner=?3",
                    (&txid, vout, owner),
                    row,
                )
                .optional()
            })
            .await?;
        if let Some(record) = existing {
            return Ok(quote_response(&record.quote, Some(&record)));
        }
        check_exposure(0, 0, 0, self.instant_max_deposit, self.instant_limit)?;
        let coin = self
            .bitcoin
            .bitcoin_rpc("gettxout", json!([txid, vout, true]))
            .await?;
        if coin.is_null() {
            return Err("instant deposit output is spent or unknown".into());
        }
        let value = crate::coop_exit::btc_amount(&coin["value"])?;
        check_exposure(value, 0, 0, self.instant_max_deposit, self.instant_limit)?;
        // A mempool entry must exist for an unconfirmed output. All inputs must
        // already be confirmed; an ancestor chain must not consume this budget.
        if coin["confirmations"].as_u64() == Some(0) {
            let entry = self
                .bitcoin
                .bitcoin_rpc("getmempoolentry", json!([txid]))
                .await?;
            if entry["ancestorcount"].as_u64() != Some(1) {
                return Err("instant deposit must spend confirmed inputs".into());
            }
        }
        let script = ScriptBuf::from_hex(
            coin["scriptPubKey"]["hex"]
                .as_str()
                .ok_or("deposit has no script")?,
        )
        .map_err(|e| e.to_string())?;
        let address = Address::from_script(&script, self.bitcoin_network())
            .map_err(|e| e.to_string())?
            .to_string();
        let metadata = self.spark.static_deposit_address(owner, &address).await?;
        let claimed: bool = self
            .db
            .with(|c| {
                c.query_row(
                    "SELECT EXISTS(SELECT 1 FROM deposit_claims WHERE txid=?1 AND vout=?2)",
                    (&txid, vout),
                    |r| r.get(0),
                )
            })
            .await?;
        if claimed {
            return Err("deposit already has a confirmed claim".into());
        }
        let old: Option<InstantQuote> = self.db.with(|c| c.query_row(
            "SELECT data FROM instant_quotes WHERE txid=?1 AND vout=?2 AND owner=?3 AND expires>?4 ORDER BY rowid DESC LIMIT 1",
            (&txid,vout,owner,chrono::Utc::now().timestamp()),row).optional()).await?;
        if let Some(old) = old {
            return Ok(quote_response(&old, None));
        }
        let estimate = self
            .bitcoin
            .bitcoin_rpc("estimatesmartfee", json!([6, "CONSERVATIVE"]))
            .await?;
        let rate = if let Some(rate) = estimate.get("feerate") {
            crate::coop_exit::btc_amount(rate)?.div_ceil(1000).max(1)
        } else if self.bitcoin_network() == bitcoin::Network::Regtest {
            1
        } else {
            return Err("Bitcoin fee estimate unavailable".into());
        };
        let fee = 99u64
            .checked_mul(rate)
            .ok_or("instant deposit fee overflow")?;
        let credit = value
            .checked_sub(fee)
            .filter(|v| *v >= 330)
            .ok_or("instant deposit is too small after the recovery fee")?;
        let outstanding = self.instant_outstanding().await?;
        check_exposure(
            value,
            credit,
            outstanding,
            self.instant_max_deposit,
            self.instant_limit,
        )?;
        let height = self
            .bitcoin
            .bitcoin_rpc("getblockcount", json!([]))
            .await?
            .as_u64()
            .ok_or("Bitcoin height unavailable")?;
        let id = uuid::Uuid::new_v4().to_string();
        let expires = chrono::Utc::now().timestamp() + 300;
        let digest = Sha256::digest(
            json!([
                "open-ssp-instant-deposit-v1",
                id,
                self.network,
                owner,
                txid,
                vout,
                value,
                credit,
                expires
            ])
            .to_string()
            .as_bytes(),
        )
        .into();
        let quote = InstantQuote {
            id,
            height,
            deposit: DepositQuote {
                txid,
                vout,
                owner: owner.into(),
                network: self.network.clone(),
                credit,
                fee,
                signature: self.spark.sign_digest(digest),
                expires,
                address,
                signing_key: hex::encode(metadata.user_signing_public_key),
                verifying_key: hex::encode(metadata.verifying_public_key),
                prev_output: hex::encode(serialize(&TxOut {
                    value: Amount::from_sat(value),
                    script_pubkey: script,
                })),
            },
        };
        self.db.with(|c| {
            let tx=c.unchecked_transaction()?;
            tx.execute("DELETE FROM instant_quotes WHERE expires<=?1 AND id NOT IN (SELECT quote_id FROM instant_claims)",[chrono::Utc::now().timestamp()])?;
            let count:u64=tx.query_row("SELECT count(*) FROM instant_quotes WHERE owner=?1 AND id NOT IN (SELECT quote_id FROM instant_claims)",[owner],|r|r.get(0))?;
            if count>=100 { return Err(rusqlite::Error::ToSqlConversionFailure("instant quote limit reached".into())); }
            tx.execute("INSERT INTO instant_quotes VALUES(?1,?2,?3,?4,?5,?6)",(&quote.id,&quote.deposit.txid,quote.deposit.vout,owner,expires,json!(quote).to_string()))?;
            tx.commit()
        }).await?;
        Ok(quote_response(&quote, None))
    }

    pub async fn instant_outstanding(&self) -> Result<u64, String> {
        self.db.with(|c|c.query_row("SELECT COALESCE(sum(credit),0) FROM instant_claims WHERE phase!='SPEND_TX_CONFIRMED'",[],|r|r.get(0))).await
    }

    pub async fn instant_claim(&self, owner: &str, input: &Value) -> Result<Value, String> {
        let quote_id = input["static_deposit_quote_id"]
            .as_str()
            .ok_or("static_deposit_quote_id required")?;
        let _guard = self.lock.lock().await;
        let _liquidity = self.spark.deposit_liquidity_lock().await;
        let existing: Option<InstantClaim> = self
            .db
            .with(|c| {
                c.query_row(
                    "SELECT data FROM instant_claims WHERE quote_id=?1 AND owner=?2",
                    (quote_id, owner),
                    row,
                )
                .optional()
            })
            .await?;
        if let Some(mut record) = existing {
            // Session ownership is enough to replay a persisted authorization.
            self.advance_instant(&mut record).await?;
            return Ok(
                json!({"__typename":"CreateClaimInstantStaticDepositOutput","claim_id":record.id}),
            );
        }
        let quote: InstantQuote = self
            .db
            .with(|c| {
                c.query_row(
                    "SELECT data FROM instant_quotes WHERE id=?1 AND owner=?2 AND expires>?3",
                    (quote_id, owner, chrono::Utc::now().timestamp()),
                    row,
                )
            })
            .await?;
        let q = &quote.deposit;
        let outstanding = self.instant_outstanding().await?;
        check_exposure(
            q.credit + q.fee,
            q.credit,
            outstanding,
            self.instant_max_deposit,
            self.instant_limit,
        )?;
        let secret = input["static_deposit_address_private_key_share"]
            .as_str()
            .ok_or("deposit key share required")?;
        let signature = input["signature"]
            .as_str()
            .ok_or("instant deposit signature required")?;
        let encrypted = self.spark.instant_authorization(q, secret, signature)?;
        // Recheck the exact output before the first advance. Later RBF changes
        // are handled by recovery; they never authorize another advance.
        let coin = self
            .bitcoin
            .bitcoin_rpc("gettxout", json!([q.txid, q.vout, true]))
            .await?;
        let prev: TxOut = deserialize(&hex::decode(&q.prev_output).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        if coin.is_null()
            || crate::coop_exit::btc_amount(&coin["value"])? != q.credit + q.fee
            || coin["scriptPubKey"]["hex"] != hex::encode(prev.script_pubkey.as_bytes())
        {
            return Err("quoted deposit output is spent or changed; request a new quote".into());
        }
        if coin["confirmations"].as_u64() == Some(0) {
            let entry = self
                .bitcoin
                .bitcoin_rpc("getmempoolentry", json!([q.txid]))
                .await?;
            if entry["ancestorcount"].as_u64() != Some(1) {
                return Err("instant deposit must spend confirmed inputs".into());
            }
        }
        let busy:bool=self.db.with(|c|c.query_row("SELECT EXISTS(SELECT 1 FROM instant_claims WHERE (owner=?1 OR (address=?2 AND value=?3)) AND phase!='SPEND_TX_CONFIRMED') OR EXISTS(SELECT 1 FROM deposit_claims WHERE txid=?4 AND vout=?5)",(owner,&q.address,q.credit+q.fee,&q.txid,q.vout),|r|r.get(0))).await?;
        if busy {
            return Err("wallet or deposit already has a pending claim".into());
        }
        let destination = self.bitcoin.bitcoin_change_address().await?;
        let spend = recovery_transaction(
            OutPoint::new(q.txid.parse().map_err(|_| "invalid txid")?, q.vout),
            q.credit,
            destination.script_pubkey(),
        );
        let transfer_id = uuid::Uuid::new_v4().to_string();
        let plan = self
            .spark
            .prepare_static_claim(q, &encrypted, signature, &transfer_id, &spend)
            .await?;
        let record = InstantClaim {
            id: uuid::Uuid::new_v4().to_string(),
            scan_height: quote.height.saturating_sub(6),
            quote,
            transfer_id,
            plan,
            phase: "CREATED".into(),
            signed_spend: None,
        };
        self.db.with(|c|{
            let tx=c.unchecked_transaction()?;
            // Check again in the write transaction, including across processes.
            let exposure:u64=tx.query_row("SELECT COALESCE(sum(credit),0) FROM instant_claims WHERE phase!='SPEND_TX_CONFIRMED'",[],|r|r.get(0))?;
            check_exposure(record.quote.deposit.credit+record.quote.deposit.fee,record.quote.deposit.credit,exposure,self.instant_max_deposit,self.instant_limit)
                .map_err(|e|rusqlite::Error::ToSqlConversionFailure(e.into()))?;
            let q=&record.quote.deposit;
            tx.execute("INSERT INTO instant_claims(id,quote_id,txid,vout,owner,address,value,credit,phase,data) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",rusqlite::params![record.id,record.quote.id,q.txid,q.vout,owner,q.address,q.credit+q.fee,q.credit,record.phase,json!(record).to_string()])?;
            let payload=instant_payload(&record);
            tx.execute("INSERT INTO requests(id,kind,owner,created_at,payload) VALUES(?1,'CLAIM_INSTANT_STATIC_DEPOSIT_V2',?2,?3,?4)",(&record.id,owner,chrono::Utc::now().to_rfc3339(),payload.to_string()))?;
            tx.commit()
        }).await?;
        let mut record = record;
        self.advance_instant(&mut record).await?;
        Ok(json!({"__typename":"CreateClaimInstantStaticDepositOutput","claim_id":record.id}))
    }

    async fn save_instant(&self, r: &InstantClaim) -> Result<(), String> {
        self.db.with(|c|{
            let tx=c.unchecked_transaction()?;
            tx.execute("UPDATE instant_claims SET phase=?2,data=?3,last_error=NULL WHERE id=?1",(&r.id,&r.phase,json!(r).to_string()))?;
            tx.execute("UPDATE requests SET payload=?2 WHERE id=?1",(&r.id,instant_payload(r).to_string()))?;
            if r.phase!="CREATED" {
                tx.execute("INSERT INTO transfers(spark_id,request_id,kind,status,owner) VALUES(?1,?2,'CLAIM_INSTANT_STATIC_DEPOSIT','COMPLETED',?3) ON CONFLICT(spark_id) DO NOTHING",(&r.transfer_id,&r.id,&r.quote.deposit.owner))?;
            }
            tx.commit()
        }).await
    }

    async fn advance_instant(&self, r: &mut InstantClaim) -> Result<(), String> {
        if r.phase == "SPEND_TX_CONFIRMED" {
            return Ok(());
        }
        if r.phase == "CREATED" {
            self.spark
                .submit_instant_reserve(&r.quote.deposit, &r.transfer_id, &r.plan)
                .await?;
            r.phase = "TRANSFER_COMPLETED".into();
            self.save_instant(r).await?;
        }
        if r.phase == "TRANSFER_COMPLETED" {
            let Some(point) = self.confirmed_instant_outpoint(r).await? else {
                return Ok(());
            };
            // Freeze the replacement outpoint BEFORE a signing RPC. Once a nonce
            // can have been used, retries must use this exact transaction.
            let mut request =
                ::spark::operator::rpc::spark_ssp_internal::StaticDepositSwapRequest::decode(
                    r.plan.request.as_slice(),
                )
                .map_err(|e| e.to_string())?;
            let job = request
                .spend_tx_signing_job
                .as_mut()
                .ok_or("instant recovery has no signing job")?;
            let mut spend: Transaction = deserialize(&job.raw_tx).map_err(|e| e.to_string())?;
            spend.input[0].previous_output = point;
            job.raw_tx = serialize(&spend);
            let coin = request
                .on_chain_utxo
                .as_mut()
                .ok_or("instant recovery has no UTXO")?;
            coin.txid = hex::decode(point.txid.to_string()).map_err(|e| e.to_string())?;
            coin.vout = point.vout;
            r.plan.request = request.encode_to_vec();
            r.phase = "RECOVERY_PREPARED".into();
            self.save_instant(r).await?;
        }
        if r.signed_spend.is_none() {
            let spend = self
                .spark
                .submit_static_claim(&r.quote.deposit, &r.transfer_id, &r.plan, true)
                .await?;
            r.signed_spend = Some(hex::encode(serialize(&spend)));
            r.phase = "SPEND_TX_CREATED".into();
            self.save_instant(r).await?;
        }
        let raw = r
            .signed_spend
            .as_ref()
            .ok_or("instant recovery has no signed transaction")?;
        let spend: Transaction = deserialize(&hex::decode(raw).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let observed = self
            .bitcoin
            .bitcoin_rpc("gettransaction", json!([spend.compute_txid().to_string()]))
            .await;
        let confirmations = observed
            .as_ref()
            .ok()
            .and_then(|v| v["confirmations"].as_i64())
            .unwrap_or(0);
        if confirmations <= 0 {
            self.bitcoin
                .bitcoin_rpc("sendrawtransaction", json!([raw]))
                .await?;
        }
        r.phase = if confirmations >= 3 {
            "SPEND_TX_CONFIRMED"
        } else {
            "SPEND_TX_BROADCAST"
        }
        .into();
        self.save_instant(r).await
    }

    async fn confirmed_instant_outpoint(
        &self,
        r: &mut InstantClaim,
    ) -> Result<Option<OutPoint>, String> {
        let q = &r.quote.deposit;
        let coin = self
            .bitcoin
            .bitcoin_rpc("gettxout", json!([q.txid, q.vout, true]))
            .await?;
        if !coin.is_null() {
            return if coin["confirmations"].as_u64().unwrap_or(0) > 0 {
                Ok(Some(OutPoint::new(
                    q.txid.parse().map_err(|_| "invalid txid")?,
                    q.vout,
                )))
            } else {
                Ok(None)
            };
        }
        // The original can be replaced. Search blocks from the quote height
        // for the same script and amount; never scan the whole chain at once.
        let tip = self
            .bitcoin
            .bitcoin_rpc("getblockcount", json!([]))
            .await?
            .as_u64()
            .ok_or("Bitcoin height unavailable")?;
        let start = r.scan_height.min(tip);
        let end = start.saturating_add(20).min(tip);
        let prev: TxOut = deserialize(&hex::decode(&q.prev_output).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        for height in start..=end {
            let hash = self
                .bitcoin
                .bitcoin_rpc("getblockhash", json!([height]))
                .await?;
            let block = self
                .bitcoin
                .bitcoin_rpc("getblock", json!([hash, 2]))
                .await?;
            for tx in block["tx"]
                .as_array()
                .ok_or("Bitcoin block has no transactions")?
            {
                for output in tx["vout"]
                    .as_array()
                    .ok_or("Bitcoin transaction has no outputs")?
                {
                    if output["scriptPubKey"]["hex"] == hex::encode(prev.script_pubkey.as_bytes())
                        && crate::coop_exit::btc_amount(&output["value"])? == q.credit + q.fee
                    {
                        let txid = tx["txid"].as_str().ok_or("Bitcoin transaction has no ID")?;
                        let vout = output["n"].as_u64().ok_or("Bitcoin output has no index")?;
                        let unspent = self
                            .bitcoin
                            .bitcoin_rpc("gettxout", json!([txid, vout, true]))
                            .await?;
                        if !unspent.is_null() {
                            return Ok(Some(OutPoint::new(
                                txid.parse().map_err(|_| "invalid replacement txid")?,
                                vout as u32,
                            )));
                        }
                    }
                }
            }
        }
        r.scan_height = if end == tip {
            tip.saturating_sub(6)
        } else {
            end + 1
        };
        self.save_instant(r).await?;
        Ok(None)
    }

    pub async fn run_instant(self: Arc<Self>) {
        loop {
            let records:Result<Vec<InstantClaim>,String>=self.db.with(|c|{
                let mut s=c.prepare("SELECT data FROM instant_claims WHERE phase!='SPEND_TX_CONFIRMED' ORDER BY random() LIMIT 20")?;
                let rows=s.query_map([],row)?;
                rows.collect()
            }).await;
            match records {
                Ok(records) => {
                    for record in records {
                        let _guard = self.lock.lock().await;
                        let _liquidity = self.spark.deposit_liquidity_lock().await;
                        // A foreground retry may have advanced the row since the batch read.
                        let current = self
                            .db
                            .with(|c| {
                                c.query_row(
                                    "SELECT data FROM instant_claims WHERE id=?1",
                                    [&record.id],
                                    row::<InstantClaim>,
                                )
                            })
                            .await;
                        if let Ok(mut current) = current {
                            if let Err(error) = self.advance_instant(&mut current).await {
                                tracing::warn!(
                                    request_id = current.id,
                                    "instant deposit recovery pending: {error}"
                                );
                                let _ = self
                                    .db
                                    .with(|c| {
                                        c.execute(
                                            "UPDATE instant_claims SET last_error=?2 WHERE id=?1",
                                            (&current.id, &error),
                                        )
                                    })
                                    .await;
                            }
                        }
                    }
                }
                Err(error) => tracing::warn!("instant deposit worker: {error}"),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

fn instant_payload(r: &InstantClaim) -> Value {
    let q = &r.quote.deposit;
    json!({"network":q.network,"instant":true,"transaction_id":q.txid,"output_index":q.vout,
        "credit_amount_sats":q.credit,"deposit_amount_sats":q.credit+q.fee,"max_fee_sats":q.fee,
        "status":if r.phase=="CREATED" {"IN_PROGRESS"} else {"SUCCEEDED"},
        "phase":if r.phase=="RECOVERY_PREPARED" {"TRANSFER_COMPLETED"} else {&r.phase},
        "transfer_spark_id":if r.phase=="CREATED" {None} else {Some(&r.transfer_id)}})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn database_reservations_exclude_duplicate_and_confirmed_claims() {
        let dir = std::env::temp_dir().join(format!("ssp-instant-test-{}", uuid::Uuid::new_v4()));
        let db = crate::db::Db::open(dir.to_str().unwrap()).unwrap();
        db.with(|c| c.execute("INSERT INTO instant_claims(id,quote_id,txid,vout,owner,address,value,credit,phase,data) VALUES('instant','quote','tx',0,'alice','address',10000,9901,'TRANSFER_COMPLETED','{}')",[])).await.unwrap();
        let fixed = json!({"quote":{"address":"address","credit":9901,"fee":99}}).to_string();
        for txid in ["tx", "replacement"] {
            assert!(db.with(|c|c.execute("INSERT INTO deposit_claims(id,txid,vout,owner,status,data) VALUES('fixed',?1,0,'alice','IN_PROGRESS',?2)",(txid,&fixed))).await.is_err());
        }
        assert!(db.with(|c|c.execute("INSERT INTO instant_claims(id,quote_id,txid,vout,owner,address,value,credit,phase,data) VALUES('duplicate','other','replacement',0,'bob','address',10000,9901,'CREATED','{}')",[])).await.is_err());
        drop(db);
        let db = crate::db::Db::open(dir.to_str().unwrap()).unwrap();
        // Restart must preserve the exposure and reservation.
        let credit: u64 = db
            .with(|c| {
                c.query_row(
                    "SELECT sum(credit) FROM instant_claims WHERE phase!='SPEND_TX_CONFIRMED'",
                    [],
                    |r| r.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(credit, 9901);
        db.with(|c| {
            c.execute(
                "UPDATE instant_claims SET phase='SPEND_TX_CONFIRMED' WHERE id='instant'",
                [],
            )
        })
        .await
        .unwrap();
        // A new output is eligible, but the original outpoint remains consumed.
        assert!(db.with(|c|c.execute("INSERT INTO deposit_claims(id,txid,vout,owner,status,data) VALUES('fixed','tx',0,'alice','IN_PROGRESS',?1)",[&fixed])).await.is_err());
        db.with(|c|c.execute("INSERT INTO deposit_claims(id,txid,vout,owner,status,data) VALUES('fixed','new-tx',0,'alice','IN_PROGRESS',?1)",[&fixed])).await.unwrap();
        assert!(db.with(|c|c.execute("INSERT INTO instant_claims(id,quote_id,txid,vout,owner,address,value,credit,phase,data) VALUES('conflict','other','new-tx',0,'alice','address',10000,9901,'CREATED','{}')",[])).await.is_err());
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn advances_are_bounded_including_overflow_and_disabled_policy() {
        assert!(check_exposure(10000, 9901, 0, 10000, 9901).is_ok());
        assert!(check_exposure(10000, 9901, 1, 10000, 9901).is_err());
        assert!(check_exposure(10001, 9902, 0, 10000, 20000).is_err());
        assert!(check_exposure(10000, 9901, 0, 10000, 0).is_err());
        assert!(check_exposure(10000, 9901, u64::MAX, 10000, u64::MAX).is_err());
    }
}
