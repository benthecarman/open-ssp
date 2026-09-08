//! Confirmed deposits: quote real UTXOs, commit the Spark payout, then recover
//! the Bitcoin output. The operator request and FROST nonce survive restarts.
use crate::{coop_exit::CoopExitService, db::Db, spark::SparkService};
use bitcoin::{
    absolute::LockTime,
    consensus::{deserialize, serialize},
    transaction::Version,
    Address, Amount, Network, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{str::FromStr, sync::Arc, time::Duration};

#[derive(Clone, Serialize, Deserialize)]
pub struct DepositQuote {
    pub txid: String,
    pub vout: u32,
    pub owner: String,
    pub network: String,
    pub credit: u64,
    pub fee: u64,
    pub signature: String,
    pub expires: i64,
    pub address: String,
    pub signing_key: String,
    pub verifying_key: String,
    pub prev_output: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct StaticPlan {
    pub request: Vec<u8>,
    pub nonce_ciphertext: Vec<u8>,
    pub encrypted_key: Vec<u8>,
    pub prev_output: String,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct DepositClaim {
    pub id: String,
    pub quote: DepositQuote,
    pub transfer_id: String,
    pub plan: StaticPlan,
    pub signed_spend: Option<String>,
    pub status: String,
    #[serde(default)]
    pub phase: String,
}
pub struct StaticDepositService {
    pub(crate) db: Arc<Db>,
    pub(crate) spark: Arc<SparkService>,
    pub(crate) bitcoin: Arc<CoopExitService>,
    pub(crate) network: String,
    pub(crate) lock: tokio::sync::Mutex<()>,
    pub(crate) instant_limit: u64,
    pub(crate) instant_max_deposit: u64,
}
pub fn migrate(c: &rusqlite::Connection) -> rusqlite::Result<()> {
    c.execute_batch("CREATE TABLE IF NOT EXISTS deposit_quotes(txid TEXT NOT NULL,vout INTEGER NOT NULL,owner TEXT NOT NULL,expires INTEGER NOT NULL,data TEXT NOT NULL,PRIMARY KEY(txid,vout));
        CREATE TABLE IF NOT EXISTS deposit_claims(id TEXT PRIMARY KEY,txid TEXT NOT NULL,vout INTEGER NOT NULL,owner TEXT NOT NULL,status TEXT NOT NULL,data TEXT NOT NULL,last_error TEXT,UNIQUE(txid,vout));")
}
pub(crate) fn recovery_transaction(
    outpoint: OutPoint,
    credit: u64,
    destination: ScriptBuf,
) -> Transaction {
    Transaction {
        version: Version(3),
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: outpoint,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(credit),
            script_pubkey: destination,
        }],
    }
}

fn quote_response(quote: &DepositQuote) -> Value {
    json!({"__typename":"StaticDepositQuoteOutput","transaction_id":quote.txid,"output_index":quote.vout,"network":quote.network,"credit_amount_sats":quote.credit,"signature":quote.signature})
}
pub(crate) fn outpoint(input: &Value) -> Result<(String, u32), String> {
    let txid = bitcoin::Txid::from_str(
        input["transaction_id"]
            .as_str()
            .ok_or("transaction_id required")?,
    )
    .map_err(|_| "invalid transaction ID")?
    .to_string();
    let vout = u32::try_from(
        input["output_index"]
            .as_u64()
            .ok_or("output_index required")?,
    )
    .map_err(|_| "invalid output index")?;
    Ok((txid, vout))
}
pub(crate) fn row<T: serde::de::DeserializeOwned>(r: &rusqlite::Row<'_>) -> rusqlite::Result<T> {
    let text: String = r.get(0)?;
    serde_json::from_str(&text).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}
impl StaticDepositService {
    pub fn new(
        db: Arc<Db>,
        spark: Arc<SparkService>,
        bitcoin: Arc<CoopExitService>,
        network: String,
        instant_limit: u64,
        instant_max_deposit: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            db,
            spark,
            bitcoin,
            network,
            instant_limit,
            instant_max_deposit,
            lock: tokio::sync::Mutex::new(()),
        })
    }
    pub(crate) fn validate_network(&self, input: &Value) -> Result<(), String> {
        if input["network"].as_str().is_some_and(|n| n != self.network) {
            return Err("static deposit network mismatch".into());
        }
        Ok(())
    }
    pub(crate) fn bitcoin_network(&self) -> Network {
        match self.network.as_str() {
            "MAINNET" => Network::Bitcoin,
            "TESTNET" => Network::Testnet,
            "SIGNET" => Network::Signet,
            _ => Network::Regtest,
        }
    }
    pub async fn quote(&self, owner: &str, input: &Value) -> Result<Value, String> {
        self.validate_network(input)?;
        let (txid, vout) = outpoint(input)?;
        let _guard = self.lock.lock().await;
        let claimed: Option<DepositClaim> = self
            .db
            .with(|c| {
                c.query_row(
                    "SELECT data FROM deposit_claims WHERE txid=?1 AND vout=?2",
                    (&txid, vout),
                    row,
                )
                .optional()
            })
            .await?;
        if let Some(claimed) = claimed {
            if claimed.quote.owner != owner {
                return Err("deposit belongs to another wallet".into());
            }
            return Ok(quote_response(&claimed.quote));
        }
        let coin = self
            .bitcoin
            .bitcoin_rpc("gettxout", json!([txid, vout, true]))
            .await?;
        if coin.is_null() || coin["confirmations"].as_u64().unwrap_or(0) < 3 {
            return Err("static deposit needs an unspent output with 3 confirmations".into());
        }
        let value = crate::coop_exit::btc_amount(&coin["value"])?;
        let script = ScriptBuf::from_hex(
            coin["scriptPubKey"]["hex"]
                .as_str()
                .ok_or("Bitcoin returned no output script")?,
        )
        .map_err(|e| e.to_string())?;
        let address = Address::from_script(&script, self.bitcoin_network())
            .map_err(|_| "unsupported static deposit script")?;
        let deposit = self
            .spark
            .static_deposit_address(owner, &address.to_string())
            .await?;
        let old:Option<DepositQuote>=self.db.with(|c|c.query_row("SELECT data FROM deposit_quotes WHERE txid=?1 AND vout=?2 AND owner=?3 AND expires>?4",(&txid,vout,owner,chrono::Utc::now().timestamp()),row).optional()).await?;
        let quote = if let Some(old) = old {
            old
        } else {
            let estimate = self
                .bitcoin
                .bitcoin_rpc("estimatesmartfee", json!([6, "CONSERVATIVE"]))
                .await?;
            let rate = if let Some(rate) = estimate.get("feerate") {
                crate::coop_exit::btc_amount(rate)?.div_ceil(1000).max(1)
            } else if self.bitcoin_network() == Network::Regtest {
                1
            } else {
                return Err("Bitcoin fee estimate unavailable".into());
            };
            // One Taproot input and one native SegWit recovery output.
            let fee = 99_u64.checked_mul(rate).ok_or("deposit fee overflow")?;
            let credit = value
                .checked_sub(fee)
                .filter(|v| *v >= 330)
                .ok_or("static deposit is too small after the recovery fee")?;
            let expires = chrono::Utc::now().timestamp() + 300;
            let digest: [u8; 32] = Sha256::digest(
                json!([
                    "open-ssp-static-deposit-v1",
                    self.network,
                    txid,
                    vout,
                    owner,
                    credit,
                    expires
                ])
                .to_string()
                .as_bytes(),
            )
            .into();
            let quote = DepositQuote {
                txid: txid.clone(),
                vout,
                owner: owner.into(),
                network: self.network.clone(),
                credit,
                fee,
                signature: self.spark.sign_digest(digest),
                expires,
                address: address.to_string(),
                signing_key: hex::encode(deposit.user_signing_public_key),
                verifying_key: hex::encode(deposit.verifying_public_key),
                prev_output: hex::encode(serialize(&TxOut {
                    value: Amount::from_sat(value),
                    script_pubkey: script,
                })),
            };
            self.db.with(|c| {
                let tx=c.unchecked_transaction()?;
                tx.execute("DELETE FROM deposit_quotes WHERE expires<=?1",[chrono::Utc::now().timestamp()])?;
                let count:u64=tx.query_row("SELECT count(*) FROM deposit_quotes WHERE owner=?1",[owner],|r|r.get(0))?;
                if count>=100{return Err(rusqlite::Error::ToSqlConversionFailure("static deposit quote limit reached".into()));}
                tx.execute("INSERT INTO deposit_quotes VALUES(?1,?2,?3,?4,?5) ON CONFLICT(txid,vout) DO UPDATE SET owner=excluded.owner,expires=excluded.expires,data=excluded.data",(&txid,vout,owner,expires,json!(quote).to_string()))?;
                tx.commit()
            }).await?;
            quote
        };
        Ok(quote_response(&quote))
    }
    pub async fn claim(&self, owner: &str, input: &Value) -> Result<String, String> {
        self.validate_network(input)?;
        let (txid, vout) = outpoint(input)?;
        let _guard = self.lock.lock().await;
        let _liquidity = self.spark.deposit_liquidity_lock().await;
        let mut existing: Option<DepositClaim> = self
            .db
            .with(|c| {
                c.query_row(
                    "SELECT data FROM deposit_claims WHERE txid=?1 AND vout=?2 AND owner=?3",
                    (&txid, vout, owner),
                    row,
                )
                .optional()
            })
            .await?;
        if let Some(record) = existing.as_mut() {
            if input["quote_signature"].as_str() != Some(&record.quote.signature) {
                return Err("claim quote signature mismatch".into());
            }
            self.advance(record).await?;
            return Ok(record.transfer_id.clone());
        }
        let quote:DepositQuote=self.db.with(|c|c.query_row("SELECT data FROM deposit_quotes WHERE txid=?1 AND vout=?2 AND owner=?3 AND expires>?4",(&txid,vout,owner,chrono::Utc::now().timestamp()),row)).await?;
        if input["quote_signature"].as_str() != Some(&quote.signature)
            || input["credit_amount_sats"].as_u64() != Some(quote.credit)
            || input["request_type"].as_str() != Some("FIXED_AMOUNT")
        {
            return Err("static claim does not match its fixed-amount quote".into());
        }
        let encrypted = input["encrypted_deposit_secret_key"]
            .as_str()
            .ok_or("encrypted_deposit_secret_key required")?;
        let signature = input["signature"]
            .as_str()
            .ok_or("user signature required")?;
        self.spark
            .validate_static_authorization(&quote, encrypted, signature)?;
        // Recheck the chain immediately before preparing a payout. Operators
        // independently check confirmations and the unique UTXO claim slot.
        let coin = self
            .bitcoin
            .bitcoin_rpc("gettxout", json!([txid, vout, true]))
            .await?;
        if coin.is_null() || coin["confirmations"].as_u64().unwrap_or(0) < 3 {
            return Err("deposit is spent or no longer confirmed".into());
        }
        let destination = self.bitcoin.bitcoin_change_address().await?;
        let spend = recovery_transaction(
            OutPoint::new(txid.parse().map_err(|_| "invalid txid")?, vout),
            quote.credit,
            destination.script_pubkey(),
        );
        let transfer_id = uuid::Uuid::new_v4().to_string();
        let plan = self
            .spark
            .prepare_static_claim(&quote, encrypted, signature, &transfer_id, &spend)
            .await?;
        let mut record = DepositClaim {
            id: uuid::Uuid::new_v4().to_string(),
            quote,
            transfer_id,
            plan,
            signed_spend: None,
            status: "IN_PROGRESS".into(),
            phase: "CREATED".into(),
        };
        self.db.with(|c| {
            let tx=c.unchecked_transaction()?;
            tx.execute("INSERT INTO deposit_claims(id,txid,vout,owner,status,data) VALUES(?1,?2,?3,?4,?5,?6)",(&record.id,&txid,vout,owner,&record.status,json!(record).to_string()))?;
            let payload=json!({"network":self.network,"transaction_id":txid,"output_index":vout,"credit_amount_sats":record.quote.credit,"deposit_amount_sats":record.quote.credit+record.quote.fee,"max_fee_sats":record.quote.fee,"status":"IN_PROGRESS","phase":record.phase,"transfer_spark_id":record.transfer_id}).to_string();
            tx.execute("INSERT INTO requests(id,kind,owner,created_at,payload) VALUES(?1,'CLAIM_STATIC_DEPOSIT',?2,?3,?4)",(&record.id,owner,chrono::Utc::now().to_rfc3339(),payload))?;
            tx.commit()
        }).await?;
        self.advance(&mut record).await?;
        Ok(record.transfer_id)
    }
    async fn advance(&self, record: &mut DepositClaim) -> Result<(), String> {
        if record.phase == "SPEND_TX_CONFIRMED" {
            return Ok(());
        }
        if record.signed_spend.is_none() {
            let raw = self
                .spark
                .submit_static_claim(&record.quote, &record.transfer_id, &record.plan, false)
                .await?;
            record.signed_spend = Some(hex::encode(serialize(&raw)));
            record.phase = "SPEND_TX_CREATED".into();
            self.save(record).await?;
        }
        let raw = record
            .signed_spend
            .as_ref()
            .ok_or("deposit has no recovery transaction")?;
        let tx: Transaction = deserialize(&hex::decode(raw).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        // A confirmed recovery may be rejected as already in chain on rebroadcast.
        let observed = self
            .bitcoin
            .bitcoin_rpc("gettransaction", json!([tx.compute_txid().to_string()]))
            .await;
        let confirmed = observed
            .as_ref()
            .ok()
            .is_some_and(|v| v["confirmations"].as_i64().unwrap_or(0) > 0);
        if !confirmed {
            self.bitcoin
                .bitcoin_rpc("sendrawtransaction", json!([raw]))
                .await?;
        }
        record.status = "SUCCEEDED".into();
        record.phase = if confirmed {
            "SPEND_TX_CONFIRMED"
        } else {
            "SPEND_TX_BROADCAST"
        }
        .into();
        self.save(record).await
    }
    async fn save(&self, record: &DepositClaim) -> Result<(), String> {
        self.db.with(|c| {
            let tx=c.unchecked_transaction()?;
            tx.execute("UPDATE deposit_claims SET status=?2,data=?3,last_error=NULL WHERE id=?1",(&record.id,&record.status,json!(record).to_string()))?;
            tx.execute("UPDATE requests SET payload=json_set(payload,'$.status',?2,'$.phase',?3) WHERE id=?1",(&record.id,&record.status,&record.phase))?;
            if record.signed_spend.is_some() {
                tx.execute("INSERT INTO transfers(spark_id,request_id,kind,status,owner) VALUES(?1,?2,'CLAIM_STATIC_DEPOSIT','COMPLETED',?3) ON CONFLICT(spark_id) DO NOTHING",(&record.transfer_id,&record.id,&record.quote.owner))?;
            }
            tx.commit()
        }).await
    }
    pub async fn run(self: Arc<Self>) {
        loop {
            {
                let _guard = self.lock.lock().await;
                let records:Result<Vec<DepositClaim>,String>=self.db.with(|c|{let mut s=c.prepare("SELECT data FROM deposit_claims WHERE COALESCE(json_extract(data,'$.phase'),'')!='SPEND_TX_CONFIRMED' ORDER BY rowid LIMIT 100")?;let rows=s.query_map([],row)?;rows.collect()}).await;
                match records {
                    Ok(records) => {
                        for mut record in records {
                            let _liquidity = self.spark.deposit_liquidity_lock().await;
                            if let Err(error) = self.advance(&mut record).await {
                                tracing::warn!(
                                    request_id = record.id,
                                    "static deposit recovery pending: {error}"
                                );
                                let _ = self
                                    .db
                                    .with(|c| {
                                        c.execute(
                                            "UPDATE deposit_claims SET last_error=?2 WHERE id=?1",
                                            (&record.id, &error),
                                        )
                                    })
                                    .await;
                            }
                        }
                    }
                    Err(error) => tracing::warn!("static deposit worker: {error}"),
                }
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_matches_operator_template_and_default_client_fee_cap() {
        let destination = ScriptBuf::from_bytes([vec![0, 20], vec![1; 20]].concat());
        let point = OutPoint::new("01".repeat(32).parse().unwrap(), 2);
        let mut tx = recovery_transaction(point, 9901, destination.clone());
        assert_eq!(tx.version, Version(3));
        assert_eq!(tx.input[0].sequence, Sequence::MAX);
        assert_eq!(tx.input[0].previous_output, point);
        assert_eq!(
            tx.output,
            vec![TxOut {
                value: Amount::from_sat(9901),
                script_pubkey: destination
            }]
        );
        let id = tx.compute_txid();
        tx.input[0].witness.push([0; 64]);
        assert_eq!(tx.compute_txid(), id);
        assert!(tx.vsize() <= 99, "quoted fee must fit Breez's 99-vbyte cap");
        assert_eq!(deserialize::<Transaction>(&serialize(&tx)).unwrap(), tx);
    }
    #[test]
    fn deposit_outpoints_reject_negative_and_overflowing_indices() {
        let txid = "01".repeat(32);
        assert_eq!(
            outpoint(&json!({"transaction_id":txid,"output_index":2})).unwrap(),
            (txid.clone(), 2)
        );
        for index in [json!(-1), json!(4294967296u64), json!("2")] {
            assert!(outpoint(&json!({"transaction_id":txid,"output_index":index})).is_err());
        }
        assert!(outpoint(&json!({"transaction_id":"garbage","output_index":0})).is_err());
    }
}
