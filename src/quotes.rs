//! Zero-fee receive quotes use the shared Spark protobuf and signature rules.
use crate::AppState;
use bitcoin::secp256k1::{ecdsa::Signature, Message, PublicKey, Secp256k1};
use prost::Message as _;
use serde_json::{json, Value};
use spark_token_primitives::{
    hash_transfer_manifest,
    proto::spark::{manifest_amount, ManifestAmount, ManifestEdge, Network, TransferManifest},
    quote_envelope_digest, receive_attestor_target,
};

fn network(name: &str) -> Result<Network, String> {
    match name {
        "REGTEST" | "LOCAL" => Ok(Network::Regtest),
        "MAINNET" => Ok(Network::Mainnet),
        "TESTNET" => Ok(Network::Testnet),
        "SIGNET" => Ok(Network::Signet),
        _ => Err("invalid quote network".into()),
    }
}
fn digest(bytes: Vec<u8>, network: i32, role: u32, target: Vec<u8>) -> Result<[u8; 32], String> {
    let hash = hash_transfer_manifest(bytes).map_err(|e| e.to_string())?;
    quote_envelope_digest(network as u32, hash, 1, role, target)
        .map_err(|e| e.to_string())?
        .try_into()
        .map_err(|_| "invalid quote digest".into())
}

// The signed manifest has no partner fee. Do not claim that a supplied
// token was absent or verified when this SSP has no partner registry.
fn attribution_status(has_partner_jwt: bool) -> &'static str {
    if has_partner_jwt {
        "PARTNER_ATTRIBUTION_UNSUPPORTED"
    } else {
        "NO_PARTNER_JWT"
    }
}

pub async fn issue(
    state: &AppState,
    owner: &str,
    amount: u64,
    input: &Value,
    has_partner_jwt: bool,
) -> Result<Value, String> {
    let receiver = input["receiver_identity_pubkey"].as_str().unwrap_or(owner);
    let receiver: PublicKey = receiver.parse().map_err(|_| "invalid quote receiver")?;
    let sender: PublicKey = state
        .spark
        .identity()
        .parse()
        .map_err(|_| "invalid SSP identity")?;
    let manifest = TransferManifest {
        version: 1,
        transfer_id: uuid::Uuid::now_v7().to_string(),
        network: network(&state.config.network)? as i32,
        transfer_expiry_time: None,
        edges: vec![ManifestEdge {
            sender_identity_public_key: sender.serialize().to_vec(),
            receiver_identity_public_key: receiver.serialize().to_vec(),
            amount: Some(ManifestAmount {
                amount: Some(manifest_amount::Amount::Sats(amount)),
            }),
        }],
        fees: vec![],
        quote_expiry_time: Some(prost_types::Timestamp {
            seconds: chrono::Utc::now().timestamp() + 300,
            nanos: 0,
        }),
    };
    let bytes = manifest.encode_to_vec();
    let signature = state
        .spark
        .sign_digest(digest(bytes.clone(), manifest.network, 1, vec![])?);
    // Retain the exact issuer bytes: attestors submit their own signature only.
    state
        .db
        .with(|c| {
            let tx = c.unchecked_transaction()?;
            tx.execute(
                "DELETE FROM receive_quotes WHERE expires_at<=?1",
                [chrono::Utc::now().timestamp()],
            )?;
            let count: u64 = tx.query_row(
                "SELECT count(*) FROM receive_quotes WHERE owner=?1",
                [owner],
                |r| r.get(0),
            )?;
            if count >= 100 {
                return Err(rusqlite::Error::ToSqlConversionFailure(
                    "receive quote limit reached".into(),
                ));
            }
            tx.execute(
                "INSERT INTO receive_quotes VALUES(?1,?2,?3,?4)",
                (
                    &manifest.transfer_id,
                    owner,
                    &bytes,
                    manifest.quote_expiry_time.unwrap().seconds,
                ),
            )?;
            tx.commit()
        })
        .await?;
    Ok(
        json!({"issued_quote":{"serialized_manifest":hex::encode(bytes),"issuer_signature":signature},"attribution_status":attribution_status(has_partner_jwt)}),
    )
}

pub async fn validate(
    state: &AppState,
    owner: &str,
    receiver: &str,
    hash: &str,
    amount: u64,
    input: &Value,
) -> Result<Option<String>, String> {
    let Some(quote) = input.get("committed_quote").filter(|v| !v.is_null()) else {
        return Ok(None);
    };
    let encoded = quote["serialized_manifest"]
        .as_str()
        .filter(|v| v.len() <= 16_384)
        .ok_or("invalid receive manifest")?;
    let bytes = hex::decode(encoded).map_err(|_| "invalid manifest hex")?;
    let manifest =
        TransferManifest::decode(bytes.as_slice()).map_err(|_| "invalid protobuf manifest")?;
    let stored:bool=state.db.with(|c|c.query_row("SELECT EXISTS(SELECT 1 FROM receive_quotes WHERE id=?1 AND owner=?2 AND manifest=?3 AND expires_at>?4)",(&manifest.transfer_id,owner,&bytes,chrono::Utc::now().timestamp()),|r|r.get(0))).await?;
    if !stored {
        return Err("receive quote is unknown, expired, or belongs to another wallet".into());
    }
    verify_attestation(
        &manifest,
        &bytes,
        owner,
        receiver,
        hash,
        amount,
        &state.config.network,
        quote,
    )?;
    Ok(Some(manifest.transfer_id))
}

#[allow(clippy::too_many_arguments)]
fn verify_attestation(
    manifest: &TransferManifest,
    bytes: &[u8],
    owner: &str,
    receiver: &str,
    hash: &str,
    amount: u64,
    network_name: &str,
    quote: &Value,
) -> Result<(), String> {
    if manifest.network != network(network_name)? as i32
        || !manifest.fees.is_empty()
        || manifest.edges.len() != 1
    {
        return Err("unsupported receive quote".into());
    }
    let edge = &manifest.edges[0];
    let receiver: PublicKey = receiver.parse().map_err(|_| "invalid receiver")?;
    if edge.receiver_identity_public_key != receiver.serialize()
        || edge.amount.as_ref().and_then(|a| a.amount)
            != Some(manifest_amount::Amount::Sats(amount))
    {
        return Err("receive amount or receiver differs from quote".into());
    }
    let attestor: PublicKey = owner.parse().map_err(|_| "invalid attestor")?;
    let target = receive_attestor_target(hex::decode(hash).map_err(|_| "invalid payment hash")?)
        .map_err(|e| e.to_string())?;
    let digest = digest(bytes.to_vec(), manifest.network, 2, target)?;
    let signature = Signature::from_der(
        &hex::decode(
            quote["attestor_signature"]
                .as_str()
                .ok_or("attestor_signature required")?,
        )
        .map_err(|_| "invalid signature hex")?,
    )
    .map_err(|_| "invalid DER signature")?;
    let mut canonical = signature;
    canonical.normalize_s();
    if canonical != signature {
        return Err("noncanonical quote signature".into());
    }
    Secp256k1::verification_only()
        .verify_ecdsa(&Message::from_digest(digest), &signature, &attestor)
        .map_err(|_| "invalid quote attestation".to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::secp256k1::SecretKey;

    #[test]
    fn partner_tokens_are_reported_without_claiming_attribution() {
        assert_eq!(attribution_status(false), "NO_PARTNER_JWT");
        assert_eq!(attribution_status(true), "PARTNER_ATTRIBUTION_UNSUPPORTED");
    }

    #[test]
    fn quote_signatures_bind_network_role_recipient_amount_and_hash() {
        let secp = Secp256k1::new();
        let secret = SecretKey::from_slice(&[1; 32]).unwrap();
        let owner = PublicKey::from_secret_key(&secp, &secret).to_string();
        let manifest = TransferManifest {
            version: 1,
            transfer_id: uuid::Uuid::now_v7().to_string(),
            network: Network::Regtest as i32,
            edges: vec![ManifestEdge {
                sender_identity_public_key: hex::decode(&owner).unwrap(),
                receiver_identity_public_key: hex::decode(&owner).unwrap(),
                amount: Some(ManifestAmount {
                    amount: Some(manifest_amount::Amount::Sats(1234)),
                }),
            }],
            quote_expiry_time: Some(prost_types::Timestamp {
                seconds: 2000000000,
                nanos: 0,
            }),
            ..Default::default()
        };
        let bytes = manifest.encode_to_vec();
        let hash = "02".repeat(32);
        let target = receive_attestor_target(hex::decode(&hash).unwrap()).unwrap();
        let d = digest(bytes.clone(), manifest.network, 2, target).unwrap();
        let signature = secp.sign_ecdsa(&Message::from_digest(d), &secret);
        let quote = json!({"attestor_signature":hex::encode(signature.serialize_der())});
        assert!(verify_attestation(
            &manifest, &bytes, &owner, &owner, &hash, 1234, "REGTEST", &quote
        )
        .is_ok());
        assert!(verify_attestation(
            &manifest, &bytes, &owner, &owner, &hash, 1235, "REGTEST", &quote
        )
        .is_err());
        assert!(verify_attestation(
            &manifest, &bytes, &owner, &owner, &hash, 1234, "MAINNET", &quote
        )
        .is_err());
        assert!(verify_attestation(
            &manifest,
            &bytes,
            &owner,
            &owner,
            &"03".repeat(32),
            1234,
            "REGTEST",
            &quote
        )
        .is_err());
        let other = PublicKey::from_secret_key(&secp, &SecretKey::from_slice(&[2; 32]).unwrap())
            .to_string();
        assert!(verify_attestation(
            &manifest, &bytes, &owner, &other, &hash, 1234, "REGTEST", &quote
        )
        .is_err());
        let issuer = digest(bytes.clone(), manifest.network, 1, vec![]).unwrap();
        let wrong_role = json!({"attestor_signature":hex::encode(secp.sign_ecdsa(&Message::from_digest(issuer),&secret).serialize_der())});
        assert!(verify_attestation(
            &manifest,
            &bytes,
            &owner,
            &owner,
            &hash,
            1234,
            "REGTEST",
            &wrong_role
        )
        .is_err());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn quote_reuse_rolls_back_the_second_receive_and_survives_restart() {
        let path = std::env::temp_dir().join(format!("quote-test-{}", uuid::Uuid::new_v4()));
        let db = crate::db::Db::open(path.to_str().unwrap()).unwrap();
        let mut payload = json!({"payment_hash":"one","invoice":"invoice","amount_sats":1234,"quote_transfer_id":"quote"});
        db.insert_request(
            "one",
            "LIGHTNING_RECEIVE",
            "owner",
            "2026-01-01T00:00:00Z",
            &payload,
            None,
        )
        .await
        .unwrap();
        drop(db);
        let db = crate::db::Db::open(path.to_str().unwrap()).unwrap();
        payload["payment_hash"] = json!("two");
        assert!(db
            .insert_request(
                "two",
                "LIGHTNING_RECEIVE",
                "owner",
                "2026-01-01T00:00:00Z",
                &payload,
                None
            )
            .await
            .is_err());
        let count: u64 = db
            .with(|c| c.query_row("SELECT count(*) FROM requests", [], |r| r.get(0)))
            .await
            .unwrap();
        assert_eq!(count, 1);
        drop(db);
        std::fs::remove_dir_all(path).unwrap();
    }
}
