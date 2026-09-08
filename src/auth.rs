use axum::http::HeaderMap;
use base64::{
    engine::general_purpose::{STANDARD as B64, URL_SAFE_NO_PAD},
    Engine,
};
use prost::Message as ProstMessage;
use rand::RngCore;
use secp256k1::{ecdsa::Signature, Message, Secp256k1};
use sha2::{Digest, Sha256};

use crate::AppState;

// Wire schema: vendor/breez-sdk/crates/spark/protos/ssp/ssp_authn.proto.
#[derive(Clone, PartialEq, prost::Message)]
struct Challenge {
    #[prost(int32, tag = "1")]
    version: i32,
    #[prost(int64, tag = "10")]
    timestamp: i64,
    #[prost(bytes = "vec", tag = "20")]
    nonce: Vec<u8>,
    #[prost(bytes = "vec", tag = "30")]
    public_key: Vec<u8>,
}
#[derive(Clone, PartialEq, prost::Message)]
struct ProtectedChallenge {
    #[prost(int32, tag = "1")]
    version: i32,
    #[prost(message, optional, tag = "10")]
    challenge: Option<Challenge>,
    #[prost(bytes = "vec", tag = "20")]
    server_hmac: Vec<u8>,
}

fn validate_challenge(bytes: &[u8], owner: &secp256k1::PublicKey) -> Result<(), String> {
    let protected = ProtectedChallenge::decode(bytes).map_err(|_| "malformed challenge")?;
    let challenge = protected.challenge.ok_or("missing challenge")?;
    if protected.version != 1
        || challenge.version != 1
        || challenge.nonce.len() != 32
        || protected.server_hmac.len() != 32
        || secp256k1::PublicKey::from_slice(&challenge.public_key)
            .ok()
            .as_ref()
            != Some(owner)
    {
        return Err("foreign or malformed challenge".into());
    }
    Ok(())
}

/// Create a challenge for a wallet identity pubkey.
/// Mirrors `mutation GetChallenge(public_key)`. Single-use, 5-minute expiry.
pub async fn get_challenge(state: &AppState, identity_pubkey: &str) -> Result<String, String> {
    let public_key =
        hex::decode(identity_pubkey).map_err(|_| "malformed public_key".to_string())?;
    secp256k1::PublicKey::from_slice(&public_key)
        .map_err(|_| "malformed public_key".to_string())?;
    let mut nonce = vec![0; 32];
    rand::thread_rng().fill_bytes(&mut nonce);
    let challenge = Challenge {
        version: 1,
        timestamp: chrono::Utc::now().timestamp(),
        nonce,
        public_key,
    };
    let server_hmac = state
        .spark
        .authentication_challenge_mac(&challenge.encode_to_vec());
    let protected = URL_SAFE_NO_PAD.encode(
        ProtectedChallenge {
            version: 1,
            challenge: Some(challenge),
            server_hmac,
        }
        .encode_to_vec(),
    );
    state
        .db
        .save_challenge(
            identity_pubkey,
            &protected,
            &chrono::Utc::now().to_rfc3339(),
        )
        .await?;
    Ok(protected)
}

/// Verify `signature` over sha256(base64-decoded protected_challenge) with the
/// wallet identity key. Returns a session token on success.
/// Signature: base64 (SDK) or hex (curl), DER or compact.
pub async fn verify_challenge(
    state: &AppState,
    identity_pubkey: &str,
    protected_challenge: &str,
    signature_hex: &str,
) -> Result<(String, chrono::DateTime<chrono::Utc>), String> {
    let secp = Secp256k1::new();
    let pubkey_bytes = hex::decode(identity_pubkey).map_err(|e| e.to_string())?;
    let pubkey = secp256k1::PublicKey::from_slice(&pubkey_bytes).map_err(|e| e.to_string())?;
    // The challenge must be one we issued, unused, and fresh. Consumed
    // atomically: replays and foreign signatures fail closed here.
    let fresh = state
        .db
        .consume_challenge(
            identity_pubkey,
            protected_challenge,
            chrono::Utc::now().timestamp(),
            300,
        )
        .await?;
    if !fresh {
        return Err("unknown, reused, or expired challenge".to_string());
    }
    let sig = decode_signature(signature_hex)?;
    // SDK signs sha256 of the DECODED challenge bytes (client.ts authenticate()).
    let challenge_bytes = URL_SAFE_NO_PAD
        .decode(protected_challenge.trim())
        .or_else(|_| B64.decode(protected_challenge.trim()))
        .map_err(|_| "malformed challenge".to_string())?;
    // The database binds these exact protobuf bytes to the owner and expiry.
    validate_challenge(&challenge_bytes, &pubkey)?;
    let digest = Sha256::digest(&challenge_bytes);
    let msg = Message::from_digest(*digest.as_ref());
    secp.verify_ecdsa(&msg, &sig, &pubkey)
        .map_err(|e| format!("bad challenge signature: {e}"))?;

    let token = uuid::Uuid::new_v4().to_string();
    let valid_until = chrono::Utc::now() + chrono::Duration::hours(24);
    state
        .db
        .save_session(&token, identity_pubkey, &valid_until.to_rfc3339())
        .await?;
    Ok((token, valid_until))
}

fn decode_signature(encoded: &str) -> Result<Signature, String> {
    let encoded = encoded.trim();
    let parse = |bytes: Vec<u8>| {
        Signature::from_der(&bytes)
            .or_else(|_| Signature::from_compact(&bytes))
            .ok()
    };

    URL_SAFE_NO_PAD
        .decode(encoded)
        .ok()
        .and_then(&parse)
        .or_else(|| B64.decode(encoded).ok().and_then(&parse))
        .or_else(|| hex::decode(encoded).ok().and_then(parse))
        .ok_or_else(|| "malformed signature".to_string())
}

/// Extract bearer session. `get_challenge` allows unauthenticated access;
/// everything else requires it.
pub async fn require_session(state: &AppState, headers: &HeaderMap) -> Result<String, String> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = auth.strip_prefix("Bearer ").unwrap_or("").trim();
    if token.is_empty() {
        return Err("UNAUTHORIZED: missing bearer session token".to_string());
    }
    match state
        .db
        .session_owner(token, &chrono::Utc::now().to_rfc3339())
        .await?
    {
        Some(owner) => Ok(owner),
        None => Err("UNAUTHORIZED: bad or expired session".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protected_challenge_rejects_foreign_keys_and_text() {
        let owner = secp256k1::PublicKey::from_secret_key(
            &Secp256k1::new(),
            &secp256k1::SecretKey::from_slice(&[1; 32]).unwrap(),
        );
        let other = secp256k1::PublicKey::from_secret_key(
            &Secp256k1::new(),
            &secp256k1::SecretKey::from_slice(&[2; 32]).unwrap(),
        );
        let mut protected = ProtectedChallenge {
            version: 1,
            challenge: Some(Challenge {
                version: 1,
                timestamp: 1_786_974_052,
                nonce: vec![7; 32],
                public_key: owner.serialize().to_vec(),
            }),
            server_hmac: vec![9; 32],
        };
        assert!(validate_challenge(&protected.encode_to_vec(), &owner).is_ok());
        assert!(validate_challenge(&protected.encode_to_vec(), &other).is_err());
        assert!(validate_challenge(b"spark-ssp-challenge:sign this", &owner).is_err());
        protected.challenge.as_mut().unwrap().nonce.clear();
        assert!(validate_challenge(&protected.encode_to_vec(), &owner).is_err());
    }

    fn test_signature() -> Signature {
        let secp = Secp256k1::new();
        let secret_key = secp256k1::SecretKey::from_slice(&[1; 32]).unwrap();
        let message = Message::from_digest([2; 32]);
        secp.sign_ecdsa(&message, &secret_key)
    }

    #[test]
    fn decodes_sdk_url_safe_signature() {
        let signature = test_signature();
        let encoded = URL_SAFE_NO_PAD.encode(signature.serialize_der());

        assert_eq!(decode_signature(&encoded).unwrap(), signature);
    }

    #[test]
    fn keeps_standard_base64_and_hex_compatibility() {
        let signature = test_signature();
        let der = signature.serialize_der();

        assert_eq!(decode_signature(&B64.encode(der)).unwrap(), signature);
        assert_eq!(decode_signature(&hex::encode(der)).unwrap(), signature);
    }
}
