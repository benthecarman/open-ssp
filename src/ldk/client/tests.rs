use super::*;
use crate::lightning_store::SendStatus;
use ldk_node::lightning::offers::offer::OfferId;
use ldk_node::lightning_types::string::UntrustedString;

struct TestDir(std::path::PathBuf);
impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("ssp-embedded-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn config(dir: &TestDir) -> Config {
    let mut config = Config::from_env().unwrap();
    config.ldk_backend = LdkBackendMode::Embedded;
    config.data_dir = dir.0.to_str().unwrap().into();
    config.ldk_node_data_dir = String::new();
    config.ldk_node_esplora_url = "http://127.0.0.1:1".into();
    config.ldk_node_listen_addr = "127.0.0.1:0".into();
    config.ldk_node_seed_required = false;
    config
}

#[test]
fn embedded_identity_survives_restart_and_directory_is_exclusive() {
    let dir = TestDir::new();
    let mut config = config(&dir);
    let node = EmbeddedNode::build(&config, bitcoin::Network::Regtest).unwrap();
    let id = node.node.node_id();
    assert!(node.node.config().manually_handle_unknown_bolt11_payments);
    assert!(EmbeddedNode::build(&config, bitcoin::Network::Regtest)
        .err()
        .unwrap()
        .contains("already in use"));
    let seed_path = dir.0.join("ldk-node/seed");
    let seed = std::fs::read(&seed_path).unwrap();
    assert_eq!(seed.len(), 64);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&seed_path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    drop(node);
    config.ldk_node_seed_required = true;
    let node = EmbeddedNode::build(&config, bitcoin::Network::Regtest).unwrap();
    assert_eq!(node.node.node_id(), id);
    drop(node);
    assert_eq!(std::fs::read(&seed_path).unwrap(), seed);
    std::fs::remove_file(&seed_path).unwrap();
    config.ldk_node_seed_required = false;
    assert!(EmbeddedNode::build(&config, bitcoin::Network::Regtest)
        .err()
        .unwrap()
        .contains("seed missing"));
    assert!(!seed_path.exists());
}

#[test]
fn embedded_config_and_seed_errors_fail_closed() {
    let dir = TestDir::new();
    let mut config = config(&dir);
    config.ldk_node_esplora_url.clear();
    assert!(EmbeddedNode::build(&config, bitcoin::Network::Regtest)
        .err()
        .unwrap()
        .contains("LDK_NODE_ESPLORA_URL"));
    config.ldk_node_esplora_url = "file:///tmp/chain".into();
    assert!(EmbeddedNode::build(&config, bitcoin::Network::Regtest).is_err());
    config.ldk_node_esplora_url = "http://127.0.0.1:1".into();
    config.ldk_node_listen_addr = "invalid".into();
    assert!(EmbeddedNode::build(&config, bitcoin::Network::Regtest)
        .err()
        .unwrap()
        .contains("LDK_NODE_LISTEN_ADDR"));
    assert!(load_entropy(&dir.0, true).is_err());
    std::fs::write(dir.0.join("seed"), [1u8; 63]).unwrap();
    assert!(load_entropy(&dir.0, false).is_err());
    assert_eq!(std::fs::read(dir.0.join("seed")).unwrap().len(), 63);
}

fn details(kind: payment::PaymentKind) -> PaymentDetails {
    PaymentDetails {
        id: PaymentId([1; 32]),
        kind,
        amount_msat: Some(42_000),
        fee_paid_msat: Some(12),
        direction: payment::PaymentDirection::Outbound,
        status: payment::PaymentStatus::Succeeded,
        latest_update_timestamp: 123,
    }
}

#[test]
fn payment_snapshots_preserve_settlement_and_recovery_fields() {
    let payment = payment_snapshot(details(payment::PaymentKind::Bolt11 {
        hash: PaymentHash([2; 32]),
        preimage: Some(PaymentPreimage([3; 32])),
        secret: None,
        counterparty_skimmed_fee_msat: Some(21),
    }));
    assert_eq!(payment.payment_id, "01".repeat(32));
    assert_eq!(payment.status, types::PaymentStatus::Succeeded as i32);
    assert_eq!(payment.direction, types::PaymentDirection::Outbound as i32);
    assert_eq!(payment.fee_paid_msat, Some(12));
    assert_eq!(payment.latest_update_timestamp, 123);
    assert_eq!(
        super::super::bolt11_claimable_amount(&payment),
        Some(41_979)
    );
    assert_eq!(
        super::super::bolt11_hash(Some(payment)),
        Some("02".repeat(32))
    );

    let send = LightningSend {
        request_id: "request".into(),
        owner: "owner".into(),
        outbound_transfer_id: "funding".into(),
        invoice: "offer".into(),
        amount_sats: 42,
        amount_override: Some(42),
        kind: SendKind::Bolt12,
        expected_id: "04".repeat(32),
        payment_id: None,
        status: SendStatus::Submitting,
    };
    let mut payment = payment_snapshot(details(payment::PaymentKind::Bolt12Offer {
        hash: Some(PaymentHash([2; 32])),
        preimage: Some(PaymentPreimage([3; 32])),
        secret: None,
        offer_id: OfferId([4; 32]),
        payer_note: Some(UntrustedString(send.payer_note())),
        quantity: Some(1),
    }));
    super::super::validate_send_payment(&send, &payment).unwrap();
    assert_eq!(
        super::super::bolt12_preimage(Some(&payment)),
        Some("03".repeat(32))
    );
    assert_eq!(
        super::super::bolt12_offer_ids(Some(payment.clone())),
        Some(("04".repeat(32), "02".repeat(32)))
    );
    payment.direction = types::PaymentDirection::Inbound as i32;
    assert!(super::super::validate_send_payment(&send, &payment).is_err());
}

#[test]
fn claimable_event_keeps_payment_id_distinct_from_hash() {
    let event = map_event(
        ldk_node::Event::PaymentClaimable {
            payment_id: PaymentId([1; 32]),
            payment_hash: PaymentHash([2; 32]),
            claimable_amount_msat: 42_000,
            claim_deadline: Some(120),
            custom_records: vec![],
        },
        |_| panic!("claimable event does not need a snapshot"),
    )
    .unwrap()
    .unwrap();
    assert!(
        matches!(event, LnEvent::InboundClaimable(ClaimableReceive { payment_id, payment_hash, amount_msat: Some(42_000) })
        if payment_id == "01".repeat(32) && payment_hash == "02".repeat(32))
    );
}

#[test]
fn received_offer_event_preserves_proof_and_amount() {
    let event = map_event(
        ldk_node::Event::PaymentReceived {
            payment_id: PaymentId([1; 32]),
            payment_hash: PaymentHash([2; 32]),
            amount_msat: 42_000,
            custom_records: vec![],
        },
        |id| {
            assert_eq!(id, PaymentId([1; 32]));
            Ok(payment_snapshot(details(
                payment::PaymentKind::Bolt12Offer {
                    hash: Some(PaymentHash([2; 32])),
                    preimage: Some(PaymentPreimage([3; 32])),
                    secret: None,
                    offer_id: OfferId([4; 32]),
                    payer_note: None,
                    quantity: None,
                },
            )))
        },
    )
    .unwrap()
    .unwrap();
    assert!(
        matches!(event, LnEvent::InboundBolt12Received { offer_id, payment_hash, preimage: Some(preimage), amount_msat: Some(42_000) }
        if offer_id == "04".repeat(32) && payment_hash == "02".repeat(32) && preimage == "03".repeat(32))
    );
    assert!(hex32("00").is_err());
}

mod regtest;
