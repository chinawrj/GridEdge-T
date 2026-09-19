use gridedge_t::market_mqtt::{reviewed_market_mqtt_contract, MarketMqttMessage};
use rumqttc::v5::mqttbytes::QoS;

#[test]
fn mqtt_market_wire_contract_requires_qos1_non_retained_canonical_json() {
    let topic = "gridedge/market/v1/XSHE/002256/trade";
    let payload = br#"{"spec":"gridedge.market"}"#;
    assert!(MarketMqttMessage::from_wire_contract(
        topic,
        payload,
        QoS::AtLeastOnce,
        false,
        Some("application/json"),
    )
    .is_ok());
    assert!(MarketMqttMessage::from_wire_contract(
        topic,
        payload,
        QoS::AtMostOnce,
        false,
        Some("application/json"),
    )
    .is_err());
    assert!(MarketMqttMessage::from_wire_contract(
        topic,
        payload,
        QoS::AtLeastOnce,
        true,
        Some("application/json"),
    )
    .is_err());
    assert!(
        MarketMqttMessage::from_wire_contract(topic, payload, QoS::AtLeastOnce, false, None,)
            .is_err()
    );
}

#[test]
fn worker_consumes_only_postgresql_committed_market_topics() {
    let event_id = "f".repeat(64);
    let payload = market_message(&event_id, 2_770).payload;
    let committed = MarketMqttMessage::from_committed_wire_contract(
        "gridedge/market-committed/v1/XSHE/002256/trade",
        &payload,
        QoS::AtLeastOnce,
        false,
        Some("application/json"),
    )
    .unwrap();
    assert_eq!(committed.topic, "gridedge/market/v1/XSHE/002256/trade");
    assert_eq!(committed.payload, payload);
    assert!(MarketMqttMessage::from_committed_wire_contract(
        "gridedge/market/v1/XSHE/002256/trade",
        &committed.payload,
        QoS::AtLeastOnce,
        false,
        Some("application/json"),
    )
    .is_err());

    let source = std::fs::read_to_string("src/market_mqtt.rs").unwrap();
    assert!(source.contains("format!(\"{}{venue}/{symbol}/trade\", contract.committed_prefix)"));
    assert!(source.contains("format!(\"{}{venue}/{symbol}/status\", contract.committed_prefix)"));
    assert!(!source.contains("client.subscribe(\n            format!(\"gridedge/market/v1/"));
    assert!(!source.contains("client.subscribe(\"gridedge/market-ack/v1/#\""));
    assert!(source.contains("set_session_expiry_interval(Some(31_536_000))"));
    assert!(!source.contains("set_session_expiry_interval(Some(172_800))"));
}

#[test]
fn isolated_worker_contract_binds_loopback_client_and_nonce_namespace_atomically() {
    let nonce = "e2e-0629-b4a83e91-73f7-4c7f-9bb9-7c92d3e1a640";
    let namespace = format!("gridedge-e2e/{nonce}");
    let contract = reviewed_market_mqtt_contract(
        "127.0.0.1",
        18883,
        "gridedge-e2e-worker",
        &format!("{nonce}-worker"),
        &namespace,
    )
    .unwrap();
    assert_eq!(
        contract.committed_prefix,
        format!("{namespace}/market-committed/v1/")
    );
    assert!(reviewed_market_mqtt_contract(
        "192.168.1.201",
        8883,
        "gridedge-e2e-worker",
        &format!("{nonce}-worker"),
        &namespace,
    )
    .is_err());
    assert!(reviewed_market_mqtt_contract(
        "127.0.0.1",
        18883,
        "gridedge-publisher",
        "gridedge-paper-committed-002256",
        "gridedge",
    )
    .is_err());
    for (host, port) in [("192.168.1.202", 8883), ("192.168.1.201", 18883)] {
        assert!(reviewed_market_mqtt_contract(
            host,
            port,
            "gridedge-publisher",
            "gridedge-paper-committed-002256",
            "gridedge",
        )
        .is_err());
    }
    assert!(reviewed_market_mqtt_contract(
        "192.168.1.201",
        8883,
        "gridedge-publisher",
        "gridedge-paper-committed-002256",
        "gridedge",
    )
    .is_ok());
}

#[test]
fn isolated_committed_topic_normalizes_only_after_exact_namespace_validation() {
    let namespace = "gridedge-e2e/e2e-0629-b4a83e91-73f7-4c7f-9bb9-7c92d3e1a640";
    let payload = br#"{"spec":"gridedge.market"}"#;
    let accepted = MarketMqttMessage::from_namespaced_committed_wire_contract(
        &format!("{namespace}/market-committed/v1/XSHE/002256/trade"),
        payload,
        QoS::AtLeastOnce,
        false,
        Some("application/json"),
        namespace,
    )
    .unwrap();
    assert_eq!(accepted.topic, "gridedge/market/v1/XSHE/002256/trade");
    assert!(MarketMqttMessage::from_namespaced_committed_wire_contract(
        "gridedge/market-committed/v1/XSHE/002256/trade",
        payload,
        QoS::AtLeastOnce,
        false,
        Some("application/json"),
        namespace,
    )
    .is_err());
}

#[test]
fn mqtt_keepalive_is_pumped_while_the_trading_thread_performs_ui_reconciliation() {
    let source =
        std::fs::read_to_string("src/market_mqtt.rs").expect("reviewed market MQTT client source");
    assert!(source.contains("thread::Builder::new()"));
    assert!(source.contains("MarketMqttPumpEvent"));
    assert!(source.contains("pump_receiver.recv_timeout(timeout)"));
    assert!(!source.contains("self.connection.recv_timeout(timeout)"));
}

fn market_message(event_id: &str, sequence: u64) -> MarketMqttMessage {
    MarketMqttMessage {
        topic: "gridedge/market/v1/XSHE/002256/trade".to_owned(),
        payload: format!(
            r#"{{"event_id":"{event_id}","source":{{"source_id":"eastmoney-web","source_instance_id":"8101d65c-bdba-4de3-83e0-8983506f159e"}},"source_sequence":{sequence}}}"#
        )
        .into_bytes(),
    }
}
