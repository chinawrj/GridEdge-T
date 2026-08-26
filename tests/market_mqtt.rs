use gridedge_t::market_mqtt::MarketMqttMessage;
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
    assert!(source.contains("gridedge/market-committed/v1/{venue}/{symbol}/trade"));
    assert!(source.contains("gridedge/market-committed/v1/{venue}/{symbol}/status"));
    assert!(!source.contains("client.subscribe(\n            format!(\"gridedge/market/v1/"));
    assert!(!source.contains("client.subscribe(\"gridedge/market-ack/v1/#\""));
    assert!(source.contains("set_session_expiry_interval(Some(31_536_000))"));
    assert!(!source.contains("set_session_expiry_interval(Some(172_800))"));
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
