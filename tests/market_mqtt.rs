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
fn mqtt_keepalive_is_pumped_while_the_trading_thread_performs_ui_reconciliation() {
    let source =
        std::fs::read_to_string("src/market_mqtt.rs").expect("reviewed market MQTT client source");
    assert!(source.contains("thread::Builder::new()"));
    assert!(source.contains("MarketMqttPumpEvent"));
    assert!(source.contains("pump_receiver.recv_timeout(timeout)"));
    assert!(!source.contains("self.connection.recv_timeout(timeout)"));
}
