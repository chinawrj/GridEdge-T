from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[2]


class DeploymentContractTest(unittest.TestCase):
    def test_mqtt_replacement_forces_a_fresh_ingestor_subscription(self) -> None:
        installer = (ROOT / "deploy/market_data/install_synology.sh").read_text()
        mqtt = 'compose up -d --force-recreate mqtt'
        ingestor = 'compose up -d --build --force-recreate ingestor'
        self.assertIn(mqtt, installer)
        self.assertIn(ingestor, installer)
        self.assertLess(installer.index(mqtt), installer.index(ingestor))

    def test_committed_consumer_is_primed_before_the_first_ingestor_publish(self) -> None:
        installer = (ROOT / "deploy/market_data/install_synology.sh").read_text()
        prime = "-i gridedge-paper-committed-002256 -c -x 31536000 -q 1"
        ingestor = 'compose up -d --build --force-recreate ingestor'
        self.assertIn(prime, installer)
        self.assertIn("gridedge/market-committed/v1/XSHE/002256/trade", installer)
        self.assertIn("gridedge/market-committed/v1/XSHE/002256/status", installer)
        self.assertIn("Subscribed (mid: 1): 1, 1", installer)
        self.assertLess(installer.index(prime), installer.index(ingestor))
        marker_guard = 'if [ -e "$prime_marker" ]; then'
        marker_write = 'mv -f "$prime_marker_tmp" "$prime_marker"'
        self.assertIn('prime_marker="$MARKET_ROOT/data/committed-consumer-002256.genesis-v1"', installer)
        self.assertIn("committed consumer genesis marker is malformed; refusing to re-prime", installer)
        self.assertIn(marker_guard, installer)
        self.assertIn(marker_write, installer)
        self.assertLess(installer.index(marker_guard), installer.index(prime))
        suback = "Subscribed (mid: 1): 1, 1"
        flush = 'kill --signal USR1 gridedge-market-mqtt'
        self.assertLess(installer.index(prime), installer.index(suback))
        self.assertLess(installer.index(suback), installer.index(flush))
        self.assertLess(installer.index(flush), installer.index(marker_write))
        self.assertLess(installer.index(marker_write), installer.index(ingestor))

    def test_broker_never_shortens_the_client_session_and_has_a_bounded_long_holiday_queue(self) -> None:
        config = (ROOT / "deploy/market_data/mosquitto/mosquitto.conf").read_text()
        self.assertNotIn("persistent_client_expiration", config)
        self.assertIn("max_queued_messages 200000", config)

    def test_browser_websocket_listener_is_authenticated_and_persisted(self) -> None:
        config = (ROOT / "deploy/market_data/mosquitto/mosquitto.conf").read_text()
        compose = (ROOT / "deploy/market_data/compose.yaml").read_text()
        listener = config[config.index("listener 9001 0.0.0.0") :]
        self.assertIn("protocol websockets", listener)
        self.assertIn("listener_allow_anonymous false", listener)
        self.assertIn("password_file /mosquitto/config/mosquitto.passwd", listener)
        self.assertIn("acl_file /mosquitto/config/acl", listener)
        self.assertIn('"9001:9001"', compose)

    def test_publisher_credential_can_feed_only_the_read_only_market_consumer(self) -> None:
        acl = (ROOT / "deploy/market_data/mosquitto/acl").read_text()
        publisher = acl.split("user gridedge-publisher", 1)[1].split(
            "user gridedge-ingestor", 1
        )[0]
        self.assertIn("topic write gridedge/market/v1/#", publisher)
        self.assertIn("topic read gridedge/market/v1/#", publisher)
        self.assertIn("topic read gridedge/market-ack/v1/#", publisher)
        self.assertIn("topic read gridedge/market-committed/v1/#", publisher)
        self.assertNotIn("topic write gridedge/market-ack/v1/#", publisher)
        topic_lines = "\n".join(
            line for line in publisher.splitlines() if line.startswith("topic ")
        )
        self.assertNotIn("order", topic_lines.lower())
        self.assertNotIn("ledger", topic_lines.lower())

    def test_only_ingestor_can_publish_database_commit_acknowledgements(self) -> None:
        acl = (ROOT / "deploy/market_data/mosquitto/acl").read_text()
        ingestor = acl.split("user gridedge-ingestor", 1)[1]
        self.assertIn("topic read gridedge/market/v1/#", ingestor)
        self.assertIn("topic write gridedge/market-ack/v1/#", ingestor)
        self.assertIn("topic write gridedge/market-committed/v1/#", ingestor)


if __name__ == "__main__":
    unittest.main()
