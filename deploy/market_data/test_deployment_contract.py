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


if __name__ == "__main__":
    unittest.main()
