"""MQTT producer for worker telemetry events."""

import json
import time
import ssl
import paho.mqtt.client as mqtt

from src.utils import settings, log
from src.producers.simulator import WorkforceSimulator


class MQTTTelemetryProducer:
    """Produces synthetic worker telemetry to MQTT broker (EMQX)."""

    def __init__(self):
        """Initialize MQTT client."""
        self.client = mqtt.Client(client_id="ergostream-mqtt-producer")

        # EMQX credentials (from your MQTT connector config)
        self.broker = "x9b181ae.ala.eu-central-1.emqxsl.com"
        self.port = 8883
        self.username = "threia-emqx"
        self.password = "Eagle1994@Ankpa"

        # Configure TLS/SSL
        self.client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)

        # Set credentials
        self.client.username_pw_set(self.username, self.password)

        # Set callbacks
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        self.client.on_disconnect = self.on_disconnect

        # Initialize simulator
        self.simulator = WorkforceSimulator(num_workers=settings.num_workers)
        self.events_sent = 0

        log.info(f"MQTT Producer initialized")
        log.info(f"Broker: {self.broker}:{self.port}")
        log.info(f"Simulating {settings.num_workers} workers")

    def on_connect(self, client, userdata, flags, rc):
        """Callback when connected to MQTT broker."""
        if rc == 0:
            log.info("✅ Connected to EMQX MQTT broker successfully!")
        else:
            log.error(f"❌ Failed to connect to MQTT broker. Return code: {rc}")
            if rc == 1:
                log.error("Connection refused - incorrect protocol version")
            elif rc == 2:
                log.error("Connection refused - invalid client identifier")
            elif rc == 3:
                log.error("Connection refused - server unavailable")
            elif rc == 4:
                log.error("Connection refused - bad username or password")
            elif rc == 5:
                log.error("Connection refused - not authorized")

    def on_publish(self, client, userdata, mid):
        """Callback when message is published."""
        self.events_sent += 1
        if self.events_sent % 10 == 0:
            log.info(f"Published {self.events_sent} events to MQTT")

    def on_disconnect(self, client, userdata, rc):
        """Callback when disconnected."""
        if rc != 0:
            log.warning(f"Unexpected disconnection from MQTT broker. Code: {rc}")
        else:
            log.info("Disconnected from MQTT broker")

    def connect(self):
        """Connect to MQTT broker."""
        try:
            log.info(f"Connecting to {self.broker}:{self.port}...")
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_start()  # Start background thread for network loop
            time.sleep(2)  # Give it time to connect
            return True
        except Exception as e:
            log.error(f"Failed to connect to MQTT broker: {e}")
            return False

    def publish_event(self, event):
        """Publish a single event to MQTT."""
        try:
            # Topic pattern: workers/{worker_id}/telemetry
            # This matches the connector pattern: workers/+/telemetry
            topic = f"workers/{event.worker_id}/telemetry"

            # Convert event to JSON
            payload = json.dumps(event.model_dump(mode='json'), default=str)

            # Publish to MQTT
            result = self.client.publish(
                topic=topic,
                payload=payload,
                qos=0,  # Matches connector QoS
                retain=False
            )

            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                log.error(f"Failed to publish to {topic}: {result.rc}")

        except Exception as e:
            log.error(f"Error publishing event: {e}")

    def run(self, interval_seconds: float = None):
        """Run the MQTT producer continuously."""
        if interval_seconds is None:
            interval_seconds = 60.0 / settings.simulation_speed

        log.info(f"Starting MQTT telemetry stream (interval: {interval_seconds:.2f}s)")
        log.info("Press Ctrl+C to stop")

        # Connect to MQTT broker
        if not self.connect():
            log.error("Could not connect to MQTT broker. Exiting.")
            return

        try:
            for batch in self.simulator.generate_stream(interval_seconds):
                for event in batch:
                    self.publish_event(event)

                # Log batch summary
                log.info(f"Batch published: {len(batch)} events | Total: {self.events_sent}")

        except KeyboardInterrupt:
            log.info("Stopping MQTT producer...")
        finally:
            log.info("Disconnecting from MQTT broker...")
            self.client.loop_stop()
            self.client.disconnect()
            log.info(f"Producer stopped. Total events sent: {self.events_sent}")


def main():
    """Entry point for MQTT producer."""
    log.info("=" * 60)
    log.info("ErgoStream - MQTT Worker Telemetry Producer")
    log.info("=" * 60)

    producer = MQTTTelemetryProducer()
    producer.run()


if __name__ == "__main__":
    main()
