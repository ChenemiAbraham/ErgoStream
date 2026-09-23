"""Quick test script to verify MQTT connection to EMQX."""

import json
import time
import ssl
import paho.mqtt.client as mqtt


def on_connect(client, userdata, flags, rc):
    """Callback when connected."""
    if rc == 0:
        print("✅ Successfully connected to EMQX!")
    else:
        print(f"❌ Connection failed with code: {rc}")


def on_publish(client, userdata, mid):
    """Callback when message published."""
    print(f"✅ Message {mid} published successfully")


def test_mqtt_connection():
    """Test MQTT connection and publish a test message."""
    print("=" * 60)
    print("Testing MQTT Connection to EMQX")
    print("=" * 60)

    # Create client
    client = mqtt.Client(client_id="ergostream-test")

    # Configure TLS
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)

    # Set credentials
    client.username_pw_set("threia-emqx", "Eagle1994@Ankpa")

    # Set callbacks
    client.on_connect = on_connect
    client.on_publish = on_publish

    try:
        # Connect
        print("\n1. Connecting to x9b181ae.ala.eu-central-1.emqxsl.com:8883...")
        client.connect("x9b181ae.ala.eu-central-1.emqxsl.com", 8883, keepalive=60)

        # Start network loop
        client.loop_start()
        time.sleep(3)  # Wait for connection

        # Publish test message
        print("\n2. Publishing test message...")
        test_data = {
            "worker_id": "W9999",
            "timestamp": "2026-09-23T10:00:00Z",
            "back_angle": 25.0,
            "load_kg": 10.0,
            "test": True
        }

        result = client.publish(
            topic="workers/W9999/telemetry",
            payload=json.dumps(test_data),
            qos=0
        )

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print("✅ Message queued for publishing")
        else:
            print(f"❌ Failed to publish: {result.rc}")

        # Wait for publish to complete
        time.sleep(2)

        print("\n3. Cleaning up...")
        client.loop_stop()
        client.disconnect()

        print("\n" + "=" * 60)
        print("✅ MQTT Test Complete!")
        print("=" * 60)
        print("\nCheck Confluent Cloud:")
        print("  Topics → ergo.worker.motion → Messages")
        print("  You should see the test message within 30 seconds")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    test_mqtt_connection()
