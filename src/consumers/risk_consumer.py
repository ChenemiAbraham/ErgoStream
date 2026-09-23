"""Kafka consumer for risk detection events."""

import json
from confluent_kafka import Consumer, KafkaException
from typing import Callable

from src.utils import settings, log
from src.models import RiskDetectionEvent


class RiskEventConsumer:
    """Consumes risk detection events from Flink output."""

    def __init__(self, callback: Callable[[RiskDetectionEvent], None]):
        """Initialize consumer with callback function."""
        consumer_config = settings.kafka_config.copy()
        consumer_config.update({
            'group.id': 'ergostream-risk-consumer',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
        })

        self.consumer = Consumer(consumer_config)
        self.topic = settings.topic_risk_detected
        self.callback = callback
        self.events_processed = 0

        self.consumer.subscribe([self.topic])
        log.info(f"Consumer subscribed to: {self.topic}")

    def run(self):
        """Run the consumer loop."""
        log.info("Starting risk event consumer...")
        log.info("Waiting for risk events from Flink...")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    log.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Parse the risk event
                    value = json.loads(msg.value().decode('utf-8'))
                    risk_event = RiskDetectionEvent(**value)

                    self.events_processed += 1
                    log.info(f"Risk detected: {risk_event.worker_id} - {risk_event.risk_level} (score: {risk_event.risk_score:.1f})")

                    # Invoke callback
                    self.callback(risk_event)

                except json.JSONDecodeError as e:
                    log.error(f"Failed to decode message: {e}")
                except Exception as e:
                    log.error(f"Error processing event: {e}")

        except KeyboardInterrupt:
            log.info("Stopping consumer...")
        finally:
            self.consumer.close()
            log.info(f"Consumer stopped. Events processed: {self.events_processed}")


def main():
    """Entry point for consumer."""
    def handle_risk_event(event: RiskDetectionEvent):
        """Simple handler that prints risk events."""
        log.warning(f"🚨 HIGH RISK DETECTED 🚨")
        log.warning(f"Worker: {event.worker_id} @ Station {event.station_id}")
        log.warning(f"Risk: {event.primary_risk}")
        log.warning(f"Score: {event.risk_score:.1f} | Confidence: {event.confidence:.2%}")

    consumer = RiskEventConsumer(callback=handle_risk_event)
    consumer.run()


if __name__ == "__main__":
    main()
