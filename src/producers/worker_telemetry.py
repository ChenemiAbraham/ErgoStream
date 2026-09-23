"""Kafka producer for worker telemetry events."""

import json
import time
from confluent_kafka import Producer
from confluent_kafka.error import KafkaException

from src.utils import settings, log
from src.producers.simulator import WorkforceSimulator


class WorkerTelemetryProducer:
    """Produces synthetic worker telemetry to Kafka."""

    def __init__(self):
        """Initialize Kafka producer."""
        producer_config = settings.kafka_config.copy()
        producer_config.update({
            'client.id': 'ergostream-telemetry-producer',
            'acks': 'all',
            'compression.type': 'lz4',
        })

        self.producer = Producer(producer_config)
        self.topic = settings.topic_worker_motion
        self.simulator = WorkforceSimulator(num_workers=settings.num_workers)
        self.events_sent = 0

        log.info(f"Producer initialized for topic: {self.topic}")
        log.info(f"Simulating {settings.num_workers} workers")

    def delivery_callback(self, err, msg):
        """Callback for message delivery reports."""
        if err:
            log.error(f"Message delivery failed: {err}")
        else:
            self.events_sent += 1
            if self.events_sent % 10 == 0:
                log.info(f"Delivered message to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()} | Total: {self.events_sent}")

    def send_event(self, event):
        """Send a single event to Kafka."""
        try:
            # Convert Pydantic model to dict, then to JSON
            value = json.dumps(event.model_dump(mode='json'), default=str).encode('utf-8')
            key = event.worker_id.encode('utf-8')

            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                callback=self.delivery_callback
            )

            # Poll to handle delivery callbacks
            self.producer.poll(0)

        except KafkaException as e:
            log.error(f"Failed to produce message: {e}")
        except Exception as e:
            log.error(f"Unexpected error: {e}")

    def run(self, interval_seconds: float = None):
        """Run the producer continuously."""
        if interval_seconds is None:
            interval_seconds = 60.0 / settings.simulation_speed

        log.info(f"Starting telemetry stream (interval: {interval_seconds:.2f}s)")
        log.info("Press Ctrl+C to stop")

        try:
            for batch in self.simulator.generate_stream(interval_seconds):
                for event in batch:
                    self.send_event(event)

                # Flush periodically
                self.producer.flush()

        except KeyboardInterrupt:
            log.info("Stopping producer...")
        finally:
            log.info(f"Flushing {len(self.producer)} remaining messages...")
            self.producer.flush()
            log.info(f"Producer stopped. Total events sent: {self.events_sent}")


def main():
    """Entry point for producer."""
    log.info("=" * 60)
    log.info("ErgoStream - Worker Telemetry Producer")
    log.info("=" * 60)

    producer = WorkerTelemetryProducer()
    producer.run()


if __name__ == "__main__":
    main()
