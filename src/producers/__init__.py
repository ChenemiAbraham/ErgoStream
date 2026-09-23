"""Producers for ErgoStream."""

from .simulator import WorkerSimulator, WorkforceSimulator

# Only import what's available
# worker_telemetry requires confluent-kafka (optional)
# mqtt_telemetry requires paho-mqtt (installed)

__all__ = ["WorkerSimulator", "WorkforceSimulator"]
