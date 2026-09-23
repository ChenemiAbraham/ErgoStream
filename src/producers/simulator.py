"""Synthetic worker telemetry data generator."""

import random
from datetime import datetime
from typing import Iterator
from faker import Faker

from src.models import WorkerMotionEvent, TaskType, RiskLevel


fake = Faker()


class WorkerSimulator:
    """Simulates realistic worker motion patterns."""

    def __init__(self, worker_id: str, station_id: str):
        self.worker_id = worker_id
        self.station_id = station_id
        self.current_task = random.choice(list(TaskType))
        self.task_duration = 0.0
        self.risk_state = RiskLevel.LOW
        self.fatigue_factor = 0.0  # 0.0 to 1.0

    def _generate_normal_motion(self) -> dict:
        """Generate normal, safe motion parameters."""
        return {
            "back_angle": random.uniform(5, 25),
            "neck_angle": random.uniform(5, 20),
            "load_kg": random.uniform(2, 12),
            "repetition_rate": random.uniform(3, 8),
        }

    def _generate_moderate_risk_motion(self) -> dict:
        """Generate moderate risk motion parameters."""
        return {
            "back_angle": random.uniform(25, 45),
            "neck_angle": random.uniform(20, 35),
            "load_kg": random.uniform(12, 18),
            "repetition_rate": random.uniform(8, 14),
        }

    def _generate_high_risk_motion(self) -> dict:
        """Generate high risk motion parameters."""
        return {
            "back_angle": random.uniform(45, 75),
            "neck_angle": random.uniform(35, 60),
            "load_kg": random.uniform(18, 30),
            "repetition_rate": random.uniform(14, 22),
        }

    def _increase_fatigue(self):
        """Simulate worker fatigue over time."""
        self.fatigue_factor = min(1.0, self.fatigue_factor + 0.01)

    def _maybe_change_task(self) -> bool:
        """Randomly change task (task rotation)."""
        if self.task_duration > random.uniform(15, 45):
            self.current_task = random.choice(list(TaskType))
            self.task_duration = 0.0
            self.fatigue_factor *= 0.5  # Rest reduces fatigue
            return True
        return False

    def _determine_risk_state(self) -> RiskLevel:
        """Determine current risk state based on various factors."""
        risk_roll = random.random()

        # More fatigued = higher risk probability
        if self.fatigue_factor > 0.7 and risk_roll < 0.15:
            return RiskLevel.HIGH
        elif self.fatigue_factor > 0.5 and risk_roll < 0.25:
            return RiskLevel.MODERATE
        elif risk_roll < 0.05:
            return RiskLevel.HIGH
        elif risk_roll < 0.15:
            return RiskLevel.MODERATE
        else:
            return RiskLevel.LOW

    def generate_event(self) -> WorkerMotionEvent:
        """Generate a single motion event."""
        self.task_duration += random.uniform(0.8, 1.2)  # ~1 minute per event
        self._increase_fatigue()
        self._maybe_change_task()

        # Determine risk state
        self.risk_state = self._determine_risk_state()

        # Generate motion based on risk state
        if self.risk_state == RiskLevel.HIGH:
            motion = self._generate_high_risk_motion()
        elif self.risk_state == RiskLevel.MODERATE:
            motion = self._generate_moderate_risk_motion()
        else:
            motion = self._generate_normal_motion()

        # Add some noise/variation
        motion["back_angle"] += random.gauss(0, 3)
        motion["load_kg"] += random.gauss(0, 1.5)
        motion["repetition_rate"] += random.gauss(0, 1)

        # Clamp to realistic ranges
        motion["back_angle"] = max(0, min(180, motion["back_angle"]))
        motion["neck_angle"] = max(0, min(90, motion["neck_angle"]))
        motion["load_kg"] = max(0, min(50, motion["load_kg"]))
        motion["repetition_rate"] = max(0, min(30, motion["repetition_rate"]))

        return WorkerMotionEvent(
            worker_id=self.worker_id,
            timestamp=datetime.utcnow(),
            back_angle=motion["back_angle"],
            neck_angle=motion["neck_angle"],
            load_kg=motion["load_kg"],
            repetition_rate=motion["repetition_rate"],
            station_id=self.station_id,
            task_type=self.current_task,
            duration_minutes=self.task_duration,
            temperature_celsius=random.uniform(18, 26),
            humidity_percent=random.uniform(40, 65),
        )

    def generate_stream(self, interval_seconds: float = 60.0) -> Iterator[WorkerMotionEvent]:
        """Generate continuous stream of motion events."""
        import time

        while True:
            event = self.generate_event()
            yield event
            time.sleep(interval_seconds)


class WorkforceSimulator:
    """Manages multiple worker simulators."""

    def __init__(self, num_workers: int = 10):
        self.workers = []
        stations = [f"A-{i:02d}" for i in range(1, 11)] + [f"B-{i:02d}" for i in range(1, 11)]

        for i in range(num_workers):
            worker_id = f"W{1000 + i}"
            station_id = random.choice(stations)
            self.workers.append(WorkerSimulator(worker_id, station_id))

    def generate_batch(self) -> list[WorkerMotionEvent]:
        """Generate one event from each worker."""
        return [worker.generate_event() for worker in self.workers]

    def generate_stream(self, interval_seconds: float = 60.0) -> Iterator[list[WorkerMotionEvent]]:
        """Generate continuous batch stream."""
        import time

        while True:
            batch = self.generate_batch()
            yield batch
            time.sleep(interval_seconds)
