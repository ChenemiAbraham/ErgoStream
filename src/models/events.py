"""Event data models for ErgoStream."""

from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class RiskLevel(str, Enum):
    """Risk level classification."""
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class TaskType(str, Enum):
    """Types of worker tasks."""
    PALLET_PICKING = "pallet_picking"
    ASSEMBLY = "assembly"
    PACKAGING = "packaging"
    LIFTING = "lifting"
    REACHING = "reaching"
    OVERHEAD_WORK = "overhead_work"


class WorkerMotionEvent(BaseModel):
    """Raw motion sensor event from worker."""

    worker_id: str = Field(..., description="Unique worker identifier")
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    # Motion metrics
    back_angle: float = Field(..., ge=0, le=180, description="Trunk flexion angle in degrees")
    neck_angle: float = Field(..., ge=0, le=90, description="Neck flexion angle in degrees")
    load_kg: float = Field(..., ge=0, description="Current load weight in kg")
    repetition_rate: float = Field(..., ge=0, description="Repetitions per minute")

    # Context
    station_id: str = Field(..., description="Workstation identifier")
    task_type: TaskType = Field(..., description="Current task type")
    duration_minutes: float = Field(..., ge=0, description="Time on current task")

    # Environmental
    temperature_celsius: float = Field(default=22.0)
    humidity_percent: float = Field(default=50.0)

    class Config:
        json_schema_extra = {
            "example": {
                "worker_id": "W1042",
                "timestamp": "2026-09-22T14:30:00Z",
                "back_angle": 45.0,
                "neck_angle": 20.0,
                "load_kg": 18.5,
                "repetition_rate": 12.0,
                "station_id": "B-17",
                "task_type": "pallet_picking",
                "duration_minutes": 11.5
            }
        }


class WorkerPostureEvent(BaseModel):
    """Aggregated posture analysis event."""

    worker_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    avg_back_angle: float
    max_back_angle: float
    avg_load_kg: float
    max_load_kg: float
    total_repetitions: int

    analysis_window_minutes: float = Field(default=5.0)


class RiskDetectionEvent(BaseModel):
    """High-risk condition detected by Flink."""

    worker_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    risk_level: RiskLevel
    risk_score: float = Field(..., ge=0, le=100)
    confidence: float = Field(..., ge=0, le=1)

    # Risk factors
    primary_risk: str = Field(..., description="Primary risk factor")
    contributing_factors: list[str] = Field(default_factory=list)

    # Context
    station_id: str
    task_type: TaskType
    exposure_minutes: float

    # Metrics at detection time
    current_back_angle: float
    current_load_kg: float
    current_repetition_rate: float

    class Config:
        json_schema_extra = {
            "example": {
                "worker_id": "W1042",
                "risk_level": "HIGH",
                "risk_score": 87.5,
                "confidence": 0.91,
                "primary_risk": "Lower back strain",
                "contributing_factors": [
                    "Excessive trunk flexion",
                    "High repetition rate",
                    "Sustained heavy load"
                ],
                "station_id": "B-17",
                "task_type": "pallet_picking",
                "exposure_minutes": 13.7,
                "current_back_angle": 61.0,
                "current_load_kg": 22.4,
                "current_repetition_rate": 18.0
            }
        }


class InterventionEvent(BaseModel):
    """AI-generated intervention recommendation."""

    intervention_id: str
    worker_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    # Related risk event
    risk_event_id: Optional[str] = None
    risk_level: RiskLevel

    # AI-generated recommendations
    immediate_action: str = Field(..., description="Immediate action to take")
    recommendations: list[str] = Field(..., description="Detailed recommendations")
    rationale: str = Field(..., description="Why these interventions are recommended")

    # Expected outcome
    expected_risk_reduction: float = Field(..., ge=0, le=100)
    monitoring_duration_minutes: int = Field(default=20)

    # Metadata
    generated_by: str = Field(default="ErgoOpsAgent")
    agent_version: str = Field(default="rule-based-v1", description="Agent version that generated this intervention")

    class Config:
        json_schema_extra = {
            "example": {
                "intervention_id": "INT-20260922-001",
                "worker_id": "W1042",
                "risk_level": "HIGH",
                "immediate_action": "Rotate Worker W1042 from pallet picking immediately",
                "recommendations": [
                    "Rotate worker to light-duty task for 30 minutes",
                    "Inspect workstation B-17 height adjustment",
                    "Reduce maximum load at this station to 15kg",
                    "Schedule ergonomic assessment for Worker W1042"
                ],
                "rationale": "Worker has been exposed to high trunk flexion with heavy loads for 13+ minutes. Continued exposure significantly increases injury risk. Station B-17 may require height adjustment based on worker anthropometrics.",
                "expected_risk_reduction": 75.0,
                "monitoring_duration_minutes": 30
            }
        }
