"""Configuration management for ErgoStream."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # Confluent Cloud
    confluent_bootstrap_servers: str = Field(..., description="Kafka bootstrap servers")
    confluent_api_key: str = Field(..., description="Confluent API key")
    confluent_api_secret: str = Field(..., description="Confluent API secret")
    confluent_schema_registry_url: str = Field(..., description="Schema Registry URL")
    confluent_schema_registry_api_key: str = Field(..., description="Schema Registry API key")
    confluent_schema_registry_api_secret: str = Field(..., description="Schema Registry API secret")

    # Application
    environment: str = Field("development", description="Environment (development/production)")
    log_level: str = Field("INFO", description="Logging level")
    enable_metrics: bool = Field(True, description="Enable Prometheus metrics")

    # Simulation
    num_workers: int = Field(10, description="Number of simulated workers")
    simulation_speed: float = Field(1.0, description="Simulation speed multiplier")

    # Topics
    topic_worker_motion: str = "ergo.worker.motion"
    topic_worker_posture: str = "ergo.worker.posture"
    topic_worker_task: str = "ergo.worker.task"
    topic_risk_detected: str = "ergo.risk.detected"
    topic_interventions: str = "ergo.interventions"

    @property
    def kafka_config(self) -> dict:
        """Generate Kafka client configuration."""
        return {
            'bootstrap.servers': self.confluent_bootstrap_servers,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.confluent_api_key,
            'sasl.password': self.confluent_api_secret,
        }

    @property
    def schema_registry_config(self) -> dict:
        """Generate Schema Registry configuration."""
        return {
            'url': self.confluent_schema_registry_url,
            'basic.auth.user.info': f'{self.confluent_schema_registry_api_key}:{self.confluent_schema_registry_api_secret}'
        }


# Global settings instance
settings = Settings()
