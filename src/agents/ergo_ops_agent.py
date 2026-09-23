"""Rule-based agent for generating ergonomic intervention recommendations."""

import json
import uuid
from datetime import datetime
from confluent_kafka import Producer

from src.utils import settings, log
from src.models import RiskDetectionEvent, InterventionEvent, RiskLevel, TaskType
from src.consumers import RiskEventConsumer


class ErgoOpsAgent:
    """Rule-based agent that monitors risk events and generates interventions."""

    def __init__(self):
        """Initialize the agent with Kafka producer."""
        # Kafka producer for intervention events
        producer_config = settings.kafka_config.copy()
        producer_config.update({
            'client.id': 'ergostream-agent',
            'acks': 'all',
        })
        self.producer = Producer(producer_config)
        self.interventions_generated = 0

        log.info("ErgoOps Rule-Based Agent initialized")
        log.info("Using deterministic intervention logic")

    def generate_intervention(self, risk_event: RiskDetectionEvent) -> InterventionEvent:
        """Generate intervention recommendation using rule-based logic."""
        log.info(f"Generating intervention for worker {risk_event.worker_id}...")

        intervention = self._build_rule_based_intervention(risk_event)

        log.info(f"Intervention generated: {intervention.immediate_action}")
        return intervention

    def _build_rule_based_intervention(self, risk: RiskDetectionEvent) -> InterventionEvent:
        """Generate rule-based intervention using deterministic logic."""

        # Determine immediate action based on primary risk
        immediate_actions = {
            "Lower back strain": f"Rotate Worker {risk.worker_id} from {risk.task_type.value} immediately",
            "Excessive load handling": f"Reduce load weight for Worker {risk.worker_id} at Station {risk.station_id}",
            "Repetitive strain injury risk": f"Implement task rotation for Worker {risk.worker_id}",
            "Prolonged exposure fatigue": f"Mandate 15-minute rest break for Worker {risk.worker_id}",
        }

        immediate_action = immediate_actions.get(
            risk.primary_risk,
            f"Intervene immediately for Worker {risk.worker_id} at Station {risk.station_id}"
        )

        # Build recommendations based on risk factors
        recommendations = []

        # Back angle risk
        if risk.current_back_angle >= 50:
            recommendations.append(f"Adjust workstation {risk.station_id} height to reduce trunk flexion")
            recommendations.append("Provide mechanical lift assist for heavy items")
        elif risk.current_back_angle >= 35:
            recommendations.append(f"Review workstation {risk.station_id} ergonomics")

        # Load risk
        if risk.current_load_kg >= 20:
            recommendations.append(f"Reduce maximum load at Station {risk.station_id} to 15kg")
            recommendations.append("Implement two-person lift protocol for heavy items")
        elif risk.current_load_kg >= 15:
            recommendations.append("Provide weight distribution equipment")

        # Repetition risk
        if risk.current_repetition_rate >= 15:
            recommendations.append("Rotate worker to different task type every 30 minutes")
            recommendations.append("Implement micro-breaks (30 seconds every 10 minutes)")
        elif risk.current_repetition_rate >= 12:
            recommendations.append("Monitor repetition rate and rotate if it increases")

        # Exposure time risk
        if risk.exposure_minutes >= 30:
            recommendations.append(f"Mandate immediate 15-minute rest break for Worker {risk.worker_id}")
            recommendations.append("Schedule full ergonomic assessment within 24 hours")
        elif risk.exposure_minutes >= 20:
            recommendations.append("Monitor worker closely for next 15 minutes")

        # Task-specific recommendations
        task_recommendations = {
            TaskType.PALLET_PICKING: "Review pallet height optimization at this station",
            TaskType.LIFTING: "Verify proper lifting technique training compliance",
            TaskType.OVERHEAD_WORK: "Provide overhead work platforms or ladders",
            TaskType.ASSEMBLY: "Adjust workbench height and tool placement",
            TaskType.PACKAGING: "Optimize packaging material placement for minimal reaching",
            TaskType.REACHING: "Reorganize workstation to minimize reach distances",
        }

        if risk.task_type in task_recommendations:
            recommendations.append(task_recommendations[risk.task_type])

        # General recommendations
        recommendations.append(f"Monitor Worker {risk.worker_id} for next 20 minutes post-intervention")
        recommendations.append("Document incident in ergonomic assessment log")

        # Build rationale
        risk_factors = [f for f in risk.contributing_factors if f]
        rationale_parts = [
            f"Worker {risk.worker_id} has been exposed to {risk.primary_risk.lower()}",
            f"for {risk.exposure_minutes:.1f} minutes.",
        ]

        if risk_factors:
            rationale_parts.append(f"Contributing factors: {', '.join(risk_factors).lower()}.")

        rationale_parts.append("Immediate intervention required to prevent injury.")

        # Calculate expected risk reduction
        risk_reduction = 75.0  # Base reduction
        if risk.risk_level == RiskLevel.CRITICAL:
            risk_reduction = 85.0
        elif risk.risk_level == RiskLevel.HIGH:
            risk_reduction = 75.0
        else:
            risk_reduction = 60.0

        return InterventionEvent(
            intervention_id=f"INT-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:6]}",
            worker_id=risk.worker_id,
            risk_level=risk.risk_level,
            immediate_action=immediate_action,
            recommendations=recommendations[:6],  # Limit to top 6
            rationale=" ".join(rationale_parts),
            expected_risk_reduction=risk_reduction,
            generated_by="ErgoOpsAgent-RuleBased",
            agent_version="rule-based-v1",
        )

    def publish_intervention(self, intervention: InterventionEvent):
        """Publish intervention to Kafka."""
        try:
            value = json.dumps(intervention.model_dump(mode='json'), default=str).encode('utf-8')
            key = intervention.worker_id.encode('utf-8')

            self.producer.produce(
                topic=settings.topic_interventions,
                key=key,
                value=value,
            )
            self.producer.flush()

            self.interventions_generated += 1
            log.info(f"Intervention published to Kafka | Total: {self.interventions_generated}")

        except Exception as e:
            log.error(f"Failed to publish intervention: {e}")

    def handle_risk_event(self, risk_event: RiskDetectionEvent):
        """Handle incoming risk event - main callback."""
        log.warning(f"🚨 RISK DETECTED: {risk_event.worker_id} - {risk_event.risk_level}")

        # Only generate interventions for HIGH and CRITICAL risks
        if risk_event.risk_level in [RiskLevel.HIGH, RiskLevel.CRITICAL]:
            intervention = self.generate_intervention(risk_event)
            self.publish_intervention(intervention)

            # Display intervention
            log.info("=" * 60)
            log.info("💡 INTERVENTION GENERATED")
            log.info(f"Action: {intervention.immediate_action}")
            log.info("Recommendations:")
            for rec in intervention.recommendations:
                log.info(f"  • {rec}")
            log.info(f"Rationale: {intervention.rationale}")
            log.info("=" * 60)
        else:
            log.info(f"Risk level {risk_event.risk_level} - monitoring only")

    def run(self):
        """Run the agent continuously."""
        log.info("=" * 60)
        log.info("ErgoOps AI Agent - Starting")
        log.info("=" * 60)
        log.info("Monitoring risk events and generating interventions...")

        # Create consumer with this agent's callback
        consumer = RiskEventConsumer(callback=self.handle_risk_event)
        consumer.run()


def main():
    """Entry point for AI agent."""
    agent = ErgoOpsAgent()
    agent.run()


if __name__ == "__main__":
    main()
