"""Create Kafka topics on Confluent Cloud."""

import yaml
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource
from pathlib import Path

from src.utils import settings, log


def load_topic_config():
    """Load topic configuration from YAML."""
    config_path = Path(__file__).parent.parent / "config" / "topics.yaml"
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def create_topics():
    """Create all topics defined in configuration."""
    # Initialize admin client
    admin_config = settings.kafka_config.copy()
    admin = AdminClient(admin_config)

    # Load topic configuration
    config = load_topic_config()
    topics = config['topics']

    log.info(f"Creating {len(topics)} topics on Confluent Cloud...")

    # Create NewTopic objects
    new_topics = []
    for topic_def in topics:
        new_topic = NewTopic(
            topic=topic_def['name'],
            num_partitions=topic_def.get('partitions', 3),
            replication_factor=topic_def.get('replication_factor', 3),
            config=topic_def.get('config', {})
        )
        new_topics.append(new_topic)
        log.info(f"  • {topic_def['name']} ({topic_def.get('partitions', 3)} partitions)")

    # Create topics
    fs = admin.create_topics(new_topics, request_timeout=30.0)

    # Wait for operations to complete
    for topic, f in fs.items():
        try:
            f.result()  # Blocks until topic is created
            log.info(f"✅ Topic '{topic}' created successfully")
        except Exception as e:
            if "already exists" in str(e).lower():
                log.warning(f"⚠️  Topic '{topic}' already exists")
            else:
                log.error(f"❌ Failed to create topic '{topic}': {e}")

    log.info("Topic creation complete!")


def list_topics():
    """List all topics in the cluster."""
    admin = AdminClient(settings.kafka_config)

    metadata = admin.list_topics(timeout=10)

    log.info("\nExisting topics:")
    for topic in sorted(metadata.topics.keys()):
        if not topic.startswith('_'):  # Skip internal topics
            t = metadata.topics[topic]
            log.info(f"  • {topic} ({len(t.partitions)} partitions)")


if __name__ == "__main__":
    log.info("=" * 60)
    log.info("ErgoStream - Topic Creation")
    log.info("=" * 60)

    try:
        create_topics()
        print()
        list_topics()

    except KeyboardInterrupt:
        log.info("\nOperation cancelled")
    except Exception as e:
        log.error(f"Error: {e}")
        raise
