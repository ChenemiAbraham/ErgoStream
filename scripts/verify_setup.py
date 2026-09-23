"""Verify ErgoStream setup and configuration."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils import log


def check_dependencies():
    """Check if all required packages are installed."""
    log.info("Checking dependencies...")

    required = [
        'confluent_kafka',
        'streamlit',
        'pydantic',
        'loguru',
        'faker',
        'pandas',
        'plotly'
    ]

    missing = []
    for package in required:
        try:
            __import__(package)
            log.info(f"  ✅ {package}")
        except ImportError:
            log.error(f"  ❌ {package} - MISSING")
            missing.append(package)

    if missing:
        log.error(f"\nMissing packages: {', '.join(missing)}")
        log.error("Run: pip install -r requirements.txt")
        return False

    log.info("✅ All dependencies installed\n")
    return True


def check_configuration():
    """Check if .env is configured."""
    log.info("Checking configuration...")

    try:
        from src.utils import settings

        # Check Confluent settings
        if 'xxxxx' in settings.confluent_bootstrap_servers:
            log.error("  ❌ Confluent bootstrap servers not configured")
            log.error("     Edit .env with your Confluent Cloud credentials")
            return False

        log.info(f"  ✅ Bootstrap servers: {settings.confluent_bootstrap_servers}")

        if settings.confluent_api_key == 'your-api-key-here':
            log.error("  ❌ Confluent API key not configured")
            return False

        log.info(f"  ✅ Kafka API key: {settings.confluent_api_key[:8]}...")

        log.info(f"  ✅ Environment: {settings.environment}")
        log.info(f"  ✅ Workers: {settings.num_workers}")

        log.info("✅ Configuration looks good\n")
        return True

    except Exception as e:
        log.error(f"  ❌ Configuration error: {e}")
        log.error("     Make sure .env file exists and is properly formatted")
        return False


def check_kafka_connection():
    """Test Kafka connection."""
    log.info("Testing Kafka connection...")

    try:
        from confluent_kafka.admin import AdminClient
        from src.utils import settings

        admin_config = settings.kafka_config.copy()
        admin = AdminClient(admin_config)

        # List topics (with timeout)
        metadata = admin.list_topics(timeout=10)

        topic_count = len([t for t in metadata.topics.keys() if not t.startswith('_')])

        log.info(f"  ✅ Connected to Confluent Cloud")
        log.info(f"  ✅ Found {topic_count} topics")

        # Check for our topics
        our_topics = [
            'ergo.worker.motion',
            'ergo.risk.detected',
            'ergo.interventions'
        ]

        existing = [t for t in our_topics if t in metadata.topics]
        missing = [t for t in our_topics if t not in metadata.topics]

        if existing:
            log.info(f"  ✅ ErgoStream topics found: {len(existing)}/{len(our_topics)}")
            for topic in existing:
                log.info(f"     • {topic}")

        if missing:
            log.warning(f"  ⚠️  Missing topics: {', '.join(missing)}")
            log.warning(f"     Run: python scripts/create_topics.py")

        log.info("✅ Kafka connection successful\n")
        return True

    except Exception as e:
        log.error(f"  ❌ Kafka connection failed: {e}")
        log.error("     Check your Confluent Cloud credentials in .env")
        return False


def check_agent_logic():
    """Verify agent logic can be imported."""
    log.info("Checking agent logic...")

    try:
        from src.agents import ErgoOpsAgent

        log.info("  ✅ Rule-based agent module loaded")
        log.info("  ✅ No API keys required for intervention generation\n")
        return True

    except Exception as e:
        log.error(f"  ❌ Agent import failed: {e}")
        return False


def main():
    """Run all verification checks."""
    log.info("=" * 60)
    log.info("ErgoStream - Setup Verification")
    log.info("=" * 60)
    log.info("")

    checks = [
        ("Dependencies", check_dependencies),
        ("Configuration", check_configuration),
        ("Kafka Connection", check_kafka_connection),
        ("Agent Logic", check_agent_logic),
    ]

    results = {}
    for name, check_func in checks:
        try:
            results[name] = check_func()
        except KeyboardInterrupt:
            log.info("\nVerification cancelled")
            sys.exit(1)
        except Exception as e:
            log.error(f"Unexpected error in {name}: {e}")
            results[name] = False

    # Summary
    log.info("=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)

    all_passed = all(results.values())

    for name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        log.info(f"{status} - {name}")

    log.info("")

    if all_passed:
        log.info("🎉 All checks passed! You're ready to run ErgoStream.")
        log.info("")
        log.info("Next steps:")
        log.info("  1. python scripts/create_topics.py  (if topics missing)")
        log.info("  2. python -m src.producers.worker_telemetry")
        log.info("  3. python -m src.agents.ergo_ops_agent")
        log.info("  4. streamlit run src/dashboard/app.py")
        log.info("")
        log.info("See docs/QUICKSTART.md for detailed instructions.")
        sys.exit(0)
    else:
        log.error("❌ Some checks failed. Fix the issues above and try again.")
        sys.exit(1)


if __name__ == "__main__":
    main()
