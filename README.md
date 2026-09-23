# 🏗️ ErgoStream

**Real-time human intelligence for the physical workplace**

A real-time streaming intelligence system that detects ergonomic risk before an injury happens, built on Confluent's Data Streaming Platform.

## 🎯 Architecture

```
Worker Telemetry → Kafka Topics → Flink Processing → Risk Detection → AI Agent → Dashboard
```

### Components

1. **Synthetic Data Generator** - Simulates worker motion events
2. **Kafka Producers** - Streams events to Confluent Cloud
3. **Flink SQL** - Real-time risk detection and aggregation
4. **Rule-Based Agent** - Fast, deterministic intervention engine
5. **Real-time Dashboard** - Live monitoring and alerts

## 🚀 Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Edit .env with your Confluent Cloud credentials
```

### 2. Configure Confluent Cloud

See `docs/CONFLUENT_SETUP.md` for detailed setup instructions.

### 3. Run Components

```bash
# Terminal 1: Start data simulator
python -m src.producers.worker_telemetry

# Terminal 2: Start intervention agent
python -m src.agents.ergo_ops_agent

# Terminal 3: Start dashboard
streamlit run src/dashboard/app.py
```

## 📁 Project Structure

```
ergostream/
├── src/
│   ├── producers/          # Kafka producers & data generators
│   │   ├── worker_telemetry.py
│   │   └── simulator.py
│   ├── consumers/          # Kafka consumers
│   │   └── risk_consumer.py
│   ├── agents/             # AI agents
│   │   └── ergo_ops_agent.py
│   ├── models/             # Data models & schemas
│   │   ├── events.py
│   │   └── schemas.py
│   ├── flink/              # Flink SQL queries
│   │   └── risk_detection.sql
│   ├── dashboard/          # Streamlit dashboard
│   │   └── app.py
│   └── utils/              # Shared utilities
│       ├── config.py
│       └── kafka_client.py
├── config/                 # Configuration files
│   └── topics.yaml
├── scripts/                # Utility scripts
│   ├── create_topics.py
│   └── deploy_flink.py
├── tests/
├── docs/
├── requirements.txt
├── .env.example
└── README.md
```

## 🎪 Demo Flow

1. **Generate synthetic worker data** - Realistic motion patterns
2. **Stream to Kafka** - Multiple event types (motion, posture, tasks)
3. **Flink processes in real-time** - Detects high-risk conditions
4. **Agent intervenes** - Rule-based system generates actionable recommendations
5. **Dashboard updates live** - Visual alerts and risk maps

## 📊 Kafka Topics

- `ergo.worker.motion` - Raw motion sensor data
- `ergo.worker.posture` - Posture analysis events
- `ergo.worker.task` - Task assignment events
- `ergo.risk.detected` - High-risk conditions (Flink output)
- `ergo.interventions` - AI-generated interventions

## 🤖 Rule-Based Agent

The ErgoOps Agent monitors risk events and generates contextual interventions using deterministic logic:

- Analyzes risk factors and exposure metrics
- Generates actionable recommendations based on safety rules
- Fast, reliable, no API keys required
- Perfect for hackathon demos!

## 📈 Business Impact

- **Prevent injuries** before they happen
- **Reduce workers' comp costs** by early intervention
- **Improve productivity** through optimized workflows
- **Generate training data** for robotics and physical AI

## 🏆 Confluent Developer Day Submission

Built for the "Most Impactful App" challenge, demonstrating:

- ✅ Confluent Connectors
- ✅ Stream Processing with Flink
- ✅ Stream Governance & Schema Registry
- ✅ Real-time intelligent interventions
- ✅ Business impact on worker safety
- ✅ Fast, reliable, zero external dependencies

## 📝 License

MIT License - Built for Confluent AI Developer Day 2026
