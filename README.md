# 🏗️ ErgoStream

**Real-time human intelligence for the physical workplace**

A real-time streaming intelligence system that detects ergonomic risk before an injury happens, built on Confluent's Data Streaming Platform.

## 🎯 Architecture

```
IoT Devices (MQTT) → EMQX Broker → Kafka → Flink SQL → Risk Detection (LIVE!)
                                      ↓
                               ergo.worker.motion
                                      ↓
                              Real-time Processing
                                      ↓
                              ergo.risk.detected ✅
                                      ↓
                            ergo.interventions (Phase 2)
```

### Components (Status)

1. ✅ **MQTT Producer** - Simulates IoT worker sensors
2. ✅ **EMQX Integration** - Industry-standard IoT message broker
3. ✅ **MQTT Source Connector** - Confluent Cloud native connector
4. ✅ **Flink SQL** - Real-time risk detection (HIGH/CRITICAL alerts)
5. 🚧 **Rule-Based Agent** - Intervention generation (in development)
6. 🚧 **Real-time Dashboard** - Live monitoring (planned)

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

## 🎪 Demo Flow (Live)

1. ✅ **MQTT Producer generates realistic telemetry** - Back angles, loads, repetition rates
2. ✅ **EMQX broker ingests IoT data** - Standard MQTT protocol over TLS
3. ✅ **MQTT Connector streams to Kafka** - `ergo.worker.motion` topic
4. ✅ **Flink SQL processes in real-time** - Complex risk scoring algorithm
5. ✅ **HIGH/CRITICAL risks detected** - Real-time alerts in `ergo.risk.detected`

**What to show judges:**
- Live MQTT producer streaming data (10x speed for demo)
- Confluent Cloud Flink query detecting risks in real-time
- Risk events appearing in `ergo.risk.detected` topic with:
  - Risk levels (LOW/MODERATE/HIGH/CRITICAL)
  - Risk scores (0-100)
  - Contributing factors (excessive trunk flexion, heavy loads, etc.)
  - Worker IDs and station tracking

## 📊 Kafka Topics (Active)

- ✅ `ergo.worker.motion` - Raw motion sensor data (6 partitions, ~600 msgs/hour)
- ✅ `ergo.risk.detected` - HIGH/CRITICAL risk alerts (Flink output, ~150 msgs/hour)
- 🚧 `ergo.interventions` - Intervention recommendations (Phase 2 architecture)

## 🤖 Phase 2: Automated Interventions (Planned)

The intervention system is designed to consume from `ergo.risk.detected` and generate:

- Immediate action recommendations (task rotation, rest breaks)
- Workstation adjustment suggestions
- Safety protocol alerts
- Historical trend analysis

**Current Status:** Core risk detection (Phase 1) is complete and operational. Intervention automation is the next development phase.

## 📈 Business Impact

- **Prevent injuries** before they happen
- **Reduce workers' comp costs** by early intervention
- **Improve productivity** through optimized workflows
- **Generate training data** for robotics and physical AI

## 🏆 Confluent Developer Day Submission

Built for the "Most Impactful App" challenge, demonstrating:

- ✅ **Confluent Connectors** - MQTT Source Connector for IoT integration
- ✅ **Stream Processing with Flink SQL** - Complex real-time risk scoring
- ✅ **Schema Registry** - Avro data serialization for Flink output
- ✅ **Multi-partition topics** - Scalable architecture (6 partitions)
- ✅ **Business impact** - Prevents workplace injuries through early detection
- ✅ **Production-ready patterns** - MQTT/TLS, proper partitioning, real-time alerts

**Key Innovation:** Real-time ergonomic risk detection using streaming analytics - detecting dangerous postures, loads, and repetition patterns before injuries occur.

## 📝 License

MIT License - Built for Confluent AI Developer Day 2026
