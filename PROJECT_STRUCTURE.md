# ErgoStream - Project Structure

Complete Python project scaffold for the Confluent Developer Day hackathon.

## 📁 Directory Structure

```
ErgoStream/
├── src/                          # Source code
│   ├── __init__.py
│   ├── producers/                # Kafka producers
│   │   ├── __init__.py
│   │   ├── simulator.py         # Synthetic data generator
│   │   └── worker_telemetry.py  # Main producer
│   ├── consumers/                # Kafka consumers
│   │   ├── __init__.py
│   │   └── risk_consumer.py     # Risk event consumer
│   ├── agents/                   # AI agents
│   │   ├── __init__.py
│   │   └── ergo_ops_agent.py    # Claude-powered AI agent
│   ├── models/                   # Data models
│   │   ├── __init__.py
│   │   └── events.py            # Pydantic models for events
│   ├── flink/                    # Flink SQL queries
│   │   └── risk_detection.sql   # Risk detection logic
│   ├── dashboard/                # Streamlit dashboard
│   │   ├── __init__.py
│   │   └── app.py               # Real-time dashboard
│   └── utils/                    # Utilities
│       ├── __init__.py
│       ├── config.py            # Configuration management
│       └── logger.py            # Logging setup
│
├── config/                       # Configuration files
│   └── topics.yaml              # Kafka topic definitions
│
├── scripts/                      # Utility scripts
│   └── create_topics.py         # Topic creation script
│
├── docs/                         # Documentation
│   ├── CONFLUENT_SETUP.md       # Confluent Cloud setup guide
│   └── QUICKSTART.md            # Quick start guide
│
├── tests/                        # Tests
│   ├── unit/
│   └── integration/
│
├── logs/                         # Log files (generated)
│
├── .env                          # Environment variables (edit this!)
├── .env.example                  # Environment template
├── .gitignore                    # Git ignore rules
├── requirements.txt              # Python dependencies
├── README.md                     # Main documentation
├── Research.md                   # Your original research
└── PROJECT_STRUCTURE.md          # This file
```

## 🔑 Key Files to Edit

### 1. `.env` - **EDIT FIRST**
Your Confluent Cloud and API credentials.

### 2. `src/producers/simulator.py`
- Adjust risk probabilities
- Modify worker behavior patterns
- Add new task types

### 3. `src/flink/risk_detection.sql`
- Customize risk scoring algorithm
- Adjust detection thresholds
- Add new risk factors

### 4. `src/dashboard/app.py`
- Customize visualizations
- Add new metrics
- Enhance UI/UX

### 5. `config/topics.yaml`
- Add/remove topics
- Adjust partition counts
- Change retention policies

## 🚀 How to Run

### Step 1: Install
```bash
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

### Step 2: Configure
Edit `.env` with your Confluent Cloud credentials.

### Step 3: Setup Confluent
```bash
# Create topics
python scripts/create_topics.py

# Deploy Flink SQL (manually in Confluent Cloud UI)
# Copy from: src/flink/risk_detection.sql
```

### Step 4: Run Components

**Terminal 1 - Producer:**
```bash
python -m src.producers.worker_telemetry
```

**Terminal 2 - AI Agent:**
```bash
python -m src.agents.ergo_ops_agent
```

**Terminal 3 - Dashboard:**
```bash
streamlit run src/dashboard/app.py
```

## 📊 Data Flow

```
┌─────────────────┐
│ WorkerSimulator │  Generates synthetic ergonomic data
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Kafka Producer  │  Publishes to ergo.worker.motion
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Kafka Topics   │  Event streaming backbone
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Flink SQL      │  Real-time risk detection
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ risk.detected   │  High-risk events
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ ErgoOps Agent   │  AI-powered interventions (Claude)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ interventions   │  Recommendations
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Dashboard     │  Real-time visualization
└─────────────────┘
```

## 🔧 Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Event Streaming | **Confluent Cloud (Kafka)** | Real-time event backbone |
| Stream Processing | **Apache Flink (SQL)** | Risk detection logic |
| Data Generator | **Python + Faker** | Synthetic telemetry |
| Dashboard | **Streamlit** | Real-time visualization |
| Data Models | **Pydantic** | Type-safe event schemas |
| Configuration | **Python-dotenv** | Environment management |

## 🎯 Features Implemented

### ✅ Core Features
- [x] Synthetic worker telemetry generation
- [x] Kafka producer for motion events
- [x] Flink SQL risk detection
- [x] AI-powered intervention agent
- [x] Real-time dashboard
- [x] Multi-event type support

### ✅ Confluent Platform Features
- [x] Kafka topics with proper configuration
- [x] Schema Registry integration
- [x] Flink stream processing
- [x] Stream governance ready
- [x] Proper topic organization

### ✅ AI Integration
- [x] Claude API integration
- [x] Context-aware recommendations
- [x] JSON-structured responses
- [x] Fallback logic

### ✅ Dashboard Features
- [x] Live metrics
- [x] Station risk map
- [x] Alert timeline
- [x] Intervention display
- [x] Auto-refresh

## 📝 What You Need to Provide

1. **Confluent Cloud Account**
   - Sign up: https://www.confluent.io/confluent-cloud/tryfree/
   - Get $400 free credits

2. **Confluent Credentials** (add to `.env`):
   - Bootstrap servers URL
   - Kafka API key + secret
   - Schema Registry URL
   - Schema Registry API key + secret

## 🎬 Demo Script

### Opening (30 seconds)
*"Workplace injuries don't happen in monthly reports—they happen in real time. Most companies analyze ergonomic risk after the fact. ErgoStream changes that."*

### Architecture (60 seconds)
Show diagram:
- Worker telemetry → Kafka
- Flink processing
- AI intervention
- Real-time dashboard

### Live Demo (3 minutes)
1. Show Confluent Cloud - topics with flowing data
2. Open dashboard - live worker monitoring
3. Trigger high-risk scenario
4. Show Flink detecting risk
5. AI agent generates intervention
6. Dashboard updates in real-time

### Impact (30 seconds)
*"This isn't just monitoring—it's prevention. Real-time intelligence that protects workers before injuries happen."*

## 📚 Next Steps

1. **Review:** Read [docs/QUICKSTART.md](docs/QUICKSTART.md)
2. **Setup:** Follow [docs/CONFLUENT_SETUP.md](docs/CONFLUENT_SETUP.md)
3. **Test:** Run the system end-to-end
4. **Customize:** Adjust risk scoring and simulation
5. **Demo:** Practice your presentation
6. **Submit:** Enter Confluent Developer Day competition

## 🆘 Troubleshooting

See [docs/QUICKSTART.md](docs/QUICKSTART.md#troubleshooting)

## 🏆 Submission Checklist

- [ ] Confluent Cloud cluster created
- [ ] All topics created
- [ ] Flink SQL deployed
- [ ] Producer generates realistic data
- [ ] Risk detection working
- [ ] AI agent generating interventions
- [ ] Dashboard showing live updates
- [ ] Demo script prepared
- [ ] Architecture diagram ready
- [ ] Video/screenshots captured
- [ ] Submission form filled

## 📧 Support

If you encounter issues:
1. Check [docs/QUICKSTART.md](docs/QUICKSTART.md#troubleshooting)
2. Review Confluent docs: https://docs.confluent.io

---

**Built with Python for Confluent AI Developer Day 2026** 🚀
