# ErgoStream - Quick Start Guide

Get ErgoStream running in 15 minutes.

## 1. Prerequisites

- Python 3.9 or higher
- Confluent Cloud account (free trial available)
- **No API keys needed** - uses rule-based interventions!

## 2. Installation

```bash
# Clone/navigate to project
cd ErgoStream

# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (Mac/Linux)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## 3. Configure Confluent Cloud

### Get Free Trial
1. Go to https://www.confluent.io/confluent-cloud/tryfree/
2. Sign up (get $400 in free credits)
3. Create a Basic cluster

### Get Credentials
1. Create Kafka API key
2. Create Schema Registry API key
3. Copy bootstrap servers URL
4. Copy Schema Registry URL

See [CONFLUENT_SETUP.md](./CONFLUENT_SETUP.md) for detailed instructions.

## 4. Configure Environment

```bash
# Copy example environment file
cp .env.example .env

# Edit .env with your credentials
# Use notepad, vim, or any text editor
notepad .env
```

Required variables (only Confluent Cloud):
```bash
CONFLUENT_BOOTSTRAP_SERVERS=your-bootstrap-servers
CONFLUENT_API_KEY=your-api-key
CONFLUENT_API_SECRET=your-api-secret
CONFLUENT_SCHEMA_REGISTRY_URL=your-sr-url
CONFLUENT_SCHEMA_REGISTRY_API_KEY=your-sr-key
CONFLUENT_SCHEMA_REGISTRY_API_SECRET=your-sr-secret

# No AI API keys needed! Using rule-based agent
```

## 5. Create Kafka Topics

```bash
python scripts/create_topics.py
```

You should see:
```
✅ Topic 'ergo.worker.motion' created successfully
✅ Topic 'ergo.risk.detected' created successfully
...
```

## 6. Deploy Flink SQL

1. Open Confluent Cloud UI
2. Go to **Stream Processing** → **Flink SQL**
3. Open workspace
4. Run queries from `src/flink/risk_detection.sql`:
   - Copy one statement at a time
   - Start with CREATE TABLE statements
   - Then run the INSERT INTO queries

## 7. Start the System

Open 3 terminal windows:

### Terminal 1: Data Producer
```bash
python -m src.producers.worker_telemetry
```

Expected output:
```
ErgoStream - Worker Telemetry Producer
Starting telemetry stream (interval: 60.00s)
Delivered message to ergo.worker.motion [0] @ offset 0
```

### Terminal 2: AI Agent
```bash
python -m src.agents.ergo_ops_agent
```

Expected output:
```
ErgoOps AI Agent - Starting
Monitoring risk events and generating interventions...
```

### Terminal 3: Dashboard
```bash
streamlit run src/dashboard/app.py
```

Browser opens automatically at http://localhost:8501

## 8. Verify It's Working

### Check 1: Producer
Look for messages in Terminal 1:
```
Delivered message to ergo.worker.motion [0] @ offset 10 | Total: 10
```

### Check 2: Confluent Cloud
1. Go to Topics → `ergo.worker.motion`
2. Click "Messages" tab
3. You should see events flowing

### Check 3: Flink Processing
In Flink SQL workspace:
```sql
SELECT COUNT(*) FROM worker_motion;
```
Should return growing count.

### Check 4: Risk Detection
After 5-10 minutes, check:
```sql
SELECT * FROM risk_detected ORDER BY timestamp DESC LIMIT 5;
```

### Check 5: AI Agent
When high risk detected, Terminal 2 shows:
```
🚨 RISK DETECTED: W1042 - HIGH
💡 INTERVENTION GENERATED
Action: Rotate Worker W1042 from pallet picking immediately
```

### Check 6: Dashboard
Should show:
- Active workers count
- Risk alerts appearing
- Station risk map updating
- AI interventions listed

## 9. Trigger a Test Alert

Want to see it work faster? Edit `src/producers/simulator.py`:

Change line ~75:
```python
# From:
elif risk_roll < 0.05:
    return RiskLevel.HIGH

# To (increase high-risk probability):
elif risk_roll < 0.30:  # 30% chance
    return RiskLevel.HIGH
```

Restart the producer and you'll see more frequent alerts!

## Troubleshooting

### "ModuleNotFoundError"
```bash
# Make sure you're in virtual environment
pip install -r requirements.txt
```

### "Authentication failed"
- Check API keys in `.env` (no extra spaces!)
- Verify keys are correct in Confluent Cloud

### "Topic not found"
```bash
python scripts/create_topics.py
```

### "No risk events"
- Wait 5-10 minutes (Flink processes in 5-minute windows)
- Increase simulation speed in `.env`:
  ```bash
  SIMULATION_SPEED=10.0  # 10x faster
  ```

### "Dashboard not updating"
- Check that producer is running
- Verify Flink queries are active
- Check Topics in Confluent Cloud UI

## Next Steps

1. **Customize Risk Scoring** - Edit `src/flink/risk_detection.sql`
2. **Adjust Simulation** - Modify `src/producers/simulator.py`
3. **Enhance Dashboard** - Edit `src/dashboard/app.py`
4. **Add More Workers** - Change `NUM_WORKERS` in `.env`
5. **Prepare Demo** - See [DEMO.md](./DEMO.md)

## Demo Tips

For your Confluent Developer Day submission:

1. **Start with the business problem** - workplace injuries
2. **Show live data flowing** - open Confluent Cloud UI
3. **Trigger a high-risk scenario** - demonstrate detection
4. **Show AI intervention** - live generation
5. **Explain the architecture** - Kafka → Flink → AI → Action

Good luck! 🚀
