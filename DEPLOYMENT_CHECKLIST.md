# 🚀 ErgoStream - Deployment Checklist

Use this checklist to get ErgoStream running for your hackathon demo.

## Phase 1: Setup (15 minutes)

### ☐ 1. Python Environment
```bash
cd ErgoStream
python -m venv venv

# Windows
venv\Scripts\activate

# Mac/Linux
source venv/bin/activate

pip install -r requirements.txt
```

**Verify:** `pip list | grep confluent-kafka` shows version 2.3.0

---

### ☐ 2. Get Confluent Cloud Credentials

1. **Sign up:** https://www.confluent.io/confluent-cloud/tryfree/
   - Use your email
   - Get $400 free credits

2. **Create Basic Cluster:**
   - Name: `ergostream-cluster`
   - Region: Choose closest to you
   - Type: Basic (free tier)

3. **Create Kafka API Key:**
   - Cluster → Settings → API Keys
   - Create key with global access
   - **SAVE BOTH:** API Key + Secret (you won't see secret again!)

4. **Create Schema Registry Key:**
   - Schema Registry tab
   - Create credential
   - **SAVE:** Key + Secret

5. **Copy URLs:**
   - Bootstrap servers: `pkc-xxxxx.region.provider.confluent.cloud:9092`
   - Schema Registry: `https://psrc-xxxxx.region.provider.confluent.cloud`

---

### ☐ 3. Configure .env File

Copy `.env.example` to `.env` and edit:

```bash
CONFLUENT_BOOTSTRAP_SERVERS=<paste-bootstrap-servers>
CONFLUENT_API_KEY=<paste-kafka-key>
CONFLUENT_API_SECRET=<paste-kafka-secret>
CONFLUENT_SCHEMA_REGISTRY_URL=<paste-sr-url>
CONFLUENT_SCHEMA_REGISTRY_API_KEY=<paste-sr-key>
CONFLUENT_SCHEMA_REGISTRY_API_SECRET=<paste-sr-secret>

ENVIRONMENT=production
LOG_LEVEL=INFO
NUM_WORKERS=10
SIMULATION_SPEED=1.0
```

**No AI keys needed!** ✅

---

### ☐ 4. Verify Setup

```bash
python scripts/verify_setup.py
```

**Expected output:**
```
✅ PASS - Dependencies
✅ PASS - Configuration
✅ PASS - Kafka Connection
✅ PASS - Agent Logic

🎉 All checks passed!
```

**If fails:** Check your credentials in `.env`

---

## Phase 2: Confluent Configuration (10 minutes)

### ☐ 5. Create Kafka Topics

```bash
python scripts/create_topics.py
```

**Expected:** 7 topics created
- `ergo.worker.motion`
- `ergo.risk.detected`
- `ergo.interventions`
- And 4 more...

**Verify in Confluent Cloud UI:**
- Topics menu shows all 7 topics

---

### ☐ 6. Enable Flink

1. Confluent Cloud → Stream Processing
2. Click "Enable Flink"
3. Choose compute pool: **5 CFUs** (minimum)
4. Wait ~2 minutes for provisioning

---

### ☐ 7. Deploy Flink SQL

1. Stream Processing → Flink SQL → Open workspace

2. **Create environment variable:**
   ```sql
   SET 'BOOTSTRAP_SERVERS' = 'your-bootstrap-servers-here';
   ```

3. **Run these queries ONE AT A TIME** (copy from `src/flink/risk_detection.sql`):

   **Query 1:** Create `worker_motion` table (lines 8-28)
   ```sql
   CREATE TABLE worker_motion (...);
   ```
   ✅ Should say "Table created"

   **Query 2:** Create `risk_detected` table (lines 33-53)
   ```sql
   CREATE TABLE risk_detected (...);
   ```
   ✅ Should say "Table created"

   **Query 3:** Create temporary views and INSERT (lines 58-170)
   ```sql
   CREATE TEMPORARY VIEW worker_motion_aggregated AS ...
   CREATE TEMPORARY VIEW risk_scores AS ...
   INSERT INTO risk_detected SELECT ...
   ```
   ✅ Should show "Running" in statements list

4. **Verify Flink is processing:**
   ```sql
   SELECT COUNT(*) FROM worker_motion;
   ```
   Initially returns 0 (no data yet)

---

## Phase 3: Launch System (5 minutes)

### ☐ 8. Start Producer (Terminal 1)

```bash
python -m src.producers.worker_telemetry
```

**Expected output:**
```
ErgoStream - Worker Telemetry Producer
Starting telemetry stream (interval: 60.00s)
Simulating 10 workers
Delivered message to ergo.worker.motion [0] @ offset 0 | Total: 10
```

**Keep running!** ✅

---

### ☐ 9. Start Agent (Terminal 2)

```bash
python -m src.agents.ergo_ops_agent
```

**Expected output:**
```
ErgoOps Rule-Based Agent initialized
Using deterministic intervention logic
Monitoring risk events and generating interventions...
```

**Wait for risk events...** (takes 5-10 min for first HIGH risk)

---

### ☐ 10. Start Dashboard (Terminal 3)

```bash
streamlit run src/dashboard/app.py
```

**Expected:**
- Browser opens to http://localhost:8501
- Dashboard shows "Active Workers: 10"
- Station risk map appears
- Data updates every 2 seconds

---

## Phase 4: Verification (10 minutes)

### ☐ 11. Verify Data Flow

**In Confluent Cloud UI:**

1. **Topics → ergo.worker.motion:**
   - Click "Messages" tab
   - Should see JSON events flowing
   - Throughput graph shows activity

2. **Flink SQL Workspace:**
   ```sql
   SELECT * FROM worker_motion ORDER BY timestamp DESC LIMIT 5;
   ```
   Should show recent events

3. **Topics → ergo.risk.detected:**
   - Wait 5-10 minutes for first events
   - Check "Messages" tab
   - Should see HIGH risk events

---

### ☐ 12. Trigger Test Alert (Optional)

Want to see it faster? Edit `src/producers/simulator.py`:

**Line 75-76:**
```python
# Change from:
elif risk_roll < 0.05:
    return RiskLevel.HIGH

# To:
elif risk_roll < 0.30:  # 30% chance of high risk
    return RiskLevel.HIGH
```

**Restart producer** (Ctrl+C, then restart)

Within 1-2 minutes you should see:
```
🚨 RISK DETECTED: W1042 - HIGH
💡 INTERVENTION GENERATED
Action: Rotate Worker W1042 from pallet_picking immediately
```

---

## Phase 5: Demo Preparation (15 minutes)

### ☐ 13. Prepare Demo Story

**Opening (30 seconds):**
> "Workplace injuries don't happen in monthly reports—they happen in real time. Most companies analyze ergonomic risk after the fact. ErgoStream changes that by detecting and preventing injuries as they happen."

**Architecture (60 seconds):**
Show diagram and explain:
1. Workers generate motion events
2. Kafka streams events to Confluent Cloud
3. Flink detects high-risk patterns in real time
4. Agent generates intelligent interventions
5. Dashboard alerts supervisors immediately

**Live Demo (3 minutes):**
1. **Show Confluent Cloud UI** - "Live data streaming from 10 workers"
2. **Show Dashboard** - "Real-time monitoring across all stations"
3. **Point to risk map** - "Color-coded by risk level"
4. **Wait for/trigger alert** - "Here's a high-risk detection"
5. **Show agent output** - "Intervention generated instantly"
6. **Show dashboard update** - "Supervisor sees alert immediately"

**Impact (30 seconds):**
> "This prevents injuries before they happen. For a 1,000-worker facility, preventing just 10 injuries per year saves $500,000. This is real-time intelligence that protects workers and reduces costs."

---

### ☐ 14. Test Your Full Demo

**Run through once:**
1. ✅ All 3 terminals running
2. ✅ Dashboard updating
3. ✅ Can navigate Confluent Cloud UI
4. ✅ Can explain each component
5. ✅ Can show risk detection happening
6. ✅ Can explain business impact

**Time it:** Should be 5-7 minutes total

---

### ☐ 15. Take Screenshots

Capture for your submission:
- [ ] Confluent Cloud cluster overview
- [ ] Topics with message throughput
- [ ] Flink SQL queries running
- [ ] Dashboard showing active workers
- [ ] Dashboard showing high-risk alert
- [ ] Agent terminal with intervention
- [ ] Architecture diagram

---

## Phase 6: Submission (10 minutes)

### ☐ 16. Prepare Submission Materials

**Required:**
- [x] Project running and tested ✅
- [ ] GitHub repo (optional but recommended)
- [ ] Demo video or screenshots
- [ ] Architecture diagram
- [ ] Impact statement

**Submission Form:** https://docs.google.com/forms/d/e/1FAIpQLSeNcjEcA3wYG6hzIIdtRG1IbeA4owiMMNCAA5lpNDducI4GJA/viewform

**Your Key Points:**
1. **Problem:** Workplace injuries cost billions, but risk detection is reactive
2. **Solution:** Real-time streaming detects and prevents injuries
3. **Tech:** Full Confluent stack - Kafka, Flink, Schema Registry, governance-ready
4. **Impact:** Prevents injuries, reduces costs, scalable to any size facility
5. **Unique:** Closed-loop system (detect → analyze → intervene → monitor)

---

## Troubleshooting

### No risk events appearing?
- Wait 5-10 minutes (Flink processes in windows)
- Check Flink SQL is running (green status)
- Increase `SIMULATION_SPEED=10.0` in `.env`
- Or edit simulator to increase high-risk probability

### Dashboard not updating?
- Check producer is sending events (Confluent UI)
- Check Flink is processing (query worker_motion)
- Refresh browser page

### "Authentication failed"?
- Double-check credentials in `.env`
- No extra spaces or quotes
- Verify keys in Confluent Cloud UI

### Agent not generating interventions?
- Only HIGH/CRITICAL risks trigger interventions
- Check `ergo.risk.detected` topic has events
- Look for errors in agent terminal

---

## 🎉 You're Ready!

**Status Check:**
- ✅ Python environment configured
- ✅ Confluent Cloud cluster running
- ✅ Topics created
- ✅ Flink processing events
- ✅ Producer generating data
- ✅ Agent generating interventions
- ✅ Dashboard showing live updates
- ✅ Demo story prepared
- ✅ Screenshots captured

**Go win that hackathon!** 🏆

---

**Questions?** Check:
- [QUICKSTART.md](docs/QUICKSTART.md) - Quick setup guide
- [CONFLUENT_SETUP.md](docs/CONFLUENT_SETUP.md) - Detailed Confluent instructions
- [HACKATHON_READY.md](HACKATHON_READY.md) - Rule-based agent explanation
- [README.md](README.md) - Project overview
