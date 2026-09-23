# 🚀 ErgoStream - Hackathon Ready!

## ✅ What Changed - Rule-Based Agent


### Why Rule-Based?

✅ **No API Keys Required** - Only need Confluent Cloud credentials  
✅ **More Reliable** - No API rate limits or latency  
✅ **Predictable** - Deterministic interventions every time  
✅ **Zero Cost** - No per-request AI costs  
✅ **Instant Response** - No network calls to external APIs  


### What You Need Now

**ONLY Confluent Cloud:**
- Bootstrap servers URL
- Kafka API key + secret
- Schema Registry URL + keys

**That's it!** No AI API keys needed.

## 🎯 How the Rule-Based Agent Works

The agent uses intelligent decision trees based on ergonomic safety rules:

### Risk Analysis
```python
# Analyzes multiple factors:
- Back angle severity
- Load weight
- Repetition rate  
- Exposure duration
- Task type
- Station context
```

### Intervention Generation
```python
# Generates specific recommendations based on:

IF back_angle >= 50°:
    → "Adjust workstation height"
    → "Provide mechanical lift assist"

IF load_kg >= 20:
    → "Reduce max load to 15kg"
    → "Implement two-person lift"

IF repetition_rate >= 15/min:
    → "Rotate worker every 30 min"
    → "Add micro-breaks"

IF exposure >= 30 min:
    → "Mandate immediate rest break"
    → "Schedule ergonomic assessment"
```

### Output Example

**Risk Event:**
```json
{
  "worker_id": "W1042",
  "risk_level": "HIGH",
  "primary_risk": "Lower back strain",
  "back_angle": 61°,
  "load_kg": 22.4,
  "exposure_minutes": 13.7
}
```

**Generated Intervention:**
```json
{
  "immediate_action": "Rotate Worker W1042 from pallet_picking immediately",
  "recommendations": [
    "Adjust workstation B-17 height to reduce trunk flexion",
    "Provide mechanical lift assist for heavy items",
    "Reduce maximum load at Station B-17 to 15kg",
    "Implement two-person lift protocol for heavy items",
    "Monitor Worker W1042 for next 20 minutes post-intervention",
    "Document incident in ergonomic assessment log"
  ],
  "rationale": "Worker W1042 has been exposed to lower back strain for 13.7 minutes. Contributing factors: excessive trunk flexion, heavy load handling. Immediate intervention required to prevent injury.",
  "expected_risk_reduction": 75.0
}
```

## 🔥 Quick Start (Updated)

### 1. Install (2 minutes)
```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure (5 minutes)
Edit `.env` with **ONLY** Confluent Cloud credentials:
```bash
CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx...
CONFLUENT_API_KEY=your-key
CONFLUENT_API_SECRET=your-secret
CONFLUENT_SCHEMA_REGISTRY_URL=https://psrc-xxxxx...
CONFLUENT_SCHEMA_REGISTRY_API_KEY=your-sr-key
CONFLUENT_SCHEMA_REGISTRY_API_SECRET=your-sr-secret

# Application settings
NUM_WORKERS=10
SIMULATION_SPEED=1.0
```

### 3. Verify Setup
```bash
python scripts/verify_setup.py
```

Should see:
```
✅ PASS - Dependencies
✅ PASS - Configuration  
✅ PASS - Kafka Connection
✅ PASS - Agent Logic

🎉 All checks passed! You're ready to run ErgoStream.
```

### 4. Create Topics
```bash
python scripts/create_topics.py
```

### 5. Deploy Flink SQL
- Open Confluent Cloud → Flink SQL
- Copy from `src/flink/risk_detection.sql`
- Run queries one by one

### 6. Start System (3 terminals)

**Terminal 1 - Producer:**
```bash
python -m src.producers.worker_telemetry
```

**Terminal 2 - Agent:**
```bash
python -m src.agents.ergo_ops_agent
```

**Terminal 3 - Dashboard:**
```bash
streamlit run src/dashboard/app.py
```

## 📊 What You'll See

### Terminal 1 (Producer)
```
Starting telemetry stream (interval: 60.00s)
Delivered message to ergo.worker.motion [0] @ offset 10 | Total: 10
```

### Terminal 2 (Agent)
```
ErgoOps Rule-Based Agent initialized
Using deterministic intervention logic
Monitoring risk events and generating interventions...

🚨 RISK DETECTED: W1042 - HIGH
💡 INTERVENTION GENERATED
Action: Rotate Worker W1042 from pallet_picking immediately
Recommendations:
  • Adjust workstation B-17 height to reduce trunk flexion
  • Provide mechanical lift assist for heavy items
  • Reduce maximum load at Station B-17 to 15kg
  ...
```

### Terminal 3 (Dashboard)
Browser opens showing:
- Active workers: 10
- High risk alerts: 2
- Station risk map (color-coded)
- Live interventions
- Risk timeline graph

## 🎪 Demo Tips

### Speed Up Risk Detection
Want faster alerts for demo? Edit `.env`:
```bash
SIMULATION_SPEED=10.0  # 10x faster
```

Or edit `src/producers/simulator.py:75`:
```python
# Increase high-risk probability
elif risk_roll < 0.30:  # Was 0.05, now 30% chance
    return RiskLevel.HIGH
```

### Story for Judges

**Problem:**
"Workplace injuries happen in real-time, but companies analyze risk after the fact."

**Solution:**
"ErgoStream uses Confluent's streaming platform to detect ergonomic risk as it happens and intervene immediately."

**Demo Flow:**
1. Show Confluent Cloud - live data streaming
2. Open dashboard - workers being monitored
3. Point out station risk map
4. Wait for/trigger high-risk event
5. Show agent generate intervention instantly
6. Highlight the full cycle: detect → analyze → intervene

**Impact:**
"This prevents injuries before they happen. Real-time intelligence that protects workers and reduces costs."

## 🏆 Why This Will Win

✅ **Real Business Problem** - Workplace safety is universal  
✅ **Full Confluent Stack** - Kafka, Flink, Schema Registry, Governance  
✅ **Real-Time Intelligence** - Not just monitoring, actual intervention  
✅ **Production-Ready** - Clean code, proper architecture  
✅ **Measurable Impact** - Injury prevention = cost savings  
✅ **Scalable** - Works for 10 or 10,000 workers  
✅ **Demo-Friendly** - Visual, live, impressive  
✅ **Fast Setup** - No external API dependencies  

## 📝 Files Changed

- [src/agents/ergo_ops_agent.py](src/agents/ergo_ops_agent.py) - Converted to rule-based
- [requirements.txt](requirements.txt) - Removed AI dependencies
- [.env](.env) - Removed AI key requirements
- [.env.example](.env.example) - Updated template
- [scripts/verify_setup.py](scripts/verify_setup.py) - Updated checks
- [README.md](README.md) - Updated documentation
- [docs/QUICKSTART.md](docs/QUICKSTART.md) - Updated instructions
- [docs/CONFLUENT_SETUP.md](docs/CONFLUENT_SETUP.md) - Simplified setup

## 🚀 You're Ready!

Everything is configured for a fast hackathon:

1. ✅ No external API keys needed
2. ✅ Fast, deterministic interventions
3. ✅ Complete Confluent platform demo
4. ✅ Professional, production-ready code
5. ✅ Comprehensive documentation
6. ✅ Visual, impressive dashboard

**Next:** Get your Confluent Cloud credentials and run `verify_setup.py`!

---

**Good luck at the hackathon!** 🏆
