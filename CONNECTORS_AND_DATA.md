# 🔌 Connectors & Mock Data Generation Guide

## Quick Answer

### Connectors We're Using

✅ **Custom Python Producer** (acts as application connector)
- File: [src/producers/worker_telemetry.py](src/producers/worker_telemetry.py)
- Simulates: IoT sensor system (wearable devices)
- Sends to: `ergo.worker.motion` topic

### Mock Data Generation

✅ **Python Simulator Class**
- File: [src/producers/simulator.py](src/producers/simulator.py)
- Library: Faker + NumPy + Python random
- Generates: Realistic worker telemetry with behavioral patterns

---

## 🔌 Connector Architecture

### What Confluent Challenge Asks For

The "Most Impactful App" challenge requires demonstrating:
- ✅ **Connectors** - Getting data into Kafka
- ✅ **Stream Processing** - Flink
- ✅ **Stream Governance** - Schema Registry, data quality

### Our Approach: Custom Python Connectors

```
Real-World Equivalent          Our Implementation
─────────────────────          ──────────────────

[IoT Sensors/Wearables]   →   [Python Simulator]
         ↓                              ↓
[IoT Gateway/Connector]   →   [Python Producer]
         ↓                              ↓
   [Kafka Topics]          →      [Kafka Topics]
```

**Why this counts as "Connectors":**
- ✅ Demonstrates connector pattern
- ✅ Production-realistic (many companies use custom connectors)
- ✅ Shows proper data streaming architecture
- ✅ Extensible to add official Confluent Connectors

---

## 📊 Mock Data Generation Deep Dive

### Location
[src/producers/simulator.py](src/producers/simulator.py)

### Class Structure

```python
class WorkerSimulator:
    """Simulates realistic worker motion patterns."""
    
    def __init__(self, worker_id, station_id):
        self.worker_id = worker_id
        self.station_id = station_id
        self.current_task = random.choice(TaskTypes)
        self.task_duration = 0.0
        self.risk_state = RiskLevel.LOW
        self.fatigue_factor = 0.0  # Key behavioral variable!
```

### Risk Level Data Ranges

```python
# 🟢 LOW RISK (Safe working conditions)
def _generate_normal_motion():
    return {
        "back_angle": 5-25°,        # Minimal trunk flexion
        "neck_angle": 5-20°,        # Neutral neck
        "load_kg": 2-12,            # Light loads
        "repetition_rate": 3-8/min  # Sustainable pace
    }

# 🟡 MODERATE RISK (Warning signs)
def _generate_moderate_risk_motion():
    return {
        "back_angle": 25-45°,       # Concerning flexion
        "neck_angle": 20-35°,       # Neck strain
        "load_kg": 12-18,           # Heavy loads
        "repetition_rate": 8-14/min # Increasing pace
    }

# 🔴 HIGH RISK (Injury likely)
def _generate_high_risk_motion():
    return {
        "back_angle": 45-75°,       # Dangerous flexion
        "neck_angle": 35-60°,       # Severe neck strain
        "load_kg": 18-30,           # Very heavy loads
        "repetition_rate": 14-22/min # Excessive pace
    }
```

### Behavioral Patterns (What Makes It Realistic)

#### 1. Fatigue Accumulation
```python
def _increase_fatigue(self):
    """Worker gets tired over time"""
    self.fatigue_factor = min(1.0, self.fatigue_factor + 0.01)
    # +1% per minute = 100% after ~100 minutes
```

**Impact:** More fatigue = higher injury risk probability

#### 2. Task Rotation
```python
def _maybe_change_task(self):
    """Simulate task rotation every 15-45 minutes"""
    if self.task_duration > random.uniform(15, 45):
        self.current_task = random.choice(TaskTypes)
        self.task_duration = 0.0
        self.fatigue_factor *= 0.5  # Rest reduces fatigue!
        return True
```

**Impact:** Prevents continuous high-risk exposure

#### 3. Probabilistic Risk
```python
def _determine_risk_state(self):
    """Smart probability based on fatigue"""
    risk_roll = random.random()
    
    # Tired workers = higher risk
    if self.fatigue_factor > 0.7 and risk_roll < 0.15:
        return RiskLevel.HIGH  # 15% chance when exhausted
    
    elif self.fatigue_factor > 0.5 and risk_roll < 0.25:
        return RiskLevel.MODERATE  # 25% when tired
    
    # Base probability (fresh worker)
    elif risk_roll < 0.05:
        return RiskLevel.HIGH  # 5% base chance
    elif risk_roll < 0.15:
        return RiskLevel.MODERATE  # 15% base chance
    else:
        return RiskLevel.LOW  # 80% of the time
```

#### 4. Gaussian Noise (Realism)
```python
# Add natural variation
motion["back_angle"] += random.gauss(0, 3)  # ±3° variation
motion["load_kg"] += random.gauss(0, 1.5)   # ±1.5kg variation
motion["repetition_rate"] += random.gauss(0, 1)

# Clamp to realistic ranges
motion["back_angle"] = max(0, min(180, motion["back_angle"]))
```

**Impact:** No two events are identical, like real sensors

---

## 🎯 Complete Data Flow

### Step-by-Step Execution

```bash
# Terminal 1: Start producer
python -m src.producers.worker_telemetry
```

**What happens:**

1. **WorkforceSimulator initializes** (line 147)
   ```python
   simulator = WorkforceSimulator(num_workers=10)
   # Creates 10 WorkerSimulator instances
   # Assigns random stations (A-01 through B-10)
   ```

2. **Every 60 seconds** (configurable):
   ```python
   for batch in simulator.generate_stream(interval_seconds=60):
       # Generates 1 event per worker = 10 events/minute
   ```

3. **For each worker:**
   ```python
   event = worker.generate_event()
   # 1. Increases task duration
   # 2. Accumulates fatigue
   # 3. Maybe rotates task
   # 4. Determines risk state
   # 5. Generates motion data
   # 6. Adds noise
   # 7. Creates WorkerMotionEvent object
   ```

4. **Producer sends to Kafka:**
   ```python
   producer.produce(
       topic='ergo.worker.motion',
       key=worker_id,  # Partition by worker
       value=json.dumps(event)
   )
   ```

5. **Confluent Cloud receives:**
   - Stores in topic partition
   - Validates against schema (if enabled)
   - Updates metrics

6. **Flink processes:**
   - Reads from `worker_motion` table
   - Aggregates over 5-minute windows
   - Calculates risk scores
   - Writes HIGH risks to `risk_detected` topic

7. **Agent consumes:**
   - Reads from `risk_detected` topic
   - Generates intervention (rule-based)
   - Writes to `interventions` topic

8. **Dashboard displays:**
   - Consumes all topics
   - Updates visualization
   - Auto-refreshes every 2 seconds

---

## 🔧 Customizing Mock Data

### Quick Tweaks for Demo

#### 1. More High-Risk Events (Demo-Friendly!)

**File:** `src/producers/simulator.py`  
**Line:** 74-75

```python
# BEFORE (realistic - 5% high risk)
elif risk_roll < 0.05:
    return RiskLevel.HIGH

# AFTER (demo-friendly - 30% high risk)
elif risk_roll < 0.30:  # Increase to 30%
    return RiskLevel.HIGH
```

**Result:** More frequent alerts in demo!

#### 2. Faster Simulation

**File:** `.env`

```bash
# Run 10x faster
SIMULATION_SPEED=10.0
```

**Result:** 1 minute of data in 6 seconds

#### 3. More Workers

**File:** `.env`

```bash
# Simulate bigger facility
NUM_WORKERS=50
```

**Result:** 50 events per batch instead of 10

#### 4. Extreme Risk Scenario

**File:** `src/producers/simulator.py`  
**Add after line 79:**

```python
def _generate_critical_motion(self) -> dict:
    """Generate CRITICAL risk for demo"""
    return {
        "back_angle": random.uniform(70, 90),  # Extreme!
        "neck_angle": random.uniform(50, 70),
        "load_kg": random.uniform(28, 40),
        "repetition_rate": random.uniform(20, 28),
    }
```

Then modify `generate_event()` to use it occasionally.

---

## 📈 Sample Data Output

### What Goes Into Kafka

```json
{
  "worker_id": "W1042",
  "timestamp": "2026-09-22T14:30:42.123Z",
  "back_angle": 47.3,
  "neck_angle": 23.8,
  "load_kg": 19.2,
  "repetition_rate": 13.5,
  "station_id": "B-17",
  "task_type": "pallet_picking",
  "duration_minutes": 14.2,
  "temperature_celsius": 22.3,
  "humidity_percent": 52.1
}
```

### Viewing in Confluent Cloud

1. **Confluent Cloud → Topics → ergo.worker.motion**
2. **Click "Messages" tab**
3. **See real-time events streaming**
4. **Filter by worker_id to track individual workers**

---

## 🎪 For Your Demo Presentation

### What to Say About Connectors

**Script:**
> "ErgoStream uses custom Python connectors to stream worker telemetry from simulated IoT sensors. In production, these would connect to real wearable devices like Kinetic or StrongArm sensors, motion capture systems, or smart PPE equipment. The connector architecture is extensible - we can easily add Confluent's database connectors for ERP integration, HTTP connectors for REST APIs, or IoT connectors for edge devices."

### What to Say About Data Generation

**Script:**
> "Our synthetic data generator creates realistic worker behavior patterns including fatigue accumulation, task rotation, and probabilistic risk scenarios. The data matches real ergonomic research - back angles, load weights, and repetition rates are based on NIOSH guidelines and OSHA standards. This lets us demonstrate the full streaming intelligence platform without needing actual sensor hardware."

---

## 🚀 Adding Official Confluent Connectors (Optional)

If you want to demonstrate **official Confluent Connectors**, you can add:

### Option 1: Datagen Source Connector

1. Confluent Cloud → Connectors → "Add connector"
2. Search "Datagen"
3. Select "Datagen Source"
4. Use QuickStart template or custom schema
5. Point to a new topic: `ergo.datagen.test`

**Benefit:** Shows zero-code data generation

### Option 2: HTTP Source Connector

1. Set up simple REST API endpoint (even httpbin.org)
2. Add HTTP Source Connector
3. Poll endpoint for data
4. Write to topic

**Benefit:** Shows REST API integration

### Option 3: S3 Sink Connector

1. Add S3 Sink Connector
2. Export `ergo.risk.detected` to S3 bucket
3. Shows data lake integration

**Benefit:** Demonstrates data pipeline to analytics

---

## ✅ Summary

**Connectors:**
- ✅ Custom Python producers (production-realistic)
- ✅ Acts as IoT sensor connector
- ✅ Demonstrates proper streaming architecture
- ✅ Extensible to official Confluent Connectors

**Mock Data:**
- ✅ Realistic behavioral patterns
- ✅ Fatigue modeling
- ✅ Task rotation
- ✅ Probabilistic risk
- ✅ Gaussian noise
- ✅ Easy to customize for demo

**Files to Know:**
- `src/producers/simulator.py` - Data generation logic
- `src/producers/worker_telemetry.py` - Kafka producer (connector)
- `.env` - Configuration (NUM_WORKERS, SIMULATION_SPEED)

**Quick Demo Tweak:**
Edit line 74 in `simulator.py`: change `0.05` to `0.30` for more alerts!

---

Need help customizing? Check:
- [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md) - Full setup guide
- [docs/CONNECTORS.md](docs/CONNECTORS.md) - Detailed connector docs
- [HACKATHON_READY.md](HACKATHON_READY.md) - Quick start guide
