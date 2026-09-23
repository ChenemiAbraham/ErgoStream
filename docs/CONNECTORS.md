# 🔌 Confluent Connectors in ErgoStream

## Current Architecture

ErgoStream demonstrates connector usage through **custom Python producers** that act as application connectors.

### What We're Using

```
┌─────────────────────────────────────────────────┐
│          DATA SOURCES (Simulated)               │
├─────────────────────────────────────────────────┤
│                                                 │
│  1. Worker Telemetry Sensor System              │
│     (IMU sensors, motion capture)               │
│                                                 │
│  2. Warehouse Management System (WMS)           │
│     (Task assignments, schedules)               │
│                                                 │
│  3. Environmental Monitoring System             │
│     (Temperature, humidity sensors)             │
│                                                 │
└────────────┬────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────┐
│      CUSTOM PYTHON CONNECTORS                   │
├─────────────────────────────────────────────────┤
│                                                 │
│  • worker_telemetry.py  → ergo.worker.motion   │
│  • task_connector.py     → ergo.worker.task    │
│  • env_connector.py      → ergo.environment    │
│                                                 │
└────────────┬────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────┐
│          CONFLUENT CLOUD (KAFKA)                │
│                                                 │
│  Topics:                                        │
│  • ergo.worker.motion                           │
│  • ergo.worker.task                             │
│  • ergo.environment                             │
│                                                 │
└────────────┬────────────────────────────────────┘
             │
             ▼
         FLINK PROCESSING
```

## Why Custom Python Connectors?

For this hackathon, custom connectors work better because:

✅ **Flexibility** - Full control over data generation  
✅ **Speed** - No external systems to set up  
✅ **Demo-friendly** - Can trigger scenarios on demand  
✅ **Real-world accurate** - Simulates actual IoT sensor behavior  

## Data Sources Being Simulated

### 1. Worker Motion Sensors (Primary)
**File:** `src/producers/worker_telemetry.py`  
**Simulates:** IMU sensors, wearable devices, motion capture systems  
**Data:**
- Back angle (trunk flexion)
- Neck angle  
- Load weight
- Repetition rate
- Exposure duration

**Real-world equivalent:**
- Wearable IoT sensors (like Kinetic, StrongArm, Soter Analytics)
- Motion capture cameras
- Smart PPE (Personal Protective Equipment)

### 2. Task Management System (Optional)
**Simulates:** Warehouse Management System (WMS) or Manufacturing Execution System (MES)  
**Data:**
- Task assignments
- Work schedules
- Station changes
- Priority levels

**Real-world equivalent:**
- SAP WM/EWM
- Oracle WMS
- Manhattan Associates
- Custom warehouse systems

### 3. Environmental Sensors (In data model)
**Data:**
- Temperature
- Humidity
- Equipment status

**Real-world equivalent:**
- Building management systems
- Industrial IoT sensors

## Mock Data Generation Strategy

### Technology Stack

```python
# Core libraries:
- Faker          # Realistic random data
- NumPy          # Statistical distributions
- Python random  # Controlled randomness
```

### How It Works

Located in [src/producers/simulator.py](../src/producers/simulator.py):

```python
class WorkerSimulator:
    """Generates realistic worker motion patterns"""
    
    # Risk states:
    - LOW risk: back_angle 5-25°, load 2-12kg
    - MODERATE: back_angle 25-45°, load 12-18kg  
    - HIGH: back_angle 45-75°, load 18-30kg
    
    # Behavioral patterns:
    - Fatigue accumulation over time
    - Task rotation
    - Gaussian noise for realism
```

### Data Realism Features

1. **Fatigue Modeling**
   ```python
   fatigue_factor = min(1.0, fatigue_factor + 0.01)
   # More fatigue → higher injury risk
   ```

2. **Task Rotation**
   ```python
   if task_duration > 15-45 minutes:
       rotate_to_new_task()
       fatigue *= 0.5  # Rest reduces fatigue
   ```

3. **Probabilistic Risk**
   ```python
   # 5% chance HIGH risk (normal)
   # 30% chance HIGH risk (can be tuned for demo)
   ```

4. **Gaussian Noise**
   ```python
   back_angle += random.gauss(0, 3)  # ±3° variation
   ```

## Viewing Data Flow in Confluent Cloud

### Step 1: Check Topics
1. Confluent Cloud → Topics
2. Click `ergo.worker.motion`
3. Go to "Messages" tab
4. See JSON events flowing:

```json
{
  "worker_id": "W1042",
  "timestamp": "2026-09-22T14:30:00Z",
  "back_angle": 45.3,
  "neck_angle": 22.1,
  "load_kg": 18.5,
  "repetition_rate": 12.0,
  "station_id": "B-17",
  "task_type": "pallet_picking",
  "duration_minutes": 11.5
}
```

### Step 2: Monitor Throughput
- Check "Throughput" graph
- Should see steady flow (~10 messages/minute for 10 workers)

### Step 3: Schema Registry
- Go to Schema Registry tab
- See JSON schemas for each event type
- Demonstrates governance and data contracts

## Optional: Add More Connector Types

### Option 1: HTTP Source Connector
If you want to demonstrate official Confluent Connectors, add HTTP Source:

1. Confluent Cloud → Connectors → Add connector
2. Select "HTTP Source"
3. Point to a REST API endpoint
4. Configure topic: `ergo.external.events`

### Option 2: Datagen Connector
Use Confluent's built-in data generator:

1. Add Datagen Source Connector
2. Use custom schema for worker events
3. Demonstrates "zero-code" data generation

### Option 3: File/S3 Sink Connector
For archival/analytics:

1. Add S3 Sink Connector
2. Export `ergo.risk.detected` to S3
3. Demonstrates data lake integration

## For Your Hackathon Demo

### What to Say About Connectors

**Approach 1: Emphasize Custom Connectors**
> "We've built custom Python connectors that simulate real IoT sensor systems. In production, these would connect to actual wearable devices, motion capture systems, and warehouse management platforms. The connector architecture makes it easy to swap in real data sources."

**Approach 2: Show Multiple Data Sources**
> "ErgoStream demonstrates connector versatility with three data sources: worker telemetry sensors, task management systems, and environmental monitoring. Each streams independently to Kafka, and Flink joins them in real-time."

**Approach 3: Highlight Extensibility**
> "Our connector architecture is extensible. We can easily add Confluent's HTTP connectors for REST APIs, database CDC connectors for ERP systems, or IoT connectors for edge devices. The streaming backbone remains the same."

## Customizing Mock Data for Demo

### Make it More Impressive

**1. Increase worker count:**
```bash
# In .env
NUM_WORKERS=50  # Simulates bigger facility
```

**2. Speed up simulation:**
```bash
# In .env  
SIMULATION_SPEED=10.0  # 10x faster data generation
```

**3. Trigger high-risk scenarios:**

Edit `src/producers/simulator.py` line 75:
```python
# Increase probability
elif risk_roll < 0.30:  # Was 0.05, now 30%
    return RiskLevel.HIGH
```

**4. Add more task variety:**

Edit `src/models/events.py` to add task types:
```python
class TaskType(str, Enum):
    PALLET_PICKING = "pallet_picking"
    ASSEMBLY = "assembly"
    HEAVY_LIFTING = "heavy_lifting"
    OVERHEAD_DRILLING = "overhead_drilling"
    # Add more...
```

## Architecture Diagram for Submission

Use this visual for your presentation:

```
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│                                                         │
│  [Wearable Sensors]  [WMS]  [Environmental IoT]       │
│         │              │            │                   │
└─────────┼──────────────┼────────────┼───────────────────┘
          │              │            │
          ▼              ▼            ▼
┌─────────────────────────────────────────────────────────┐
│              CUSTOM PYTHON CONNECTORS                   │
│                                                         │
│  worker_telemetry.py | task_mgmt.py | env_monitor.py  │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                CONFLUENT CLOUD                          │
│                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │
│  │   Topics    │  │   Schema    │  │ Governance  │   │
│  │             │  │  Registry   │  │   Layer     │   │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘   │
│         │                │                │           │
└─────────┼────────────────┼────────────────┼───────────┘
          └────────────────┴────────────────┘
                           │
                           ▼
                    FLINK PROCESSING
                           │
                           ▼
                  ┌────────────────┐
                  │ Risk Detection │
                  └────────┬───────┘
                           │
                           ▼
                  ┌────────────────┐
                  │     Agent      │
                  └────────┬───────┘
                           │
                           ▼
                     DASHBOARD
```

## Summary

**Current Setup:**
- ✅ Custom Python connectors (production-ready approach)
- ✅ Realistic mock data generation  
- ✅ Multiple simulated data sources
- ✅ Demonstrates connector architecture

**For Demo:**
- Position Python producers as "custom application connectors"
- Explain they simulate real IoT sensors and enterprise systems
- Highlight extensibility to add official Confluent Connectors
- Show data flowing in Confluent Cloud UI

**Connector Types Demonstrated:**
1. IoT/Sensor connector (worker telemetry)
2. Enterprise system connector (task management - optional)
3. Environmental monitoring connector (optional)

This architecture is **production-realistic** and demonstrates proper connector patterns! 🚀
