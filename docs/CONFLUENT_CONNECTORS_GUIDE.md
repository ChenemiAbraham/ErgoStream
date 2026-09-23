# 🔌 Official Confluent Connectors for ErgoStream

## Recommended Connectors for This Use Case

For a real-world ErgoStream deployment, here are the **best official Confluent Connectors**:

---

## 📊 SOURCE CONNECTORS (Getting Data IN)

### ⭐ #1 BEST: MQTT Source Connector (Most Realistic!)

**Why:** Real IoT sensors use MQTT protocol
**Perfect for:** Worker wearable devices, industrial sensors

```yaml
Connector: MQTT Source
Real-world use: Industrial IoT sensors
Data: Worker telemetry from wearables
Topic: ergo.worker.motion
Setup time: 15 minutes
```

#### How IoT Sensors Work in Reality

```
[Worker Wearable Device]
    ↓ (MQTT Protocol)
[MQTT Broker - HiveMQ/Mosquitto]
    ↓
[Confluent MQTT Source Connector]
    ↓
[Kafka Topic: ergo.worker.motion]
```

#### Setup Steps

1. **Run local MQTT broker:**
   ```bash
   docker run -p 1883:1883 eclipse-mosquitto
   ```

2. **Add MQTT Source Connector in Confluent Cloud:**
   - Connectors → Add connector → "MQTT Source"
   - MQTT Broker: `your-mqtt-broker-url:1883`
   - Topics: `workers/+/telemetry` (wildcard for all workers)
   - Kafka Topic: `ergo.worker.motion`

3. **Publish test data:**
   ```python
   import paho.mqtt.client as mqtt
   import json
   
   client = mqtt.Client()
   client.connect("localhost", 1883)
   
   data = {
       "worker_id": "W1042",
       "back_angle": 45.3,
       "load_kg": 18.5,
       # ...
   }
   
   client.publish("workers/W1042/telemetry", json.dumps(data))
   ```

**Pros:**
- ✅ Most realistic for IoT
- ✅ Industry standard
- ✅ Scalable to thousands of devices
- ✅ Low latency

**Cons:**
- ⚠️ Needs MQTT broker running
- ⚠️ Extra setup time

---

### ⭐ #2 Datagen Source Connector (Easiest!)

**Why:** Built into Confluent Cloud, zero external setup
**Perfect for:** Quick demo without infrastructure

```yaml
Connector: Datagen Source
Real-world use: Testing/demo
Data: Generated worker motion events
Topic: ergo.worker.motion
Setup time: 5 minutes
```

#### Setup Steps

1. **Confluent Cloud → Connectors → "Datagen Source"**

2. **Choose template:** QuickStart or Custom

3. **For custom schema, use this:**

```json
{
  "type": "record",
  "name": "WorkerMotion",
  "fields": [
    {"name": "worker_id", "type": "string"},
    {"name": "timestamp", "type": "long", "arg.properties": {"iteration": {"start": 1}}},
    {"name": "back_angle", "type": "float", "arg.properties": {"range": {"min": 5.0, "max": 75.0}}},
    {"name": "neck_angle", "type": "float", "arg.properties": {"range": {"min": 5.0, "max": 60.0}}},
    {"name": "load_kg", "type": "float", "arg.properties": {"range": {"min": 2.0, "max": 30.0}}},
    {"name": "repetition_rate", "type": "float", "arg.properties": {"range": {"min": 3.0, "max": 22.0}}},
    {"name": "station_id", "type": "string", "arg.properties": {"options": ["A-01", "A-02", "B-17", "B-18"]}},
    {"name": "task_type", "type": "string", "arg.properties": {"options": ["pallet_picking", "assembly", "lifting"]}}
  ]
}
```

4. **Configure:**
   - Output topic: `ergo.worker.motion`
   - Output format: JSON
   - Tasks: 1
   - Max interval: 1000ms (1 event/second)

**Pros:**
- ✅ Zero infrastructure needed
- ✅ Built into Confluent Cloud
- ✅ Perfect for demo
- ✅ 5 minute setup

**Cons:**
- ⚠️ Less realistic than MQTT
- ⚠️ Simple random data (no behavioral patterns)

---

### ⭐ #3 HTTP Source Connector

**Why:** Many enterprise systems expose REST APIs
**Perfect for:** Warehouse Management System (WMS) data

```yaml
Connector: HTTP Source
Real-world use: WMS task assignments, ERP data
Data: Task assignments, schedules
Topic: ergo.worker.task
Setup time: 10 minutes
```

#### Setup Steps

1. **Create mock REST API endpoint:**

   **Option A: Use httpbin.org (quick test)**
   ```
   URL: https://httpbin.org/json
   ```

   **Option B: Create simple Flask API** (more realistic)

   ```python
   # api.py
   from flask import Flask, jsonify
   import random
   
   app = Flask(__name__)
   
   @app.route('/api/tasks')
   def get_tasks():
       return jsonify({
           "worker_id": f"W{random.randint(1000, 1050)}",
           "task_type": random.choice(["pallet_picking", "assembly"]),
           "station_id": f"B-{random.randint(1, 20):02d}",
           "timestamp": "2026-09-22T14:30:00Z"
       })
   
   if __name__ == '__main__':
       app.run(host='0.0.0.0', port=5000)
   ```

   ```bash
   # Run it
   python api.py
   
   # Expose with ngrok (for Confluent Cloud to reach)
   ngrok http 5000
   ```

2. **Add HTTP Source Connector:**
   - Confluent Cloud → Connectors → "HTTP Source"
   - URL: `https://your-ngrok-url.ngrok.io/api/tasks`
   - HTTP Method: GET
   - Request Interval: 60000ms (1 request/minute)
   - Kafka Topic: `ergo.worker.task`

**Pros:**
- ✅ Realistic for enterprise integration
- ✅ Flexible
- ✅ Can integrate any REST API

**Cons:**
- ⚠️ Need accessible HTTP endpoint

---

### #4 PostgreSQL CDC Source Connector

**Why:** Many companies store worker schedules in databases
**Perfect for:** Real-time sync of worker assignments

```yaml
Connector: PostgreSQL CDC (Debezium)
Real-world use: ERP database, HR systems
Data: Worker schedules, shift assignments
Topic: ergo.worker.schedule
Setup time: 20 minutes
```

**When to use:**
- You want to show database integration
- Demonstrating Change Data Capture (CDC)
- Real-time sync from operational database

---

## 📤 SINK CONNECTORS (Sending Data OUT)

### ⭐ #1 S3 Sink Connector (Compliance/Analytics)

**Why:** Archive risk events for regulatory compliance
**Perfect for:** Long-term storage, data lake integration

```yaml
Connector: S3 Sink
Real-world use: Compliance archival, analytics
Data: ergo.risk.detected → S3
Setup time: 10 minutes
```

#### Setup Steps

1. **Create S3 bucket:**
   ```bash
   aws s3 mb s3://ergostream-risk-archive
   ```

2. **Add S3 Sink Connector:**
   - Confluent Cloud → Connectors → "Amazon S3 Sink"
   - Input topic: `ergo.risk.detected`
   - S3 Bucket: `ergostream-risk-archive`
   - Output format: JSON
   - Time interval: 300000ms (flush every 5 min)

**Pros:**
- ✅ Shows complete data pipeline
- ✅ Demonstrates compliance use case
- ✅ Easy to set up

---

### ⭐ #2 Webhook/HTTP Sink Connector

**Why:** Send interventions to external systems
**Perfect for:** Alerting supervisors, ticketing systems

```yaml
Connector: HTTP Sink
Real-world use: Alert systems, ServiceNow, Slack
Data: ergo.interventions → Webhook
Setup time: 10 minutes
```

#### Setup Steps

1. **Create webhook receiver:**

   **Option: Use webhook.site (instant test)**
   - Go to https://webhook.site
   - Copy your unique URL

   **Or: Use Slack webhook**
   ```
   https://hooks.slack.com/services/YOUR/WEBHOOK/URL
   ```

2. **Add HTTP Sink Connector:**
   - Input topic: `ergo.interventions`
   - HTTP URL: Your webhook URL
   - HTTP Method: POST
   - Request body format: JSON

**Result:** Every intervention posted to Slack/webhook in real-time!

---

### #3 Elasticsearch Sink Connector

**Why:** Real-time dashboards and analytics
**Perfect for:** Kibana dashboards, search

```yaml
Connector: Elasticsearch Sink
Real-world use: Analytics, searchable archive
Data: All topics → Elasticsearch
Setup time: 15 minutes
```

---

## 🎯 RECOMMENDED ARCHITECTURE FOR HACKATHON

### Option A: Quick Demo (Fastest - 10 minutes)

```
[Datagen Source]
    ↓
[ergo.worker.motion]
    ↓
[Flink Processing]
    ↓
[ergo.risk.detected]
    ↓
[S3 Sink]
```

**Benefits:**
- ✅ Zero infrastructure
- ✅ 10 minute setup
- ✅ Shows source + sink

---

### Option B: Most Impressive (Realistic - 30 minutes)

```
[MQTT Source] ──────────→ [ergo.worker.motion]
                               ↓
[HTTP Source] ──────────→ [ergo.worker.task]
                               ↓
                        [Flink Processing]
                               ↓
                       [ergo.risk.detected]
                         ↙           ↘
              [S3 Sink]              [HTTP Sink]
             (Archive)              (Slack Alerts)
```

**Benefits:**
- ✅ Shows multiple data sources
- ✅ Demonstrates real IoT integration
- ✅ Complete end-to-end pipeline
- ✅ Most realistic architecture

---

### Option C: Hybrid (Current + Confluent - 20 minutes)

```
[Your Python Producer] ──→ [ergo.worker.motion]
                               ↓
[Datagen Source] ────────→ [ergo.worker.task]
                               ↓
                        [Flink Processing]
                               ↓
                       [ergo.risk.detected]
                               ↓
                          [S3 Sink]
```

**Benefits:**
- ✅ Keeps your existing code
- ✅ Adds official connectors
- ✅ Shows multiple sources
- ✅ Best of both worlds

---

## 🚀 Quick Start: Add Datagen Connector (5 min)

**Want to add an official connector right now?**

### Steps:

1. **Confluent Cloud → Connectors → "Add connector"**

2. **Search "Datagen"**

3. **Select "Datagen Source"**

4. **Configuration:**
   ```json
   Output topic: ergo.datagen.test
   Output format: JSON
   Template: Orders (or Custom with schema above)
   Max interval: 1000ms
   Tasks: 1
   ```

5. **Launch!**

6. **Verify:**
   ```bash
   # Topics → ergo.datagen.test → Messages
   # Should see data flowing immediately
   ```

7. **Update Flink SQL to read from both:**
   ```sql
   -- Original table
   CREATE TABLE worker_motion (...) WITH (
       'topic' = 'ergo.worker.motion'
   );
   
   -- New table from Datagen
   CREATE TABLE worker_motion_datagen (...) WITH (
       'topic' = 'ergo.datagen.test'
   );
   
   -- Union them
   INSERT INTO risk_detected
   SELECT * FROM worker_motion
   UNION ALL
   SELECT * FROM worker_motion_datagen;
   ```

**Result:** Now showing data from **TWO sources** - custom Python + Datagen!

---

## 📊 Connector Comparison Matrix

| Connector | Setup Time | Realism | Demo Impact | Infrastructure | Cost |
|-----------|------------|---------|-------------|----------------|------|
| **Datagen Source** | 5 min | ⭐⭐ | ⭐⭐⭐⭐ | None | Free |
| **MQTT Source** | 15 min | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | MQTT broker | Low |
| **HTTP Source** | 10 min | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | REST API | Low |
| **PostgreSQL CDC** | 20 min | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Database | Medium |
| **S3 Sink** | 10 min | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | S3 bucket | Low |
| **HTTP Sink** | 10 min | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Webhook | Free |
| **Custom Python** | 0 min | ⭐⭐⭐⭐ | ⭐⭐⭐ | None | Free |

---

## 🎪 For Your Demo Presentation

### If Using Datagen

**Script:**
> "We're using Confluent's Datagen connector to simulate worker telemetry data. In production, this would be replaced with MQTT Source connectors reading from real IoT sensors on workers. The beauty of Confluent is that changing data sources is just connector configuration - the downstream Flink processing and interventions work identically."

### If Using MQTT

**Script:**
> "Data flows from simulated worker wearables via MQTT protocol - the industry standard for IoT - through Confluent's MQTT Source connector into Kafka topics. This is exactly how production ergonomic monitoring systems like Kinetic or StrongArm integrate with enterprise platforms."

### If Using Multiple Connectors

**Script:**
> "ErgoStream demonstrates a multi-source architecture: worker telemetry flows via MQTT connectors, task assignments come from our warehouse management system via HTTP Source connector, and interventions are delivered to supervisors via HTTP Sink to Slack. This shows Confluent's power as a universal data fabric."

---

## ✅ My Recommendation for Your Hackathon

### If Time is Tight (< 1 hour to demo)
**Stick with your Python producer** - it's working, realistic, and production-grade

### If You Have Extra Time (> 1 hour)
**Add Datagen + S3 Sink:**
1. Keep your Python producer
2. Add Datagen Source (5 min)
3. Add S3 Sink (10 min)
4. Show "multi-source architecture"

**Total:** +15 minutes, big demo impact

### If You Want Max Points
**Go full MQTT:**
1. Run MQTT broker (Docker - 2 min)
2. Add MQTT Source connector (10 min)
3. Modify your Python producer to publish to MQTT instead of Kafka (5 min)
4. Add S3 Sink (10 min)

**Total:** +27 minutes, maximum realism

---

## 🛠️ Implementation Files

Want to actually implement this? I can help you:

1. **Create MQTT publisher** (replaces current producer)
2. **Create REST API** for HTTP Source connector
3. **Configure Datagen schema** for your data model
4. **Set up S3 Sink** for archival

Let me know which connector(s) you want to add! 🚀

---

## 📝 Quick Reference

**Fastest to add:** Datagen Source (5 min)  
**Most realistic:** MQTT Source (15 min)  
**Best for demo impact:** MQTT + HTTP Source + S3 Sink (30 min)  
**My recommendation:** Keep Python producer + add S3 Sink (10 min)
