# 🔌 MQTT Setup Guide - EMQX to Confluent

## Architecture

```
Python Producer → EMQX MQTT Broker → MQTT Source Connector → Kafka Topic
   (paho-mqtt)    (cloud/self-hosted)   (Confluent)        (ergo.worker.motion)
```

## Prerequisites

- EMQX MQTT broker (cloud or self-hosted)
- Confluent Cloud cluster with MQTT Source Connector
- Topics: `ergo.worker.motion`, `ergo.risk.detected`, `ergo.interventions`
- Connector configured to listen to: `workers/+/telemetry`

## 🚀 Quick Start

### 1. Install paho-mqtt

```bash
pip install paho-mqtt
```

### 2. Test MQTT Connection

```bash
python scripts/test_mqtt.py
```

**Expected output:**
```
✅ Successfully connected to EMQX!
✅ Message published successfully
```

### 3. Check Confluent Cloud

1. Go to **Topics** → **ergo.worker.motion**
2. Click **Messages** tab
3. Wait 10-30 seconds
4. You should see the test message appear!

### 4. Run Full MQTT Producer

```bash
python -m src.producers.mqtt_telemetry
```

**Expected output:**
```
ErgoStream - MQTT Worker Telemetry Producer
✅ Connected to EMQX MQTT broker successfully!
Starting MQTT telemetry stream (interval: 60.00s)
Batch published: 10 events | Total: 10
Published 10 events to MQTT
```

---

## 📊 Topic Mapping

| MQTT Topic | Kafka Topic |
|------------|-------------|
| `workers/W1000/telemetry` | `ergo.worker.motion` |
| `workers/W1001/telemetry` | `ergo.worker.motion` |
| `workers/W1042/telemetry` | `ergo.worker.motion` |
| `workers/{any_worker}/telemetry` | `ergo.worker.motion` |

The `+` wildcard in `workers/+/telemetry` captures all worker IDs!

---

## 🔍 Verification Checklist

### ✅ Step 1: MQTT Connection Works
```bash
python scripts/test_mqtt.py
```
Should see: `✅ Successfully connected to EMQX!`

### ✅ Step 2: Message in Confluent
1. Confluent Cloud → Topics → ergo.worker.motion
2. Messages tab
3. Should see test message within 30 seconds

### ✅ Step 3: MQTT Connector Running
1. Confluent Cloud → Connectors
2. Find "MqttSourceConnector_0"
3. Status: **Running** (green)

### ✅ Step 4: Producer Sending Data
```bash
python -m src.producers.mqtt_telemetry
```
Should see messages incrementing

### ✅ Step 5: Data Flowing to Kafka
Check topic metrics:
- Messages/sec > 0
- Throughput graph showing activity

---

## 🎯 Data Flow Example

### 1. Producer Generates Event
```python
{
  "worker_id": "W1042",
  "back_angle": 45.3,
  "load_kg": 18.5,
  "station_id": "B-17",
  ...
}
```

### 2. Published to MQTT
```
Topic: workers/W1042/telemetry
Payload: {"worker_id": "W1042", ...}
```

### 3. MQTT Connector Reads
- Subscribes to: `workers/+/telemetry`
- Captures all worker topics

### 4. Writes to Kafka
```
Topic: ergo.worker.motion
Key: W1042
Value: {"worker_id": "W1042", ...}
```

### 5. Flink Processes
- Reads from `ergo.worker.motion`
- Detects HIGH risk
- Writes to `ergo.risk.detected`

---

## 🐛 Troubleshooting

### "Connection refused - bad username or password"

**Fix:** Check your MQTT credentials in `.env` file:
```bash
MQTT_USERNAME=your-mqtt-username
MQTT_PASSWORD=your-mqtt-password
```

### "No messages in Kafka topic"

**Check:**
1. MQTT connector status (should be Running)
2. Producer is actually running
3. Wait 30-60 seconds (connector polls periodically)
4. Check EMQX dashboard for incoming messages

### "SSL/TLS errors"

**Fix:** Ensure using correct protocol:
```python
client.tls_set(cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS)
```

### "Messages not appearing in Confluent"

**Debug steps:**
1. Check EMQX dashboard - are messages arriving?
2. Check MQTT connector logs in Confluent
3. Verify topic name matches: `ergo.worker.motion`
4. Check connector topic pattern: `workers/+/telemetry`

---

## 📈 Monitoring

### In EMQX Dashboard
- Go to your EMQX dashboard URL
- Login with your credentials
- Check **Clients** - should see `ergostream-mqtt-producer`
- Check **Topics** - should see `workers/*/telemetry`
- Monitor message rates

### In Confluent Cloud
- **Topics** → **ergo.worker.motion** → **Metrics**
- Check throughput graph
- Monitor consumer lag
- Check partition distribution

---

## 🔄 Switching Between Producers

### Use MQTT Producer
```bash
python -m src.producers.mqtt_telemetry
```

### Use Direct Kafka Producer (original)
```bash
python -m src.producers.worker_telemetry
```

**Both work!** MQTT adds an extra hop but demonstrates IoT integration.

---

## ⚡ Performance

**Expected rates:**
- 10 workers
- 1 event per worker per minute
- = 10 events/minute = 0.16 events/second

**MQTT adds ~1-2 seconds latency** compared to direct Kafka producer.

---

## 🎪 For Demo

**What to say:**
> "Worker telemetry streams from simulated wearable devices via MQTT protocol - the industry standard for IoT. EMQX acts as our IoT gateway, and Confluent's MQTT Source connector ingests these events into Kafka topics in real-time. This architecture scales to thousands of devices and mirrors production IoT deployments."

**Show:**
1. EMQX dashboard with live connections
2. MQTT producer sending events
3. Confluent Cloud receiving data
4. Flink processing in real-time
5. Dashboard showing results

---

## 🎯 Next Steps

1. ✅ Test MQTT connection
2. ✅ Verify data in Confluent
3. **Deploy Flink SQL** (next step!)
4. Run agent
5. Start dashboard

See [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md) for complete flow!

---

## 📝 Configuration Summary

**MQTT Broker:**
- Host: Your MQTT broker hostname (configured in `.env`)
- Port: `8883` (MQTT over TLS)
- Protocol: `ssl://`

**MQTT Connector:**
- Source: MQTT topics `workers/+/telemetry`
- Destination: Kafka topic `ergo.worker.motion`
- QoS: 0 (at most once)

**Producer:**
- Client ID: `ergostream-mqtt-producer`
- Publishes to: `workers/{worker_id}/telemetry`
- Format: JSON
- Rate: 10 events/minute

---

**Ready to start streaming!** 🚀
