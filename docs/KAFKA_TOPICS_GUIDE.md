# 📊 Kafka Topics & Partitions Guide

## Quick Answer

### Essential Topics (MUST CREATE)

For the hackathon, you need **3 core topics**:

```
✅ ergo.worker.motion       (Producer writes)
✅ ergo.risk.detected       (Flink writes)
✅ ergo.interventions       (Agent writes)
```

### Recommended Partition Count

**For Hackathon (10 workers):** **3 partitions**  
**For Production (100+ workers):** **6-12 partitions**

---

## 📋 Complete Topic Breakdown

### CORE TOPICS (Required)

#### 1. `ergo.worker.motion` ⭐ PRIMARY

**Purpose:** Raw worker telemetry from sensors  
**Who writes:** Python Producer  
**Who reads:** Flink SQL  
**Volume:** 10 events/minute (1 per worker)

```yaml
Partitions: 3-6
Retention: 1 day (86400000 ms)
Compression: lz4
Key: worker_id
```

**Why this topic:**
- Main data ingestion point
- Flink reads from here
- Partitioned by worker_id for parallel processing

---

#### 2. `ergo.risk.detected` ⭐ CRITICAL

**Purpose:** High-risk events detected by Flink  
**Who writes:** Flink SQL (INSERT INTO)  
**Who reads:** Agent, Dashboard  
**Volume:** 1-5 events/minute (only HIGH risks)

```yaml
Partitions: 3
Retention: 30 days (2592000000 ms)
Compression: lz4
Key: worker_id
```

**Why this topic:**
- Output of Flink risk detection
- Agent consumes from here
- Lower volume (only alerts)
- Longer retention for compliance

---

#### 3. `ergo.interventions` ⭐ IMPORTANT

**Purpose:** AI-generated intervention recommendations  
**Who writes:** Rule-based Agent  
**Who reads:** Dashboard  
**Volume:** 1-5 events/minute (matches risk events)

```yaml
Partitions: 3
Retention: 30 days (2592000000 ms)
Compression: lz4
Key: worker_id
```

**Why this topic:**
- Completes the feedback loop
- Dashboard displays interventions
- Audit trail for safety actions

---

### OPTIONAL TOPICS

#### 4. `ergo.analytics.station` (Optional)

**Purpose:** Aggregated risk by station  
**Who writes:** Flink SQL (optional query)  
**Who reads:** Dashboard  
**Volume:** Low (1 event/station/minute)

```yaml
Partitions: 3
Retention: 7 days
Cleanup: compact (keeps latest per station)
```

**When to use:** Want station-level dashboards

---

#### 5. `ergo.worker.task` (Not currently used)

**Purpose:** Task assignments from WMS  
**Who writes:** Task Management Connector (future)  
**Who reads:** Flink (for context enrichment)  

**Skip for hackathon** - not implemented yet

---

#### 6. `ergo.worker.posture` (Not currently used)

**Purpose:** Aggregated posture analysis  
**Skip for hackathon** - not implemented yet

---

## 🎯 Partition Count Decision Guide

### Understanding Partitions

```
Topic: ergo.worker.motion
├─ Partition 0: W1000, W1003, W1006, W1009
├─ Partition 1: W1001, W1004, W1007
└─ Partition 2: W1002, W1005, W1008

(Workers hashed by worker_id to partitions)
```

**Key concept:** More partitions = more parallelism

---

### For Hackathon (10 Workers)

**Recommended: 3 partitions**

**Why:**
- ✅ Good balance for demo
- ✅ Shows partitioning concept
- ✅ Enough parallelism for 10 workers
- ✅ Not overkill for small demo
- ✅ Fits Confluent Cloud free tier nicely

**Math:**
```
10 workers / 3 partitions = ~3-4 workers per partition
Each partition handles manageable load
```

---

### For Production (100+ Workers)

**Recommended: 6-12 partitions**

**Why:**
- ✅ Better throughput distribution
- ✅ More consumer parallelism
- ✅ Room to scale
- ✅ Handles burst traffic

**Math:**
```
100 workers / 6 partitions = ~16-17 workers per partition
100 workers / 12 partitions = ~8-9 workers per partition
```

---

### Partition Count Rules of Thumb

```python
# General formula
partitions = max(
    num_producers,      # Producer parallelism
    num_consumers,      # Consumer parallelism
    target_throughput / partition_throughput
)

# For ErgoStream hackathon:
partitions = max(
    1,                  # 1 producer instance
    1,                  # 1 consumer (agent)
    10 workers / 3      # ~3 workers per partition
) = 3 partitions
```

---

## 📊 Topic Size & Retention

### Storage Calculator

**For `ergo.worker.motion`:**

```
Event size: ~300 bytes (JSON)
Events per minute: 10 (1 per worker)
Events per day: 14,400
Data per day: ~4.3 MB
Retention: 1 day
Total storage: ~4.3 MB per topic

With 3 partitions: ~1.4 MB per partition
```

**Verdict:** Very small! Storage is not a concern for hackathon.

---

### Retention Guidelines

| Topic | Retention | Why |
|-------|-----------|-----|
| **worker.motion** | 1 day | Raw data, high volume, Flink processes immediately |
| **risk.detected** | 30 days | Compliance, audit trail, low volume |
| **interventions** | 30 days | Safety records, regulatory requirement |
| **analytics.station** | 7 days | Operational dashboards |

---

## 🚀 Creating Topics

### Option 1: Use Our Script (Recommended)

```bash
python scripts/create_topics.py
```

This creates all 7 topics with recommended settings.

---

### Option 2: Create Only Essential Topics

**Manually in Confluent Cloud:**

1. **ergo.worker.motion**
   - Partitions: 3
   - Retention: 1 day
   - Cleanup: delete

2. **ergo.risk.detected**
   - Partitions: 3
   - Retention: 30 days
   - Cleanup: delete

3. **ergo.interventions**
   - Partitions: 3
   - Retention: 30 days
   - Cleanup: delete

**Done!** That's all you need for the hackathon.

---

### Option 3: Using Confluent CLI

```bash
# Worker motion
confluent kafka topic create ergo.worker.motion \
  --partitions 3 \
  --config retention.ms=86400000 \
  --config compression.type=lz4

# Risk detected
confluent kafka topic create ergo.risk.detected \
  --partitions 3 \
  --config retention.ms=2592000000

# Interventions
confluent kafka topic create ergo.interventions \
  --partitions 3 \
  --config retention.ms=2592000000
```

---

## 🔧 Modifying Partition Count

### If You Want Different Partition Count

**Edit `config/topics.yaml`:**

```yaml
# For hackathon (small scale)
- name: ergo.worker.motion
  partitions: 3  # ← Change this

# For production demo (impressive scale)
- name: ergo.worker.motion
  partitions: 12  # ← More impressive!
```

Then run:
```bash
python scripts/create_topics.py
```

**Note:** You can only INCREASE partitions, never decrease.

---

### Quick Partition Recommendations

| Scenario | Partitions | Why |
|----------|------------|-----|
| **Hackathon demo (10 workers)** | 3 | Simple, efficient, works great |
| **Impressive demo (50 workers)** | 6 | Shows scalability |
| **Production simulation (100+ workers)** | 12 | Real-world scale |
| **Enterprise scale (1000+ workers)** | 24+ | Maximum throughput |

---

## 🎪 For Your Demo Presentation

### What to Say About Topics

**Script:**
> "ErgoStream uses three core topics: worker motion telemetry streams to the primary topic, Flink processes this in real-time and publishes high-risk alerts to the risk detection topic, and our intervention agent generates recommendations to the interventions topic. Each topic is partitioned by worker ID, enabling parallel processing and horizontal scaling."

### What to Say About Partitions

**Script:**
> "We're using 3 partitions for optimal load distribution. Each partition handles roughly 3-4 workers, enabling Flink to process telemetry in parallel. This architecture scales horizontally - we could easily handle 100 or 1000 workers by increasing partition count and consumer instances."

---

## ⚠️ Common Questions

### Q: Why not 1 partition?

**A:** Less parallelism, single point of processing. Works but not impressive.

### Q: Why not 50 partitions?

**A:** Overkill for 10 workers. More partitions = more overhead. Each partition has some cost.

### Q: Can I change partitions later?

**A:** Yes, but only INCREASE. Can't decrease. Start conservative (3), increase if needed.

### Q: What about replication factor?

**A:** Confluent Cloud handles this automatically. Default is 3 (highly available).

### Q: Do I need all 7 topics?

**A:** No! For hackathon, just create the 3 core topics:
- `ergo.worker.motion`
- `ergo.risk.detected`
- `ergo.interventions`

---

## ✅ Final Recommendation for Hackathon

### Create These 3 Topics

```bash
# Run this
python scripts/create_topics.py
```

Or manually create:

1. **ergo.worker.motion** - 3 partitions, 1 day retention
2. **ergo.risk.detected** - 3 partitions, 30 days retention
3. **ergo.interventions** - 3 partitions, 30 days retention

**That's it!** Skip the optional topics unless you have extra time.

---

## 📈 Monitoring Your Topics

### In Confluent Cloud UI

1. **Topics → ergo.worker.motion**
2. **Check:**
   - Throughput graph
   - Message count
   - Partition distribution
   - Consumer lag

### Expected Metrics

```
ergo.worker.motion:
├─ Messages/sec: 0.16 (10 per minute)
├─ MB/sec: 0.00048 (~300 bytes * 10/60)
├─ Consumer lag: < 5 seconds
└─ Partitions: 3 balanced

ergo.risk.detected:
├─ Messages/sec: 0.01-0.08 (1-5 per minute)
├─ Much lower volume
└─ Only HIGH risk events
```

---

## 🎯 Summary

**Essential Topics:** 3 (motion, risk, interventions)  
**Recommended Partitions:** 3 for hackathon, 6-12 for production  
**Total Storage:** < 5 MB/day  
**Setup Time:** 5 minutes with script  

**Quick command:**
```bash
python scripts/create_topics.py
```

**Verify:**
```bash
# Confluent Cloud UI → Topics
# Should see all topics listed
```

**Done!** ✅
