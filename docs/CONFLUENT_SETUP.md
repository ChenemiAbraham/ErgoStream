# Confluent Cloud Setup Guide

Complete setup guide for deploying ErgoStream on Confluent Cloud.

## Prerequisites

- Confluent Cloud account ([Sign up free](https://www.confluent.io/confluent-cloud/tryfree/))
- Python 3.9+
- Git

## Step 1: Create Confluent Cloud Environment

### 1.1 Create Cluster

1. Log into [Confluent Cloud](https://confluent.cloud)
2. Click **"Add cluster"**
3. Choose cluster type:
   - **Basic** - Good for development/testing (free trial available)
   - **Standard** - Recommended for demo
   - **Dedicated** - Production-grade (if you have budget)
4. Select cloud provider and region (choose closest to you)
5. Name your cluster: `ergostream-cluster`
6. Click **"Launch cluster"**

### 1.2 Enable Flink

1. In your cluster, navigate to **Stream Processing**
2. Click **"Enable Flink"**
3. Choose compute pool size:
   - **5 CFUs** minimum for demo
   - **10 CFUs** recommended for smooth operation
4. Wait for Flink to provision (~2-3 minutes)

### 1.3 Create API Keys

#### Kafka API Key

1. Go to **Cluster Settings** → **API Keys**
2. Click **"Create key"**
3. Select **"Global access"**
4. Save the **API Key** and **API Secret** (you won't see secret again!)
5. Add to your `.env`:
   ```bash
   CONFLUENT_API_KEY=<your-kafka-api-key>
   CONFLUENT_API_SECRET=<your-kafka-api-secret>
   ```

#### Schema Registry API Key

1. Go to **Schema Registry** tab
2. Click **"API credentials"**
3. Create new credential
4. Save the key and secret
5. Add to your `.env`:
   ```bash
   CONFLUENT_SCHEMA_REGISTRY_API_KEY=<your-sr-key>
   CONFLUENT_SCHEMA_REGISTRY_API_SECRET=<your-sr-secret>
   ```

### 1.4 Get Connection Details

1. Copy **Bootstrap server** from cluster overview
   - Format: `pkc-xxxxx.region.provider.confluent.cloud:9092`
2. Copy **Schema Registry URL**
   - Format: `https://psrc-xxxxx.region.provider.confluent.cloud`
3. Add to `.env`:
   ```bash
   CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092
   CONFLUENT_SCHEMA_REGISTRY_URL=https://psrc-xxxxx.region.provider.confluent.cloud
   ```

## Step 2: Create Topics

### Option A: Using Python Script (Recommended)

```bash
# Install dependencies
pip install -r requirements.txt

# Create topics
python scripts/create_topics.py
```

### Option B: Using Confluent Cloud UI

1. Navigate to **Topics** in your cluster
2. Click **"Create topic"**
3. For each topic in [`config/topics.yaml`](../config/topics.yaml):
   - Enter topic name
   - Set partitions (default: 6)
   - Click **"Create with defaults"**

Required topics:
- `ergo.worker.motion`
- `ergo.worker.posture`
- `ergo.worker.task`
- `ergo.risk.detected`
- `ergo.interventions`
- `ergo.analytics.station`

## Step 3: Deploy Flink SQL

### 3.1 Open Flink SQL Workspace

1. Navigate to **Stream Processing** → **Flink SQL**
2. Click **"Open workspace"**

### 3.2 Set Environment Variables

In the SQL workspace, create environment variables:

```sql
-- Set your bootstrap servers
SET 'BOOTSTRAP_SERVERS' = 'pkc-xxxxx.region.provider.confluent.cloud:9092';
```

### 3.3 Run Risk Detection SQL

1. Open [`src/flink/risk_detection.sql`](../src/flink/risk_detection.sql)
2. Copy each statement one at a time:
   - First: Create `worker_motion` table
   - Second: Create `risk_detected` table
   - Third: Run the INSERT INTO query
3. Verify queries are running in **"Running Statements"** tab

### 3.4 Verify Flink Processing

Check that Flink is processing:
```sql
SELECT COUNT(*) FROM worker_motion;
SELECT * FROM risk_detected ORDER BY timestamp DESC LIMIT 10;
```

## Step 4: Configure Application

### 4.1 Complete .env File

```bash
# Confluent Cloud (ONLY THESE ARE REQUIRED)
CONFLUENT_BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092
CONFLUENT_API_KEY=your-kafka-api-key
CONFLUENT_API_SECRET=your-kafka-api-secret
CONFLUENT_SCHEMA_REGISTRY_URL=https://psrc-xxxxx.region.provider.confluent.cloud
CONFLUENT_SCHEMA_REGISTRY_API_KEY=your-sr-key
CONFLUENT_SCHEMA_REGISTRY_API_SECRET=your-sr-secret

# Application
ENVIRONMENT=production
LOG_LEVEL=INFO
NUM_WORKERS=10
SIMULATION_SPEED=1.0

# No AI API keys needed - using rule-based agent!
```

### 4.2 Test Connection

```bash
python -c "from src.utils import settings; print('✅ Configuration loaded successfully')"
```

## Step 5: Run the Application

### Terminal 1: Start Data Generator

```bash
python -m src.producers.worker_telemetry
```

You should see:
```
Starting telemetry stream (interval: 60.00s)
Delivered message to ergo.worker.motion [0] @ offset 0 | Total: 10
```

### Terminal 2: Start AI Agent

```bash
python -m src.agents.ergo_ops_agent
```

Wait for:
```
ErgoOps AI Agent - Starting
Monitoring risk events and generating interventions...
```

### Terminal 3: Start Dashboard

```bash
streamlit run src/dashboard/app.py
```

## Step 6: Verify Data Flow

### In Confluent Cloud UI

1. **Topics** → `ergo.worker.motion`
   - Should see messages flowing
   - Check throughput graph

2. **Topics** → `ergo.risk.detected`
   - Should see risk events when workers enter high-risk states
   - May take 5-10 minutes for first detection

3. **Stream Processing** → **Flink SQL**
   - Check "Running Statements" shows active queries
   - Query `risk_detected` table to see results

### In Your Terminal

Watch for:
```
🚨 RISK DETECTED: W1042 - HIGH
💡 INTERVENTION GENERATED
Action: Rotate Worker W1042 from pallet picking immediately
```

## Step 7: Enable Stream Governance (Optional)

### 7.1 Tag Sensitive Topics

1. Navigate to **Stream Catalog**
2. Find `ergo.worker.motion`
3. Add tags:
   - `PII`
   - `Sensitive-Worker-Data`
   - `Requires-Access-Review`

### 7.2 Set Data Quality Rules

1. Go to **Stream Quality**
2. Create rule for `ergo.worker.motion`:
   - Field: `back_angle`
   - Constraint: `>= 0 AND <= 180`
   - Action: Alert on violation

### 7.3 View Stream Lineage

1. Go to **Stream Lineage**
2. Select `ergo.risk.detected` topic
3. See upstream/downstream dependencies:
   ```
   worker_motion → Flink (risk_detection) → risk_detected → AI Agent → interventions
   ```

## Troubleshooting

### "Authentication failed"
- Verify API keys are correct
- Check if keys have expired
- Ensure no extra spaces in `.env`

### "Topic not found"
- Run `python scripts/create_topics.py`
- Verify topics exist in UI

### "No messages in topic"
- Check producer is running
- Verify topic name matches configuration
- Check Confluent Cloud metrics

### "Flink query not running"
- Ensure compute pool is active
- Check for SQL syntax errors
- Verify environment variables are set

### "Agent not generating interventions"
- Verify risk events are being produced (check `ergo.risk.detected` topic)
- Risk must be HIGH or CRITICAL to trigger intervention
- Check agent terminal for errors

## Cost Optimization

### Free Tier Limits
- 30 days free trial with $400 credits
- Basic cluster free up to certain limits

### Tips for Demo
1. Use **Basic** cluster tier
2. Minimize compute pool size (5 CFUs)
3. Set short retention periods (1-7 days)
4. Delete cluster after demo

### Cost Monitoring
- Check **Billing & payment** in Confluent Cloud
- Set up usage alerts
- Stop Flink compute pools when not in use

## Next Steps

- [Build the Dashboard](./DASHBOARD.md)
- [Customize Risk Scoring](./CUSTOMIZATION.md)
- [Deploy to Production](./PRODUCTION.md)
- [Submit to Confluent Competition](./SUBMISSION.md)

## Support

- Confluent Documentation: https://docs.confluent.io
- Confluent Community Forum: https://forum.confluent.io
- ErgoStream Issues: Check your project README
