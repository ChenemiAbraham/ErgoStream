-- ErgoStream Risk Detection
-- Flink SQL for Confluent Cloud (Corrected Syntax)
-- NOTE: In Confluent Cloud Flink, table names with dots must be backtick-quoted
-- The table name becomes the topic name automatically

-- ===========================================================================
-- STEP 1: Create tables for input topics
-- ===========================================================================

-- STEP 1: Read raw strings from ergo.worker.motion topic
-- Table name MUST match the actual Kafka topic name
-- Column MUST be named 'val' when using raw format
CREATE TABLE `ergo.worker.motion` (
    val STRING
) WITH (
    'connector' = 'confluent',
    'value.format' = 'raw',
    'scan.startup.mode' = 'latest-offset'
);

-- ===========================================================================
-- STEP 2: Create output table for risk events
-- ===========================================================================

-- STEP 2: Risk detected events output table
CREATE TABLE `ergo.risk.detected` (
    worker_id STRING,
    detection_time TIMESTAMP(3),
    risk_level STRING,
    risk_score DOUBLE,
    confidence DOUBLE,
    primary_risk STRING,
    station_id STRING,
    task_type STRING,
    exposure_minutes DOUBLE,
    current_back_angle DOUBLE,
    current_load_kg DOUBLE,
    current_repetition_rate DOUBLE
) WITH (
    'connector' = 'confluent'
);

-- ===========================================================================
-- STEP 3: Risk Detection Logic (Real-Time Processing)
-- ===========================================================================

-- STEP 3: Parse JSON and detect risks in real-time
INSERT INTO `ergo.risk.detected`
SELECT
    JSON_VALUE(val, '$.worker_id') AS worker_id,
    CURRENT_TIMESTAMP AS detection_time,

    -- Risk level classification
    CASE
        WHEN (CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) > 60 AND CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) > 20)
          OR (CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) > 70)
          OR (CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) > 25) THEN 'CRITICAL'
        WHEN (CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) > 45 AND CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) > 15)
          OR (CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) > 60)
          OR (CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) > 20)
          OR (CAST(JSON_VALUE(val, '$.repetition_rate') AS DOUBLE) > 15) THEN 'HIGH'
        WHEN (CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) > 30 AND CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) > 12)
          OR (CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) > 45)
          OR (CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) > 15)
          OR (CAST(JSON_VALUE(val, '$.repetition_rate') AS DOUBLE) > 10) THEN 'MODERATE'
        ELSE 'LOW'
    END AS risk_level,

    -- Simplified risk score (0-100)
    LEAST(
        CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) * 0.8 +
        CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) * 2.0 +
        CAST(JSON_VALUE(val, '$.repetition_rate') AS DOUBLE) * 1.5,
        100.0
    ) AS risk_score,

    -- Confidence
    CASE
        WHEN CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) > 60
          OR CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) > 20
          OR CAST(JSON_VALUE(val, '$.repetition_rate') AS DOUBLE) > 15 THEN 0.95
        WHEN CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) > 45
          OR CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) > 15 THEN 0.85
        ELSE 0.75
    END AS confidence,

    -- Primary risk factor
    CASE
        WHEN CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) >= 60 THEN 'Lower back strain'
        WHEN CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) >= 20 THEN 'Excessive load handling'
        WHEN CAST(JSON_VALUE(val, '$.repetition_rate') AS DOUBLE) >= 15 THEN 'Repetitive strain injury'
        WHEN CAST(JSON_VALUE(val, '$.duration_minutes') AS DOUBLE) >= 30 THEN 'Prolonged exposure'
        ELSE 'Moderate ergonomic stress'
    END AS primary_risk,

    JSON_VALUE(val, '$.station_id') AS station_id,
    JSON_VALUE(val, '$.task_type') AS task_type,
    CAST(JSON_VALUE(val, '$.duration_minutes') AS DOUBLE) AS exposure_minutes,
    CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) AS current_back_angle,
    CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) AS current_load_kg,
    CAST(JSON_VALUE(val, '$.repetition_rate') AS DOUBLE) AS current_repetition_rate

FROM `ergo.worker.motion`
WHERE
    -- Only process HIGH and CRITICAL risks
    CAST(JSON_VALUE(val, '$.back_angle') AS DOUBLE) > 45
    OR CAST(JSON_VALUE(val, '$.load_kg') AS DOUBLE) > 15
    OR CAST(JSON_VALUE(val, '$.repetition_rate') AS DOUBLE) > 12;

-- ===========================================================================
-- OPTIONAL: Station-level analytics
-- ===========================================================================

/*
-- Uncomment if you want station aggregations

CREATE TABLE `ergo.analytics.station` (
    station_id STRING,
    window_end TIMESTAMP(3),
    total_workers BIGINT,
    high_risk_count BIGINT,
    avg_risk_score DOUBLE,
    PRIMARY KEY (station_id) NOT ENFORCED
) WITH (
    'connector' = 'confluent',
    'key.format' = 'raw',
    'value.format' = 'json-registry'
);

-- Aggregate by station (5-minute tumbling window)
INSERT INTO `ergo.analytics.station`
SELECT
    station_id,
    TUMBLE_END(`timestamp`, INTERVAL '5' MINUTE) AS window_end,
    COUNT(DISTINCT worker_id) AS total_workers,
    SUM(CASE WHEN risk_level IN ('HIGH', 'CRITICAL') THEN 1 ELSE 0 END) AS high_risk_count,
    AVG(risk_score) AS avg_risk_score
FROM `ergo.risk.detected`
GROUP BY
    station_id,
    TUMBLE(`timestamp`, INTERVAL '5' MINUTE);
*/

-- ===========================================================================
-- VERIFICATION QUERIES (Run these to test after 5-10 minutes)
-- ===========================================================================

-- Check if data is flowing
-- SELECT COUNT(*) FROM `ergo.worker.motion`;

-- View recent risk events
-- SELECT * FROM `ergo.risk.detected` ORDER BY timestamp DESC LIMIT 10;

-- Count by risk level
-- SELECT risk_level, COUNT(*) as count FROM `ergo.risk.detected` GROUP BY risk_level;

-- View highest risks
-- SELECT worker_id, station_id, risk_level, risk_score, primary_risk
-- FROM `ergo.risk.detected`
-- WHERE risk_level IN ('HIGH', 'CRITICAL')
-- ORDER BY risk_score DESC
-- LIMIT 10;

-- Check specific worker
-- SELECT * FROM `ergo.risk.detected` WHERE worker_id = 'W1042' ORDER BY timestamp DESC LIMIT 5;
