/*
Local Model (HuggingFace) Sentiment Analysis Pipeline
===============================================

This pipeline performs real-time sentiment analysis of customer messages using
the local model tabularisai/multilingual-sentiment-analysis. 
It mirrors the OpenAI pipeline but runs locally with an encoder-only transformer model.
*/

SET 'job.name' = 'Local Model Sentiment Analysis Pipeline';

-- ============================================================================
-- 1. Register Python UDF for Local Sentiment Analysis
-- ============================================================================

CREATE TEMPORARY FUNCTION analyze_message_sentiment AS 'sentiment_local_udf.analyze_message_sentiment' LANGUAGE PYTHON;

CREATE TEMPORARY FUNCTION sentiment_category AS 'sentiment_category_udf.sentiment_category' LANGUAGE PYTHON;

-- ============================================================================
-- 2. Register Kafka Source Table for Customer Messages
-- ============================================================================

CREATE TABLE customer_messages (
    message_id STRING,
    customer_id STRING,
    customer_name STRING,
    message_timestamp BIGINT,
    message_text STRING,
    metadata ROW<
        session_id STRING,
        user_agent STRING,
        ip_country STRING
    >,
    kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',
    kafka_partition INT METADATA FROM 'partition',
    kafka_offset BIGINT METADATA FROM 'offset',
    message_ts_ltz AS TO_TIMESTAMP_LTZ(message_timestamp, 3),
    WATERMARK FOR message_ts_ltz AS message_ts_ltz - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'customer_messages',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-sentiment-local',
    'properties.request.timeout.ms' = '60000',
    'properties.metadata.max.age.ms' = '30000',
    'properties.connections.max.idle.ms' = '300000',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- ============================================================================
-- 3. Register Kafka Sink Table for Sentiment Results (reuse existing)
-- ============================================================================

CREATE TABLE message_sentiments (
    message_id STRING,
    customer_id STRING,
    customer_name STRING,
    message_timestamp BIGINT,
    message_text STRING,
    ip_country STRING,
    sentiment_score DOUBLE,
    sentiment_category STRING,
    model_used STRING,
    processing_timestamp TIMESTAMP(3),
    source_partition INT,
    source_offset BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'message_sentiments',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'sink.parallelism' = '4',
    'sink.partitioner' = 'round-robin'
);

-- ============================================================================
-- 4. Main Processing Query
-- ============================================================================

INSERT INTO message_sentiments
WITH enriched_messages AS (
    SELECT 
        message_id,
        customer_id,
        customer_name,
        message_timestamp,
        message_text,
        metadata.ip_country AS ip_country,
        kafka_partition,
        kafka_offset,
        analyze_message_sentiment(message_text) AS sentiment_score
    FROM customer_messages
    WHERE message_text IS NOT NULL AND CHAR_LENGTH(TRIM(message_text)) > 0
)
SELECT 
    message_id,
    customer_id,
    customer_name,
    message_timestamp,
    message_text,
    ip_country,
    sentiment_score,
    sentiment_category(sentiment_score) AS sentiment_category,
    'tabularisai/multilingual-sentiment-analysis' AS model_used,
    CURRENT_TIMESTAMP AS processing_timestamp,
    kafka_partition AS source_partition,
    kafka_offset AS source_offset
FROM enriched_messages;


