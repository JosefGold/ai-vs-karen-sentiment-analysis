/*
OpenAI Sentiment Analysis Pipeline
==================================

This pipeline performs real-time sentiment analysis of customer messages using OpenAI LLM.

 */

SET 'job.name' = 'OpenAI Sentiment Analysis Pipeline';

-- ============================================================================
-- 1. Register Python UDF for OpenAI Sentiment Analysis
-- ============================================================================

CREATE TEMPORARY FUNCTION analyze_message_sentiment AS 'sentiment_openai_udf.analyze_message_sentiment' LANGUAGE PYTHON;

-- ============================================================================
-- 2. Helper Function for Sentiment Categories
-- ============================================================================

CREATE TEMPORARY FUNCTION sentiment_category AS 'sentiment_category_udf.sentiment_category' LANGUAGE PYTHON;

-- ============================================================================
-- 3. Register Kafka Source Table for Customer Messages
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
    -- Kafka metadata
    kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',
    kafka_partition INT METADATA FROM 'partition',
    kafka_offset BIGINT METADATA FROM 'offset',
    -- Computed column to convert epoch millis to timestamp
    message_ts_ltz AS TO_TIMESTAMP_LTZ(message_timestamp, 3),
    -- Watermark for event time processing
    WATERMARK FOR message_ts_ltz AS message_ts_ltz - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'customer_messages',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-sentiment-openai',
    'properties.request.timeout.ms' = '60000',
    'properties.metadata.max.age.ms' = '30000',
    'properties.connections.max.idle.ms' = '300000',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true' 
);

-- ============================================================================
-- 4. Register Kafka Sink Table for Sentiment Results
-- ============================================================================

CREATE TABLE message_sentiments (
    message_id STRING,
    customer_id STRING,
    customer_name STRING,
    message_timestamp BIGINT,
    message_text STRING,
    ip_country STRING,
    -- Sentiment analysis results
    sentiment_score DOUBLE,
    sentiment_category STRING,
    model_used STRING,
    processing_timestamp TIMESTAMP(3),
    -- Kafka metadata for debugging
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
-- 5. Main Processing Query - Stream Processing with Sentiment Analysis
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
        -- Single sentiment analysis call
        analyze_message_sentiment(message_text) AS sentiment_score
    FROM customer_messages
    WHERE 
        -- Only process non-empty messages
        message_text IS NOT NULL 
        AND CHAR_LENGTH(TRIM(message_text)) > 0
)
SELECT 
    message_id,
    customer_id,
    customer_name,
    message_timestamp,
    message_text,
    ip_country,
    -- Use pre-computed sentiment score
    sentiment_score,
    sentiment_category(sentiment_score) AS sentiment_category,
    'gpt-4.1-nano' AS model_used,
    CURRENT_TIMESTAMP AS processing_timestamp,
    -- Kafka metadata for debugging
    kafka_partition AS source_partition,
    kafka_offset AS source_offset
FROM enriched_messages;

