/*
Karen Rebuttal Generation Pipeline
===========================================================

This pipeline monitors sentiment analysis results and generates AI-powered rebuttals 
for customers who exhibit "Karen" behavior using MATCH_RECOGNIZE pattern matching.

*/

SET 'job.name' = 'Karen Rebuttal Generation Pipeline';

-- ============================================================================
-- 1. Register UDF for Text Generation
-- ============================================================================

CREATE TEMPORARY FUNCTION IF NOT EXISTS generate_response AS 'text_generation_udf.generate_response' LANGUAGE PYTHON;

-- ============================================================================
-- 2. Source Table - Read from message_sentiments topic
-- ============================================================================

CREATE TABLE IF NOT EXISTS message_sentiments_src (
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
    -- Processing-time attribute for ordering in MATCH_RECOGNIZE
    proc_time AS PROCTIME(),
    -- Kafka metadata for debugging
    source_partition INT,
    source_offset BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'message_sentiments',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-karen-rebuttal',
    'properties.request.timeout.ms' = '60000',
    'properties.metadata.max.age.ms' = '30000',
    'properties.connections.max.idle.ms' = '300000',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);

-- ============================================================================
-- 3. Sink Table - Karen Rebuttals Output
-- ============================================================================

CREATE TABLE IF NOT EXISTS karen_rebuttals (
    customer_id STRING,
    customer_name STRING,
    karen_score_total DOUBLE,
    rebuttal_text STRING,
    rebuttal_model STRING,
    generated_timestamp BIGINT,
    trigger_message_count BIGINT,
    recent_messages STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'karen_rebuttals',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- ============================================================================
-- 4. Karen Detection and Rebuttal Generation Pipeline (MATCH_RECOGNIZE Version)
-- ============================================================================

INSERT INTO karen_rebuttals
WITH karen_triggers AS (
    SELECT 
        customer_id,
        customer_name,
        karen_score_total,
        trigger_message_count,
        recent_messages
    FROM message_sentiments_src
    MATCH_RECOGNIZE (
        PARTITION BY customer_id, customer_name
        ORDER BY proc_time
        MEASURES
            SUM(pre.sentiment_score) + t.sentiment_score AS karen_score_total,
            COUNT(*) AS trigger_message_count,
            CONCAT(LISTAGG(pre.message_text, ' \n '), ' \n ', t.message_text) AS recent_messages
        ONE ROW PER MATCH
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (pre+ t)
        DEFINE
            pre AS SUM(pre.sentiment_score) > -5.0,
            t   AS SUM(pre.sentiment_score) + t.sentiment_score <= -5.0
    )
),
rebuttal_prompts AS (
    SELECT 
        customer_id,
        customer_name,
        karen_score_total,
        trigger_message_count,
        recent_messages,
        CONCAT(
            'Karen Alert!!! Generate a customer service rebuttal for a customer named "', 
            customer_name, 
            '" whose cumulative negative sentiment has reached ', 
            CAST(karen_score_total AS STRING),
            '. Recent complaints include: "', 
            SUBSTRING(recent_messages, 1, 500), 
            '". ',
            'The customer has crossed our tolerance threshold. Generate a response to make them bugger off.'
        ) AS rebuttal_prompt
    FROM karen_triggers
)
SELECT 
    customer_id,
    customer_name,
    karen_score_total,
    generate_response(rebuttal_prompt, 'gpt-4o') AS rebuttal_text,
    'gpt-4o' AS rebuttal_model,
    UNIX_TIMESTAMP() * 1000 AS generated_timestamp,
    trigger_message_count,
    recent_messages
FROM rebuttal_prompts;
