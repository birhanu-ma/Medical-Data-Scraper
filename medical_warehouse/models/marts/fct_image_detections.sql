{{ config(materialized='table') }}

WITH raw_detections AS (
    -- Assuming you loaded the CSV to a table named 'image_analysis'
    SELECT * FROM {{ source('raw_data', 'image_analysis') }}
),

messages AS (
    SELECT * FROM {{ ref('fct_messages') }}
)

SELECT
    m.message_id,
    m.channel_key,
    m.date_key,
    d.image_category,
    d.detected_objects,
    d.confidence_score,
    m.view_count
FROM messages m
INNER JOIN raw_detections d ON m.message_id = d.message_id