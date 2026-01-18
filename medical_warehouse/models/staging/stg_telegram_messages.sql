WITH raw_data AS (
    -- This refers to the source defined in your sources.yml
    SELECT * FROM {{ source('raw_data', 'telegram_messages') }}
)

SELECT
    -- 1. Primary Identifier
    CAST(message_id AS INT) AS message_id,

    -- 2. Timestamps & Dates
    CAST(message_date AS TIMESTAMP) AS message_timestamp,
    -- Creating a date_key for the Star Schema join (YYYYMMDD)
    CAST(TO_CHAR(CAST(message_date AS TIMESTAMP), 'YYYYMMDD') AS INT) AS date_key,

    -- 3. Text & Channel Info
    TRIM(channel_name) AS channel_name,
    TRIM(message_text) AS message_text,
    LENGTH(TRIM(message_text)) AS message_length,

    -- 4. Media & Engagement Flags
    -- Standardizing to a boolean flag as requested in instructions
    CASE 
        WHEN has_media = TRUE THEN TRUE 
        ELSE FALSE 
    END AS has_image,
    
    -- Ensuring counts are never NULL for analysis
    COALESCE(CAST(views AS INT), 0) AS view_count,
    COALESCE(CAST(forwards AS INT), 0) AS forward_count

FROM raw_data
-- Filtering out 'messy' data as per instructions
WHERE message_text IS NOT NULL 
  AND message_text != ''