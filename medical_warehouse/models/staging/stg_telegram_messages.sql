WITH raw_data AS (
    -- Reference 'raw_data.fct_messages' which you confirmed exists
    SELECT * FROM {{ source('raw_data', 'fct_messages') }}
)

SELECT
    -- 1. Primary Identifier
    CAST(message_id AS INT) AS message_id,

    -- 2. Timestamps & Dates
    -- Assuming date_key in raw is already formatted (like 20240101) or a date string
    CAST(date_key AS TEXT) AS date_key,
    
    -- 3. Text & Channel Info
    -- Using channel_key since that is what you listed in your table
    CAST(channel_key AS TEXT) AS channel_name,
    TRIM(message_text) AS message_text,
    LENGTH(TRIM(message_text)) AS message_length,

    -- 4. Media & Engagement Flags
    -- Defaulting has_image to true if missing, or use existing column
    TRUE AS has_image, 
    
    -- Ensuring counts match the raw column 'view_count'
    COALESCE(CAST(view_count AS INT), 0) AS view_count

FROM raw_data
-- Basic data cleaning
WHERE message_text IS NOT NULL 
  AND message_text != ''