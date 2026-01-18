{{ config(materialized='table') }}

SELECT
    m.message_id,
    -- Generate the same MD5 key used in your dim_channels
    MD5(m.channel_name) AS channel_key,
    -- Create the date_key from the timestamp to link to dim_dates
    CAST(TO_CHAR(m.message_timestamp, 'YYYYMMDD') AS INT) AS date_key,
    m.message_text,
    m.view_count,
    m.has_image
FROM {{ ref('stg_telegram_messages') }} m