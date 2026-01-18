{{ config(materialized='table') }}

SELECT
    MD5(channel_name) AS channel_key,
    channel_name,
    COUNT(*) AS total_messages_contributed
FROM {{ ref('stg_telegram_messages') }}
GROUP BY 1, 2