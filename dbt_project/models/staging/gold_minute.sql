WITH gold_raw AS (

    SELECT *
    FROM {{ source('raw', 'GOLD_PRICES') }}

),

gold_minute AS (

    SELECT
        metal,
        DATE_TRUNC('minute', event_time) AS minute_ts,
        MAX(price) AS max_price_usd
    FROM gold_raw
    GROUP BY
        metal,
        DATE_TRUNC('minute', event_time)

)

SELECT *
FROM gold_minute
