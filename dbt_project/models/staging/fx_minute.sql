WITH fx_raw AS (

    SELECT *
    FROM {{ source('raw', 'FX_RATES') }}

),

fx_minute AS (

    SELECT
        currency,
        DATE_TRUNC('minute', event_time) AS minute_ts,
        MAX(rate) AS max_rate
    FROM fx_raw
    GROUP BY
        currency,
        DATE_TRUNC('minute', event_time)

)

SELECT *
FROM fx_minute
