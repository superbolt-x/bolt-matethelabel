WITH granularities AS (
    SELECT 'day' AS g
    UNION ALL SELECT 'week'
    UNION ALL SELECT 'month'
    UNION ALL SELECT 'quarter'
    UNION ALL SELECT 'year'
),

initial_sho_data AS (
    SELECT *
    FROM {{ source('reporting', 'shopify_daily_sales_by_order') }}

    UNION ALL

    SELECT *
    FROM {{ source('reporting', 'shopify_daily_refunds') }}
),

sho_data AS (
    SELECT 
        date_trunc(g.g, date) as date,
        g.g as date_granularity,
        'Actual' as data_type,
        0 as spend,

        COUNT(DISTINCT order_id) as shopify_orders,
        COUNT(DISTINCT CASE WHEN customer_order_index = 1 THEN order_id END) as shopify_first_orders,

        SUM(
            COALESCE(gross_revenue,0)
            - COALESCE(subtotal_discount,0)
            + COALESCE(total_tax,0)
            + COALESCE(shipping_price,0)
            - COALESCE(shipping_discount,0)
        ) as shopify_sales

    FROM initial_sho_data, granularities g
    GROUP BY 1,2,3
),

ads_data AS (
    SELECT * 
    FROM {{ source('reporting', 'facebook_ad_performance') }}

    UNION ALL

    SELECT * 
    FROM {{ source('reporting', 'googleads_campaign_performance') }}
),

forecasted_data AS (
    SELECT 
        date_trunc(g.g, date) as date,
        g.g as date_granularity,
        'Forecasted' as data_type,

        SUM(original_spend) as spend,
        SUM(acquisitions) as first_orders,
        SUM(returning) as returning_orders,
        SUM(revenue) as total_sales

    FROM {{ source('gsheet_raw', 'forecast_data') }}, granularities g
    GROUP BY 1,2,3
)

SELECT * FROM sho_data
UNION ALL
SELECT * FROM forecasted_data
ORDER BY date DESC;
