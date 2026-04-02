{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH initial_sho_data AS
    (SELECT date, day, week, month, quarter, year, 
        order_id, customer_order_index, gross_revenue, total_revenue, subtotal_discount, shipping_price, total_tax, shipping_discount, 0 as subtotal_refund, 0 as shipping_refund, 0 as tax_refund
    FROM {{ source('reporting', 'shopify_daily_sales_by_order') }}
    UNION ALL
    SELECT date, day, week, month, quarter, year, 
        null as order_id, customer_order_index, 0 as gross_revenue, 0 as total_revenue, 0 as subtotal_discount, 0 as shipping_price, 0 as total_tax, 0 as shipping_discount, subtotal_refund, shipping_refund, tax_refund 
    FROM {{ source('reporting', 'shopify_daily_refunds') }})

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
    
, sho_data AS (
        {%- for date_granularity in date_granularity_list %}
            SELECT 
    			date_trunc('{{date_granularity}}',date) as date,
                '{{date_granularity}}' as date_granularity,
    			'Actual' as data_type,
    			sum(0) as spend,
                COUNT(DISTINCT order_id) as shopify_orders, 
                COUNT(DISTINCT CASE WHEN customer_order_index = 1 THEN order_id END) as shopify_first_orders,
                SUM(COALESCE(gross_revenue,0)-COALESCE(subtotal_discount,0)+COALESCE(total_tax,0)+COALESCE(shipping_price,0)-COALESCE(shipping_discount,0)) as shopify_sales,
                SUM(CASE WHEN customer_order_index = 1 THEN COALESCE(gross_revenue,0)-COALESCE(subtotal_discount,0)+COALESCE(total_tax,0)+COALESCE(shipping_price,0)-COALESCE(shipping_discount,0) END) as shopify_first_sales,
                SUM(COALESCE(subtotal_refund,0)-COALESCE(shipping_refund,0)+COALESCE(tax_refund,0)) as shopify_refund,
                SUM(CASE WHEN customer_order_index = 1 THEN COALESCE(subtotal_refund,0)-COALESCE(shipping_refund,0)+COALESCE(tax_refund,0) END) as shopify_first_refund
            FROM initial_sho_data
            GROUP BY 1,2,3
            {%- if not loop.last %}UNION ALL{%- endif %}
        {% endfor %}
		)

, actual_data as (select 
    date,
    date_granularity,
    data_type,
    sum(spend) as original_spend,
    sum(spend) as spend,
    sum(first_orders) as first_orders,
    sum(returning_orders) as returning_orders,
    sum(subtotal_sales) as total_sales
from 
(select date, date_granularity, 'Actual' as data_type, sum(spend) as spend, 0 as first_orders, 0 as returning_orders, 0 as subtotal_sales 
from {{ source('reporting', 'facebook_ad_performance') }}
where campaign_name != '[SB] Prospecting - Advantage+ - Video Testing'
group by 1,2,3
union all 
select date, date_granularity, 'Actual' as data_type, sum(spend) as spend, 0 as first_orders, 0 as returning_orders, 0 as subtotal_sales 
from {{ source('reporting', 'googleads_campaign_performance') }}
group by 1,2,3
union all 
select date, date_granularity, data_type, spend, shopify_first_orders as first_orders, 
    coalesce(shopify_orders,0) - coalesce(shopify_first_orders,0) as returning_orders, 
    coalesce(shopify_sales,0) - coalesce(shopify_refund,0)  as subtotal_sales 
from sho_data)
group by 1,2,3)

, forcasted_data as (
{%- for date_granularity in date_granularity_list %}
    select date_trunc('{{date_granularity}}',date)::date as date, '{{date_granularity}}' as date_granularity, 'Forecasted' as data_type, 
    sum(original_spend) as original_spend, sum(original_spend) as spend, sum(acquisitions) as first_orders, sum(returning) as returning, sum(revenue) as total_sales
    from {{ source('gsheet_raw', 'forecast_data') }}
    group by 1,2,3
    {%- if not loop.last %}union all{%- endif %}
{% endfor %}
)

, adjusted_forcasted_data as (
{%- for date_granularity in date_granularity_list %}
    select date_trunc('{{date_granularity}}',date)::date as date, '{{date_granularity}}' as date_granularity, 'Adjusted Forecasted' as data_type, 
    sum(adjusted_spend) as original_spend, sum(adjusted_spend) as spend, sum(adjusted_acquisitions) as first_orders, sum(adjusted_returning) as returning, sum(adjusted_revenue) as total_sales
    from {{ source('gsheet_raw', 'forecast_data') }}
    group by 1,2,3
    {%- if not loop.last %}union all{%- endif %}
{% endfor %}
)

select * from actual_data 
union all
select * from forcasted_data 
union all
select * from adjusted_forcasted_data 
order by date desc
