{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
product_item_id,
product_title,
campaign_status,
campaign_type_default,
case
    when campaign_name ~* 'move' then 'MOVE'
    when campaign_name ~* 'SB_Performance Max_New Product Launch' then 'New Product Launch'
    when campaign_name ~* 'sweats' then 'Sweats'
    when campaign_name ~* 'bfcm' then 'BFCM'
    when campaign_name ~* 'tees' then 'Tees'
    when campaign_name ~* 'tCPA' then 'NC tCPA'
    when campaign_name ~* 'socks' then 'Socks'
    when campaign_name ~* 'branded' then 'Branded'
    else 'Other' 
end as campaign_type_custom,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
matethelabelga4webpurchase as ga4_purchases,
matethelabelga4webpurchase_value as ga4_revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share
FROM {{ ref('googleads_performance_by_campaign_product') }}
