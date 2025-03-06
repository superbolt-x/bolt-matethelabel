{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
link_clicks,
onsite_web_add_to_cart as add_to_cart,
initiate_checkout,
onsite_web_purchase as purchases,
onsite_web_purchase_value as revenue
FROM {{ ref('facebook_performance_by_campaign') }}
