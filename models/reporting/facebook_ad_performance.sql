{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
CASE WHEN campaign_name ~* 'Prospecting' THEN 'Campaign Type: Prospecting'
    WHEN campaign_name ~* 'Retargeting' THEN 'Campaign Type: Retargeting'
    WHEN campaign_name ~* 'Lifecycle' THEN 'Campaign Type: Lifecycle'
    ELSE campaign_type_default
END as campaign_type_default,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions, 
link_clicks,
add_to_cart,
initiate_checkout,
purchases,
revenue
FROM {{ ref('facebook_performance_by_ad') }}
