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
split_part(campaign_name,' - ',3) as campaign_type_prospecting,
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
onsite_web_add_to_cart as add_to_cart,
initiate_checkout,
onsite_web_purchase as purchases,
onsite_web_purchase_value as revenue
FROM {{ ref('facebook_performance_by_ad') }}
