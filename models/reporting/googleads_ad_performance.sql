{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT
account_id,
ad_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
case
    when campaign_name ~* 'move' then 'MOVE'
    when campaign_name ~* 'sweats' then 'Sweats'
    when campaign_name ~* 'bfcm' then 'BFCM'
    when campaign_name ~* 'tees' then 'Tees'
    when campaign_name ~* 'tCPA' then 'NC tCPA'
    when campaign_name ~* 'socks' then 'Socks'
    when campaign_name ~* 'branded' then 'Branded'
    else 'Other' 
end as campaign_type_custom,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue

FROM {{ ref('googleads_performance_by_ad') }}
