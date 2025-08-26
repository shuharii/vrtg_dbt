{{ config(
    materialized='table',
    partition_by={'field': 'event_date', 'data_type': 'date'},
    cluster_by=['country','platform']
) }}

with base as (
  select
    user_id,
    date(event_date) as event_date,
    country,
    upper(platform) as platform,
    cast(match_start_count as int64) as match_start_count,
    cast(match_end_count as int64)   as match_end_count,
    cast(victory_count as int64)     as victory_count,
    cast(defeat_count as int64)      as defeat_count,
    cast(server_connection_error as int64) as server_connection_error,
    cast(iap_revenue as float64)     as iap_revenue,
    cast(ad_revenue as float64)      as ad_revenue
  from {{ source('vertigo_case','daily') }}
),

agg as (
  select
    event_date,
    country,
    platform,
    count(distinct user_id) as dau,
    sum(iap_revenue)        as total_iap_revenue,
    sum(ad_revenue)         as total_ad_revenue,
    sum(match_start_count)  as matches_started,
    sum(match_end_count)    as matches_ended,
    sum(victory_count)      as victories,
    sum(defeat_count)       as defeats,
    sum(server_connection_error) as server_errors
  from base
  group by 1,2,3
)

select
  event_date,
  country,
  platform,
  dau as DAU,
  total_iap_revenue,
  total_ad_revenue,
  safe_divide(total_iap_revenue + total_ad_revenue, dau) as arpdau,
  matches_started,
  safe_divide(matches_started, dau) as match_per_dau,
  safe_divide(victories, nullif(matches_ended,0)) as win_ratio,
  safe_divide(defeats,   nullif(matches_ended,0)) as defeat_ratio,
  safe_divide(server_errors, nullif(dau,0)) as server_error_per_dau
from agg
