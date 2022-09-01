{{ config(alias='SITESSUMMARYGROUP',
          tags='site'
         ) }}

select 
  c.clientid
  ,sl.siteid sitegroupid
  ,sl.id sitelistid
  ,sl.label
,current_timestamp ROW_INSERT_TS
from poc.raw.siteslists sl
join poc.analytics.clients c 
  on sl.client_nm =c.name
