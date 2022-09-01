{{ config(alias='TYPES',
          tags=['types']
         ) }}

SELECT 
c.clientid
, t.ID
, t.PID
, t.TAG
, t.sort
, t.type
, t.LABEL
, t.DATA
, t.ROLES
, t.ITEMID
, t.LASTMODIFIEDDATE
,current_timestamp ROW_INSERT_TS
FROM POC.hvmg_raw.TYPES t
join poc.analytics.clients c
on t.client_nm =c.name
