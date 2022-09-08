-- depends_on: {{ ref('b_calendar') }}
-- depends_on: {{ ref('b_calendarday') }}
-- depends_on: {{ ref('b_calendarperiod') }}
-- depends_on: {{ ref('b_currency') }}
-- depends_on: {{ ref('b_currencyexchangerate') }}
-- depends_on: {{ ref('b_currencyexchangerateitem') }}
-- depends_on: {{ ref('b_currencyexchangeratetype') }}
-- depends_on: {{ ref('b_items') }}
-- depends_on: {{ ref('b_itemslists') }}
-- depends_on: {{ ref('b_itemssite') }}
-- depends_on: {{ ref('b_itemstypes') }}
-- depends_on: {{ ref('b_laboritems') }}
-- depends_on: {{ ref('b_laborperson') }}
-- depends_on: {{ ref('b_labortypes') }}
-- depends_on: {{ ref('b_master') }}
-- depends_on: {{ ref('b_menupermissionstemplates') }}
-- depends_on: {{ ref('b_options') }}
-- depends_on: {{ ref('b_security') }}
-- depends_on: {{ ref('b_segmentations') }}
-- depends_on: {{ ref('b_siteoptions') }}
-- depends_on: {{ ref('b_sites') }}
-- depends_on: {{ ref('b_sitesaddresses') }}
-- depends_on: {{ ref('b_sitesbilling') }}
-- depends_on: {{ ref('b_sitesgroup') }}
-- depends_on: {{ ref('b_siteslists') }}
-- depends_on: {{ ref('b_siteslistsitems') }}
-- depends_on: {{ ref('b_sitesrooms') }}
-- depends_on: {{ ref('b_sitestypes') }}
-- depends_on: {{ ref('b_templateroles') }}
-- depends_on: {{ ref('b_transactions') }}
-- depends_on: {{ ref('b_transactionsdaydetail') }}
-- depends_on: {{ ref('b_transactionsmonth') }}
-- depends_on: {{ ref('b_transactionsperiod') }}
-- depends_on: {{ ref('b_types') }}

{{ config(alias='FILE_STATUS', 
          schema='ETL',
          tags=['calendar',
                'calendarday',
                'calendarperiod',
                'currency',
                'currencyexchangerate',
                'currencyexchangerateitem',
                'currencyexchangeratetype',
                'items',
                'itemslists',
                'itemssite',
                'itemstypes',
                'laboritems',
                'laborperson',
                'labortypes',
                'master',
                'menupermissionstemplates',
                'options',
                'security',
                'segmentations',
                'siteoptions',
                'sites',
                'sitesaddresses',
                'sitesbilling',
                'sitesgroup',
                'siteslists',
                'siteslistsitems',
                'sitesrooms',
                'sitestypes',
                'templateroles',
                'transactions',
                'transactionsdaydetail',
                'transactionsmonth',
                'transactionsperiod',
                'types']
         ) }}

/***********************************************************
 *  This query will update the FILE_STATUS to indicate 
 *  that these file(s) have been processed
************************************************************/
(
    SELECT a.FILE_NAME,
       a.FILE_STATUS,
       a.CLIENT_NM,
       a.TABLE_NAME,
       a.FILE_PROCESSED_TS
  FROM "POC"."ETL"."FILE_STATUS"  a
MINUS
SELECT s.FILE_NAME,
       s.FILE_STATUS,
       s.CLIENT_NM,
       s.TABLE_NAME,
       s.FILE_PROCESSED_TS
  FROM "POC"."ETL"."FILE_STATUS"  s
  JOIN {{ref('file_control')}}  ctrl
    ON s.FILE_NAME = ctrl.FILE_NAME and
       s.CLIENT_NM = ctrl.CLIENT_NM and
       s.TABLE_NAME = ctrl.TABLE_NAME
 WHERE s.FILE_STATUS IS NULL
 )
UNION
SELECT *
  FROM {{ref('file_control')}}  ctrl
