-- depends_on: {{ ref('calendar') }}
-- depends_on: {{ ref('calendarday') }}
-- depends_on: {{ ref('calendarperiod') }}
-- depends_on: {{ ref('currency') }}
-- depends_on: {{ ref('currencyexchangerate') }}
-- depends_on: {{ ref('currencyexchangerateitem') }}
-- depends_on: {{ ref('transactionsperiod') }}
-- depends_on: {{ ref('types') }}

{{ config(alias='FILE_STATUS', 
          schema='ETL',
          tags=['calendar',
                'calendarday',
                'calendarperiod',
                'currency',
                'currencyexchangerate',
                'currencyexchangerateitem',
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
