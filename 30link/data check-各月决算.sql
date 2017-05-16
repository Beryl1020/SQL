/* 电销激活资金 */

SELECT
  count(user_id),count(distinct fia_id),
  sum(CASE WHEN partneR_id = 'hht'
    THEN hht_net_value_sub + hht_net_in_sub
      WHEN partner_id = 'pmec'
        THEN pmec_net_value_sub + pmec_net_in_sub END)
FROM info_silver.ods_crm_transfer_record
WHERE to_char(submit_time, 'yyyymmdd') between 20170401 and 20170431
      AND valid = 1 AND process IN (5, 6)
      AND bgroup_id IN (111);


/* 电销交易额*/

SELECT sum(b.contqty)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.dw_user_account c
    ON a.user_id = c.crm_user_id AND c.partneR_id IN ('pmec', 'hht')
  JOIN info_silver.ods_history_deal b
    ON c.firm_id = b.firmid
       AND a.submit_time < b.trade_time
       AND b.fdate BETWEEN 20170401 AND 20170431
WHERE to_char(a.submit_time, 'yyyymm') BETWEEN 201701 AND 201704
      AND a.valid = 1 AND a.process IN (5, 6)
      AND a.bgroup_id IN (111);


SELECT *
FROM INFO_SILVER.ODS_CRM_TRANSFER_RECORD a
WHERE to_char(a.SUBMIT_TIME, 'yyyymmdd') BETWEEN 20160901 AND 20170431;


SELECT DISTINCT FIA_ID
FROM info_silver.ods_crm_transfer_record
WHERE to_char(submit_time, 'yyyymm') = 201703 AND fgroup_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105) AND process IN (5, 6)

/* 16年留底资金投顾交易额*/

SELECT sum(b.contqty)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.ods_history_deal b
    ON a.firm_id = b.firmid
WHERE to_char(a.submit_time, 'yyyymm') < 201701
      AND a.valid = 1 AND a.process IN (5, 6)
      AND a.submit_time < b.trade_time
      AND b.fdate BETWEEN 20170301 AND 20170331

/* 16年留底资金互联网交易额*/

SELECT sum(b.contqty)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.ods_history_deal b
    ON a.firm_id = b.firmid
WHERE to_char(a.submit_time, 'yyyymm') < 201701
      AND a.valid = 1 AND a.process IN (5, 6)
      AND a.submit_time < b.trade_time
      AND b.fdate BETWEEN 20170301 AND 20170331



/*2016年有入金且1月前未开单用户1月交易额*/
SELECT sum(b.contqty)
FROM
  (SELECT
     firmid,
     min(CASE WHEN inorout = 'A'
       THEN realdate END) AS mindate
   FROM silver_njs.history_transfer@silver_std
   WHERE partnerid = 'pmec'
   GROUP BY firmid
  ) a
  JOIN info_silver.ods_history_deal b
    ON a.firmid = b.firmid
WHERE
  to_char(a.mindate, 'yyyymm') < 201701 /*首次入金时间*/
  AND b.fdate BETWEEN 20170301 AND 20170331 /*交易时间*/
  AND
  a.firmid NOT IN
  (SELECT firm_id
   FROM info_silver.ods_crm_transfer_record
   WHERE process IN (5, 6) AND valid = 1
         AND to_char(submit_time, 'yyyymm') <= 201703) /*流转时间*/




/*2016年有入金且2017年1月开单用户流转前交易额*/
SELECT sum(b.contqty)
FROM
  (SELECT
     firmid,
     min(CASE WHEN inorout = 'A'
       THEN realdate END) AS mindate
   FROM silver_njs.history_transfer@silver_std
   WHERE partnerid = 'pmec'
   GROUP BY firmid
  ) a
  JOIN info_silver.ods_history_deal b
    ON a.firmid = b.firmid

  JOIN info_silver.ods_crm_transfer_record c
    ON a.firmid = c.firm_id

WHERE
  to_char(a.mindate, 'yyyymm') < 201701 /*首次入金时间*/
  AND b.fdate BETWEEN 20170301 AND 20170331 /*交易时间*/
  AND to_char(c.submit_time, 'yyyymm') = 201703 /*流转时间*/
  AND c.submit_time > b.trade_time




/*1月有入金且2017年1月开单用户流转前交易额*/
SELECT sum(b.contqty)
FROM
  (SELECT
     firmid,
     min(CASE WHEN inorout = 'A'
       THEN realdate END) AS mindate
   FROM silver_njs.history_transfer@silver_std
   WHERE partnerid = 'pmec'
   GROUP BY firmid
  ) a
  JOIN info_silver.ods_history_deal b
    ON a.firmid = b.firmid

  JOIN info_silver.ods_crm_transfer_record c
    ON a.firmid = c.firm_id

WHERE
  to_char(a.mindate, 'yyyymm') = 201703 /*首次入金时间*/
  AND b.fdate BETWEEN 20170301 AND 20170331 /*交易时间*/
  AND to_char(c.submit_time, 'yyyymm') = 201703  /*流转时间*/
  AND c.submit_time > b.trade_time



/*1月有入金且1月未开单用户1月交易额*/
SELECT sum(b.contqty)
FROM
  (SELECT
     firmid,
     min(CASE WHEN inorout = 'A'
       THEN realdate END) AS mindate
   FROM silver_njs.history_transfer@silver_std
   WHERE partnerid = 'pmec'
   GROUP BY firmid
  ) a
  JOIN info_silver.ods_history_deal b
    ON a.firmid = b.firmid
WHERE
  to_char(a.mindate, 'yyyymm') = 201703 /*首次入金时间*/
  AND b.fdate BETWEEN 20170301 AND 20170331/*交易时间*/
  AND
  a.firmid NOT IN
  (SELECT firm_id
   FROM info_silver.ods_crm_transfer_record
   WHERE process IN (5, 6) AND valid = 1
         AND to_char(submit_time, 'yyyymm') BETWEEN 201703 AND 201703) /*流转时间*/