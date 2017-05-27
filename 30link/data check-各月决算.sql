/* 电销激活资金 */

SELECT
  count(user_id),
  count(DISTINCT fia_id),
  sum(CASE WHEN partneR_id = 'hht'
    THEN hht_net_value_sub + hht_net_in_sub
      WHEN partner_id = 'pmec'
        THEN pmec_net_value_sub + pmec_net_in_sub END)
FROM info_silver.ods_crm_transfer_record
WHERE to_char(submit_time, 'yyyymmdd') BETWEEN 20170401 AND 20170431
      AND valid = 1 AND process IN (5, 6)
      --AND bgroup_id IN (1, 7, 8, 118);


/* 电销交易额*/

SELECT sum(b.contqty)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.dw_user_account c
    ON a.user_id = c.crm_user_id AND c.partneR_id IN ('pmec', 'hht')
  JOIN info_silver.ods_history_deal b
    ON c.firm_id = b.firmid
       AND a.submit_time < b.trade_time
       AND b.fdate BETWEEN '20170401' AND '20170431'
WHERE to_char(a.submit_time, 'yyyymm') BETWEEN '201701' AND '201704'
      AND a.valid = 1 AND a.process IN (5, 6)
      --AND a.bgroup_id IN (1, 7, 8, 118);


SELECT sum(contqty)
FROM info_silver.ods_history_deal
WHERE substr(fdate, 1, 6) = '201704' AND partner_id IN ('pmec', 'hht')

SELECT *
FROM INFO_SILVER.ODS_CRM_TRANSFER_RECORD a
WHERE to_char(a.SUBMIT_TIME, 'yyyymmdd') BETWEEN 20160901 AND 20170431;


SELECT DISTINCT FIA_ID
FROM info_silver.ods_crm_transfer_record
WHERE to_char(submit_time, 'yyyymm') = 201703 AND fgroup_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105) AND process IN (5, 6)

/* 16年留底资金投顾交易额*/

SELECT sum(b.contqty)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.dw_user_account d
    ON a.user_id = d.crm_user_id AND d.partner_id IN ('pmec', 'hht')
  JOIN info_silver.ods_history_deal b
    ON d.firm_id = b.firmid
WHERE to_char(a.submit_time, 'yyyymm') < 201701
      AND a.valid = 1 AND a.process IN (5, 6)
      AND a.submit_time < b.trade_time
      AND b.fdate BETWEEN '20170401' AND '20170431'




/*2016年有入金且1月前未开单用户1月交易额*/

SELECT sum(b.contqty)
FROM
  (SELECT
     c.user_id,
     min(CASE WHEN a.inorout = 'A'
       THEN a.realdate END) AS mindate
   FROM silver_njs.history_transfer@silver_std a
     JOIN (SELECT
             firm_id,
             user_id
           FROM info_silver.dw_user_account) b
       ON a.firmid = b.firm_id
     join (SELECT
             firm_id,
             user_id, partner_id
           FROM info_silver.dw_user_account) c
     on b.user_id = c.user_id and c.partner_id in ('pmec','hht')
   WHERE a.partnerid IN ('pmec', 'hht')
   GROUP BY c.user_id
  ) a
  JOIN info_silver.dw_user_account d
    ON a.user_id = d.user_id AND d.partner_id IN ('hht', 'pmec')
  JOIN (SELECT
          firmid,
          partner_id,
          contqty,
          trade_time,
          fdate
        FROM info_silver.ods_history_deal
        WHERE fdate >= '20170101' AND partner_id IN ('pmec', 'hht')) b
    ON b.firmid = d.firm_id
WHERE
  to_char(a.mindate, 'yyyymm') < 201701 /*首次入金时间*/
  AND b.fdate BETWEEN '20170401' AND '20170431' /*交易时间*/
  AND
  ((d.crm_user_id NOT IN
  (SELECT user_id
   FROM info_silver.ods_crm_transfer_record
   WHERE process IN (5, 6) AND valid = 1
         AND to_char(submit_time, 'yyyymm') <= '201704')) or (d.crm_user_id is null)) /*流转时间*/




/*2016年有入金且2017年1月开单用户流转前交易额*/
SELECT sum(b.contqty)
FROM
  (SELECT
     b.user_id,
     min(CASE WHEN a.inorout = 'A'
       THEN a.realdate END) AS mindate
   FROM silver_njs.history_transfer@silver_std a
     JOIN (SELECT
             firm_id,
             user_id
           FROM info_silver.dw_user_account) b
       ON a.firmid = b.firm_id
   WHERE a.partnerid IN ('pmec', 'hht')
   GROUP BY b.user_id
  ) a
   JOIN info_silver.dw_user_account d
    ON a.user_id = d.user_id AND d.partner_id IN ('hht', 'pmec')
  JOIN (SELECT
          firmid,
          partner_id,
          contqty,
          trade_time,
          fdate
        FROM info_silver.ods_history_deal
        WHERE fdate >= '20170101' AND partner_id IN ('pmec', 'hht')) b
    ON b.firmid = d.firm_id
  JOIN info_silver.ods_crm_transfer_record c
    ON d.crm_user_id = c.user_id
WHERE
  to_char(a.mindate, 'yyyymm') < 201701 /*首次入金时间*/
  AND b.fdate BETWEEN 20170401 AND 20170431 /*交易时间*/
  AND to_char(c.submit_time, 'yyyymm') = 201704 /*流转时间*/
  AND c.submit_time > b.trade_time





/*1月有入金且2017年1月开单用户流转前交易额*/
SELECT sum(b.contqty)
FROM
  (SELECT
     b.user_id,
     min(CASE WHEN a.inorout = 'A'
       THEN a.realdate END) AS mindate
   FROM silver_njs.history_transfer@silver_std a
     JOIN (SELECT
             firm_id,
             user_id
           FROM info_silver.dw_user_account) b
       ON a.firmid = b.firm_id
   WHERE a.partnerid IN ('pmec', 'hht')
   GROUP BY b.user_id
  ) a
  JOIN info_silver.dw_user_account d
    ON a.user_id = d.user_id AND d.partner_id IN ('hht', 'pmec')
  JOIN (SELECT
          firmid,
          partner_id,
          contqty,
          trade_time,
          fdate
        FROM info_silver.ods_history_deal
        WHERE fdate >= '20170101' AND partner_id IN ('pmec', 'hht')) b
    ON b.firmid = d.firm_id
  JOIN info_silver.ods_crm_transfer_record c
    ON d.crm_user_id = c.user_id

WHERE
  to_char(a.mindate, 'yyyymm') = '201701'         /*首次入金时间*/
  AND b.fdate BETWEEN '20170401' AND '20170431'   /*  交易时间  */
  AND to_char(c.submit_time, 'yyyymm') = '201704' /*  流转时间  */
  AND c.submit_time > b.trade_time                /*交易在流转之前*/



/*1月有入金且1月未开单用户1月交易额*/
SELECT sum(b.contqty)
FROM
  (SELECT
     b.user_id,
     min(CASE WHEN a.inorout = 'A'
       THEN a.realdate END) AS mindate
   FROM silver_njs.history_transfer@silver_std a
     JOIN (SELECT
             firm_id,
             user_id
           FROM info_silver.dw_user_account) b
       ON a.firmid = b.firm_id
   WHERE a.partnerid IN ('pmec', 'hht')
   GROUP BY b.user_id
  ) a
  JOIN info_silver.dw_user_account d
    ON a.user_id = d.user_id AND d.partner_id IN ('hht', 'pmec')
  JOIN (SELECT
          firmid,
          partner_id,
          contqty,
          trade_time,
          fdate
        FROM info_silver.ods_history_deal
        WHERE fdate >= '20170101' AND partner_id IN ('pmec', 'hht')) b
    ON b.firmid = d.firm_id
WHERE
  to_char(a.mindate, 'yyyymm') = '201703'  /*首次入金时间*/
  AND b.fdate BETWEEN '20170401' AND '20170431' /*交易时间*/
  AND
  ((d.crm_user_id NOT IN
  (SELECT user_id
   FROM info_silver.ods_crm_transfer_record
   WHERE process IN (5, 6) AND valid = 1
         AND to_char(submit_time, 'yyyymm') BETWEEN '201701' AND '201704')) or (d.crm_user_id is null)) /*流转时间*/