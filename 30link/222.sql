
  SELECT
  bb.ia_id,
  bb.ia_name,
  bb.group_id,
  sum(cc.流转后交易额)                                AS 周流转后交易额,
  sum(aa.hht_net_value_sub + aa.hht_net_in_sub)     AS 周激活资金,
  sum(dd.流转后净入金)                                AS 周流转后净入金

FROM
  (SELECT *
   FROM info_silver.ods_crm_transfer_record
   WHERE to_char(submit_time, 'yyyymmdd') BETWEEN 20170513 AND 20170519
         AND PROCESS IN (5, 6) AND valid = 1) aa
  JOIN info_silver.dw_user_account bb
    ON aa.user_id = bb.crm_user_id AND aa.partner_id = bb.partner_id
  LEFT JOIN
  (SELECT
     A.firmid,
     sum(A.contqty) AS 流转后交易额
   FROM info_silver.ods_history_deal A
     JOIN
     (SELECT *
      FROM info_silver.ods_crm_transfer_record
      WHERE to_char(submit_time, 'yyyymmdd') BETWEEN 20170513 AND 20170519
            AND PROCESS IN (5, 6) AND valid = 1) b
       ON A.firmid = b.firm_id
   WHERE A.fdate BETWEEN 20170513 AND 20170519 AND
         A.trade_time > b.submit_time
   GROUP BY A.firmid) cc
    ON aa.firm_id = cc.firmid
  LEFT JOIN
  (SELECT
     A.firmid,
     sum(CASE WHEN A.inorout = 'A'
       THEN A.inoutmoney
         WHEN A.inorout = 'B'
           THEN -A.inoutmoney END) AS 流转后净入金
   FROM silver_njs.history_transfer@silver_std A
     JOIN
     (SELECT *
      FROM info_silver.ods_crm_transfer_record
      WHERE to_char(submit_time, 'yyyymmdd') BETWEEN 20170513 AND 20170519
            AND PROCESS IN (5, 6) AND valid = 1) b
       ON A.firmid = b.firm_id
   WHERE A.fdate BETWEEN 20170513 AND 20170519 AND
         A.realdate > b.submit_time
   GROUP BY a.firmid) dd
    ON aa.firm_id = dd.firmid
WHERE bb.group_id IN (1, 7, 8, 111, 118)
GROUP BY
  bb.ia_id,
  bb.ia_name,
  bb.group_id)