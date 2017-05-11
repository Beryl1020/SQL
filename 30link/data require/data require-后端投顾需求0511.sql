SELECT
  to_char(sysdate - 1, 'yyyymmdd') AS 日期,
  max(aa.fa_id) AS 主站id,
  max(bb.real_name) as 用户姓名,
  max(bb.ia_id)  as 投顾id,
  max(bb.ia_name) as 投顾姓名,
  max(bb.group_id) as 投顾组别,
  sum(aa.money)                         AS 激活资金,
  sum(cc.流转后总交易公斤数),
  sum(dd.当月手续费),
  sum(ee.昨日净资产),
  sum(ff.流转后当月总入金),
  sum(ff.流转后当月总出金),
  sum(gg.流转后通话时长),
  sum(gg.流转后通话次数),
  sum(hh.首次交易风险准备金)/sum(aa.money) as 流转后首笔交易仓位,
  sum(dd.当月手续费)/sum(kk.计算资金利用率用资金) as 资金利用率
FROM
  (SELECT
     fa_id,
     submit_time,
     CASE WHEN partner_id = 'hht'
       THEN hht_net_in_sub + hht_net_value_sub
     WHEN partner_id = 'pmec'
       THEN pmec_net_in_sub + pmec_net_value_sub END AS money
   FROM info_silver.ods_crm_transfer_record
   WHERE process IN (5, 6) AND valid = 1) aa
  LEFT JOIN info_silver.dw_user_account bb
    ON aa.fa_id = bb.user_id
       AND bb.partner_id IN ('hht', 'pmec')
  LEFT JOIN
  (SELECT
     c.firm_id,
     sum(b.contnum) AS 流转后总交易公斤数
   FROM info_silver.ods_crm_transfer_record a
     JOIN info_silver.dw_user_account c
       ON a.fa_id = c.user_id AND c.partner_id IN ('hht', 'pmec')
     JOIN info_silver.ods_history_deal b
       ON c.firm_id = b.firmid
          AND a.submit_time < b.trade_time
   WHERE a.process IN (5, 6) AND a.valid = 1
   GROUP BY c.firm_id) cc
    ON bb.firm_id = cc.firm_id
  LEFT JOIN
  (SELECT
     c.firm_id,
     sum(-b.amount) AS 当月手续费
   FROM info_silver.ods_crm_transfer_record a
     JOIN info_silver.dw_user_account c
       ON a.fa_id = c.user_id AND c.partner_id IN ('hht', 'pmec')
     JOIN NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT b
       ON c.firm_id = b.fund_id
   WHERE b.type IN (3, 4) AND to_char(b.create_time - 0.25, 'yyyymm') = to_char(SYSDATE - 1, 'yyyymm')
         AND a.process IN (5, 6) AND a.valid = 1
   GROUP BY c.firm_id) dd
    ON bb.firm_id = dd.firm_id
  LEFT JOIN
  (SELECT
     c.firm_id,
     sum(net_zcmoney) AS 昨日净资产
   FROM info_silver.ods_crm_transfer_record a
     JOIN info_silver.dw_user_account c
       ON a.fa_id = c.user_id AND c.partner_id IN ('hht', 'pmec')
     JOIN info_silver.ods_order_zcmoney b
       ON c.firm_id = b.firm_id
   WHERE b.fdate = to_char(sysdate - 1, 'yyyymmdd')
         AND a.process IN (5, 6) AND a.valid = 1
   GROUP BY c.firm_id
  ) ee
    ON bb.firm_id = ee.firm_id
  LEFT JOIN
  (SELECT
     c.firm_id,
     sum(CASE WHEN b.inorout = 'A'
       THEN b.inoutmoney END)  AS 流转后当月总入金,
     sum(CASE WHEN b.inorout = 'B'
       THEN -b.inoutmoney END) AS 流转后当月总出金
   FROM info_silver.ods_crm_transfer_record a
     JOIN info_silver.dw_user_account c
       ON a.fa_id = c.user_id AND c.partner_id IN ('hht')
     JOIN silver_njs.history_transfer@silver_std b
       ON c.firm_id = b.firmid
   WHERE b.realdate > a.submit_time AND substr(b.fdate, 1, 6) = to_char(sysdate - 1, 'yyyymm') AND
         fdate <= to_char(sysdate - 1, 'yyyymmdd')
         AND a.process IN (5, 6) AND a.valid = 1
   GROUP BY c.firm_id
  ) ff
    on bb.firm_id = ff.firm_id
  LEFT JOIN
  (SELECT
     b.user_id,
     count(b.id)    AS 流转后通话次数,
     sum(b.worksec) AS 流转后通话时长
   FROM info_silver.ods_crm_transfer_record a
     JOIN info_silver.dw_user_account c
       ON a.fa_id = c.user_id AND c.partner_id IN ('hht')
     JOIN info_silver.tb_crm_tel_record b
       ON c.user_id = a.user_id
   WHERE b.connect_time > a.submit_time AND to_char(b.connect_time, 'yyyymmdd') <= to_char(sysdate - 1, 'yyyymmdd')
         AND a.process IN (5, 6) AND a.valid = 1 AND b.worksec > 0
   GROUP BY b.user_id
  ) gg
    ON aa.fa_id = gg.user_id
  LEFT JOIN
  (SELECT
     min1.user_id,
     time1.contqty / 12.5 AS 首次交易风险准备金
   FROM (SELECT
           b.user_id,
           min(b.trade_time) AS mintime
         FROM info_silver.ods_crm_transfer_record a
           JOIN info_silver.dw_user_account c
             ON a.fa_id = c.user_id AND c.partner_id IN ('hht', 'pmec')
           JOIN info_silver.ods_history_deal b
             ON c.firm_id = b.firmid
                AND a.submit_time < b.trade_time
         WHERE a.process IN (5, 6) AND a.valid = 1
         GROUP BY b.user_id) min1
     JOIN (SELECT
             a.trade_time,
             b.user_id,
             a.contqty
           FROM info_silver.ods_history_deal a
             JOIN info_silver.dw_user_account b ON a.firmid = b.firm_id) time1
       ON min1.user_id = time1.user_id AND min1.mintime = time1.trade_time) hh
    ON aa.fa_id = hh.user_id
  LEFT JOIN
  (SELECT
     c.firm_id,
     sum(CASE WHEN to_char(a.submit_time, 'yyyymm') < to_char(sysdate - 1, 'yyyymm')
       THEN nvl(b.net_zcmoney, 0)
         WHEN to_char(a.submit_time, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
           THEN (nvl(a.hht_net_in_sub, 0) + nvl(a.hht_net_value_sub, 0)) END) AS 计算资金利用率用资金
   FROM info_silver.ods_crm_transfer_record a
     JOIN info_silver.dw_user_account c
       ON a.fa_id = c.user_id AND c.partner_id IN ('hht', 'pmec')
     LEFT JOIN (select firm_id,net_zcmoney from info_silver.ods_order_zcmoney where fdate in (SELECT CASE WHEN to_char(last_day(add_months(sysdate - 1, -1)), 'day') = '星期日'
       THEN to_char(last_day(add_months(sysdate - 1, -1)) - 2, 'yyyymmdd')
                                WHEN to_char(last_day(add_months(sysdate - 1, -1)), 'day') = '星期六'
                                  THEN to_char(last_day(add_months(sysdate - 1, -1)) - 1, 'yyyymmdd')
                                ELSE to_char(last_day(add_months(sysdate - 1, -1)), 'yyyymmdd') END
                         FROM dual)) b
       ON c.firm_id = b.firm_id
   WHERE a.process IN (5, 6) AND a.valid = 1
   GROUP BY c.firm_id
  ) kk
    ON bb.firm_id = kk.firm_id




SELECT *
FROM info_silver.ods_order_zcmoney
WHERE fdate = 20170430 AND firm_id IN（'163000000070707', '163000000060833')
SELECT *
FROM silver_njs.tb_silver_data_center@silver_std
WHERE hdate = 20170430 AND firmid IN（'163000000070707', '163000000060833')


SELECT to_char(sysdate, 'day')
FROM dual


SELECT sum(inoutmoney)
FROM silver_njs.history_transfer@silver_std
WHERE fdate = '20170510' AND inorout = 'B' AND partnerid IN ('pmec', 'hht')

SELECT last_day(add_months(sysdate, -1))
FROM dual





