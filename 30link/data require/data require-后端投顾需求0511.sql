SELECT
  to_char(sysdate - 1, 'yyyymmdd')            AS 日期,
  aa.fa_id                                    AS 主站id,
  max(bb.real_name)                           AS 用户姓名,
  max(bb.ia_id)                               AS 投顾id,
  max(bb.ia_name)                             AS 投顾姓名,
  max(bb.group_id)                            AS 投顾组别,
  sum(aa.money)                               AS 激活资金,
  sum(cc.流转后总交易公斤数)                           AS 流转后总交易公斤数,
  sum(dd.当月手续费)                               AS 当月手续费,
  sum(ee.昨日净资产)                               AS 昨日净资产,
  sum(ff.流转后当月总入金)                            AS 流转后当月总入金,
  sum(ff.流转后当月总出金)                            AS 流转后当月总出金,
  min(aa.submit_time)                         AS 流转时间,
  sum(gg.流转后通话时长)                             AS 流转后通话时长,
  sum(gg.流转后通话次数)                             AS 流转后通话次数,
  sum(hh.首次交易风险准备金) / sum(aa.money)           AS 流转后首笔交易仓位,
  CASE WHEN (sum(kk.计算资金利用率用资金) = 0 OR sum(kk.计算资金利用率用资金) IS NULL)
    THEN NULL
  ELSE sum(dd.当月手续费) / sum(kk.计算资金利用率用资金) END AS 资金利用率
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
     sum(CASE WHEN b.wareid = 'GDAG'
       THEN b.contnum
         WHEN b.wareid = 'GDPT'
           THEN b.contnum * 56
         WHEN b.wareid = 'GDPD'
           THEN b.contnum * 30
         WHEN b.wareid = 'LSAG100g'
           THEN contnum END) AS 流转后总交易公斤数
   FROM info_silver.ods_crm_transfer_record a
     JOIN info_silver.dw_user_account c
       ON a.fa_id = c.user_id AND c.partner_id IN ('hht', 'pmec')
     JOIN (SELECT
             firmid,
             user_id,
             contnum,
             wareid,
             trade_time
           FROM info_silver.ods_history_deal
           WHERE fdate >= '20160901' AND partner_id IN ('hht', 'pmec')) b
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
     JOIN (SELECT
             firm_id,
             fdate,
             net_zcmoney
           FROM info_silver.ods_order_zcmoney
           WHERE partner_id IN ('hht', 'pmec') AND fdate = to_char(sysdate - 1, 'yyyymmdd')) b
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
     JOIN (SELECT
             firmid,
             inorout,
             inoutmoney,
             realdate,
             fdate
           FROM silver_njs.history_transfer@silver_std
           WHERE partnerid IN ('pmec', 'hht')) b
       ON c.firm_id = b.firmid
   WHERE b.realdate > a.submit_time AND substr(b.fdate, 1, 6) = to_char(sysdate - 1, 'yyyymm') AND
         fdate <= to_char(sysdate - 1, 'yyyymmdd')
         AND a.process IN (5, 6) AND a.valid = 1
   GROUP BY c.firm_id
  ) ff
    ON bb.firm_id = ff.firm_id
  LEFT JOIN
  (SELECT
     c.user_id,
     count(b.id)    AS 流转后通话次数,
     sum(b.worksec) AS 流转后通话时长
   FROM info_silver.ods_crm_transfer_record a
     JOIN info_silver.dw_user_account c
       ON a.fa_id = c.user_id AND c.partner_id IN ('hht')
     JOIN info_silver.tb_crm_tel_record b
       ON b.user_id = a.user_id
   WHERE b.connect_time > a.submit_time AND to_char(b.connect_time, 'yyyymmdd') <= to_char(sysdate - 1, 'yyyymmdd')
         AND a.process IN (5, 6) AND a.valid = 1 AND b.worksec > 0
   GROUP BY c.user_id
  ) gg
    ON aa.fa_id = gg.user_id
  LEFT JOIN
  (SELECT
     min1.user_id,
     sum(time1.contqty) / 12.5 AS 首次交易风险准备金
   FROM (SELECT
           c.user_id,
           min(b.trade_time) AS mintime
         FROM info_silver.ods_crm_transfer_record a
           JOIN info_silver.dw_user_account c
             ON a.fa_id = c.user_id AND c.partner_id IN ('hht', 'pmec')
           JOIN (SELECT
                   user_id,
                   firmid,
                   trade_time,
                   fdate,
                   contqty
                 FROM info_silver.ods_history_deal
                 WHERE partner_id IN ('hht', 'pmec') AND fdate >= '20160901' and operation_src = 'open') b
             ON c.firm_id = b.firmid
                AND a.submit_time < b.trade_time
         WHERE a.process IN (5, 6) AND a.valid = 1
         GROUP BY c.user_id) min1
     JOIN (SELECT
             a.trade_time,
             b.user_id,
             a.contqty
           FROM (SELECT
                   user_id,
                   firmid,
                   trade_time,
                   fdate,
                   contqty
                 FROM info_silver.ods_history_deal
                 WHERE partner_id IN ('hht', 'pmec') AND fdate >= '20160901' and operation_src = 'open') a
             JOIN info_silver.dw_user_account b ON a.firmid = b.firm_id) time1
       ON min1.user_id = time1.user_id AND min1.mintime = time1.trade_time
   GROUP BY min1.user_id) hh
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
     LEFT JOIN (SELECT
                  firm_id,
                  net_zcmoney
                FROM (SELECT
                        firm_id,
                        net_zcmoney,
                        fdate
                      FROM info_silver.ods_order_zcmoney
                      WHERE fdate >= to_char(sysdate - 63, 'yyyymmdd') AND partner_id IN ('pmec', 'hht'))
                      WHERE fdate IN ( SELECT CASE WHEN to_char(last_day(add_months( SYSDATE - 1, -1)), 'day') = '星期日'
                                                                                                               THEN to_char(last_day(add_months( SYSDATE - 1, -1)) - 2, 'yyyymmdd')
                WHEN to_char(last_day(add_months( SYSDATE - 1, -1)), 'day') = '星期六'
                                                                            THEN to_char(last_day(add_months( SYSDATE - 1, -1)) - 1, 'yyyymmdd')
                ELSE to_char(last_day(add_months( SYSDATE - 1, -1)), 'yyyymmdd') END
                                                                                 FROM dual)) b
                ON C.firm_id = b.firm_id
                   WHERE A.process IN (5, 6) AND A.valid = 1
                                             GROUP BY C.firm_id
               ) kk
       ON bb.firm_id = kk.firm_id
   WHERE bb.group_id IN (1, 7, 8, 111)
   GROUP BY to_char(sysdate - 1, 'yyyymmdd'), aa.fa_id


   SELECT *
          FROM info_silver.ods_order_zcmoney
          WHERE fdate = 20170430 AND firm_id IN ('163000000070707', '163000000060833')
SELECT *
FROM silver_njs.tb_silver_data_center@silver_std
WHERE hdate = 20170430 AND firmid IN ('163000000070707', '163000000060833')


SELECT to_char(sysdate, 'day')
FROM dual


SELECT sum(inoutmoney)
FROM silver_njs.history_transfer@silver_std
WHERE fdate = '20170510' AND inorout = 'B' AND partnerid IN ('pmec', 'hht')

SELECT last_day(add_months(sysdate, -1))
FROM dual





