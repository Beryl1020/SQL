SELECT
  f.fa_id              主站id,
  f.crm_name           客户姓名,
  f.fia_id             投顾id,
  f.fname              投顾姓名,
  f.submit_time        流转时间,
  f.jhzj               激活资金,
  f.net_in             后端净入金,
  f.jye                后端交易额,
  f.sxf                后端手续费,
  f.sr + f.znj + f.sxf 后端收入,
  g.net_assets         净资产
FROM
  (
    SELECT
      e.*,
      nvl(sum(CASE WHEN changetype = '8'
        THEN -amount END), 0) znj,
      nvl(sum(CASE WHEN changetype IN (9, 10)
        THEN -amount END), 0) sr
    FROM
      (
        SELECT
          c.*,
          sum(d.contqty)           jye,
          sum(d.contqty) * 0.00065 sxf
        FROM
          (
            SELECT
              a.fa_id,
              a.firm_id,
              a.crm_name,
              a.fia_id,
              a.fname,
              a.submit_time,
              a.PMEC_NET_VALUE_SUB + a.PMEC_NET_IN_SUB jhzj,
              sum(CASE WHEN b.inorout = 'A'
                THEN b.inoutmoney
                  ELSE -b.inoutmoney END)              net_in
            FROM info_silver.ods_crm_transfer_record a
              LEFT JOIN silver_njs.history_transfer@silver_std b
                ON a.firm_id = b.firmid AND b.realdate > a.submit_time
            WHERE a.fgroup_id IN (112, 113, 114, 106) AND fia_id != 154 AND a.process IN (5, 6)
            GROUP BY a.fa_id, a.firm_id, a.crm_name, a.fia_id, a.fname, a.submit_time,
              a.PMEC_NET_VALUE_SUB + a.PMEC_NET_IN_SUB
          ) c
          LEFT JOIN info_silver.ods_history_deal d ON c.firm_id = d.firmid AND d.trade_time > c.submit_time
        GROUP BY c.fa_id, c.firm_id, c.crm_name, c.fia_id, c.fname, c.submit_time, c.jhzj, c.net_in
      ) e LEFT JOIN silver_njs.pmec_zj_flow@silver_std f ON e.firm_id = f.loginaccount AND f.createdate > e.submit_time
    GROUP BY e.fa_id, e.firm_id, e.crm_name, e.fia_id, e.fname, e.submit_time, e.jhzj, e.net_in, e.jye, e.sxf
  ) f LEFT JOIN silver_njs.tb_silver_data_center@silver_std g ON f.firm_id = g.firmid
WHERE trunc(f.submit_time) <= sysdate - 1 AND g.hdate = to_char(sysdate - 1, 'yyyymmdd')



------------------------------------------------------------------------------------------------------------------------
SELECT
  b.user_id                              AS 主站id,
  b.real_name                            AS 客户姓名,
  b.ia_id                                AS 投顾id,
  b.ia_name                              AS 投顾姓名,
  b.group_id as 投顾组别,
  a.submit_time                          AS 流转时间,
  a.hht_net_value_sub + a.hht_net_in_sub AS 激活资金,
  aa.后端净入金,
  bb.后端交易额,
  cc.net_zcmoney                         AS 后端净资产,
  dd.后端手续费,
  nvl(ee.后端头寸加点差,0)+nvl(ee.后端滞纳金,0)+nvl(dd.后端手续费,0) as 后端收入
FROM silver_consult.tb_crm_transfer_record@consul_std a
  JOIN info_silver.dw_user_account b
    ON a.user_id = b.crm_user_id
       AND b.partner_id = 'hht' AND a.partnerid = 'hht'
       AND b.group_id IN (1, 7, 8, 111)
  LEFT JOIN
  (SELECT
     a1.user_id,
     sum(CASE WHEN a3.inorout = 'A'
       THEN a3.inoutmoney
         WHEN a3.inorout = 'B'
           THEN -a3.inoutmoney END) AS 后端净入金
   FROM silver_consult.tb_crm_transfer_record@consul_std a1
     JOIN info_silver.dw_user_account a2
       ON a1.user_id = a2.crm_user_id
          AND a2.partner_id = 'hht' AND a1.partnerid = 'hht'
          AND a2.group_id IN (1, 7, 8, 111)
     JOIN silver_njs.history_transfer@silver_std a3
       ON a2.firm_id = a3.firmid
          AND a3.realdate > a1.submit_time
     where a1.valid =1 and a1.process in (5,6)
   GROUP BY a1.user_id) aa
    ON a.user_id = aa.user_id
  LEFT JOIN
  (SELECT
     a1.user_id,
     sum(a3.contqty) AS 后端交易额
   FROM silver_consult.tb_crm_transfer_record@consul_std a1
     JOIN info_silver.dw_user_account a2
       ON a1.user_id = a2.crm_user_id
          AND a2.partner_id = 'hht' AND a1.partnerid = 'hht'
          AND a2.group_id IN (1, 7, 8, 111)
     JOIN info_silver.ods_history_deal a3
       ON a2.firm_id = a3.firmid
          AND a3.trade_time > a1.submit_time
     where a1.valid =1 and a1.process in (5,6)
   GROUP BY a1.user_id) bb
    ON a.user_id = bb.user_id
  LEFT JOIN
  info_silver.ods_order_zcmoney cc
    ON b.firm_id = cc.firm_id
       AND cc.fdate = to_char(sysdate - 1, 'yyyymmdd')
  LEFT JOIN
  (SELECT
     a1.user_id,
     sum(a3.trade_price * a3.weight) * 0.00065 AS 后端手续费
   FROM silver_consult.tb_crm_transfer_record@consul_std a1
     JOIN info_silver.dw_user_account a2
       ON a1.user_id = a2.crm_user_id
          AND a2.partner_id = 'hht' AND a1.partnerid = 'hht'
          AND a2.group_id IN (1, 7, 8, 111)
     JOIN info_silver.tb_nsip_t_filled_order a3
       ON a2.firm_id = a3.trader_id
          AND a3.trade_time > a1.submit_time
     where a1.valid =1 and a1.process in (5,6)
   GROUP BY a1.user_id) dd
    ON a.user_id = dd.user_id
  LEFT JOIN
  (SELECT
     a1.user_id,
     sum(CASE WHEN a3.type = 7
       THEN a3.amount END)  AS 后端滞纳金,
     sum(CASE WHEN a3.type IN (5, 6)
       THEN -a3.amount END) AS 后端头寸加点差
   FROM silver_consult.tb_crm_transfer_record@consul_std a1
     JOIN info_silver.dw_user_account a2
       ON a1.user_id = a2.crm_user_id
          AND a2.partner_id = 'hht' AND a1.partnerid = 'hht'
          AND a2.group_id IN (1, 7, 8, 111)
     JOIN NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT a3
       ON a2.firm_id = a3.fund_id
          AND a3.create_time > a1.submit_time
     where a1.valid =1 and a1.process in (5,6)
   GROUP BY a1.user_id) ee
  on a.user_id = ee.user_id
WHERE to_char(a.submit_time, 'yyyymmdd') >= '20170424'
and a.process in (5,6) and a.valid = 1


SELECT *
FROM silver_consult.tb_crm_transfer_record@consul_std
WHERE to_char(submit_time, 'yyyymmdd') >= 20170424

