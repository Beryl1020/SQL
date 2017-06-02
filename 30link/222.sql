SELECT *
FROM info_silver.rpt_crm_transfer_user_stat
WHERE to_char(stat_date, 'yyyymmdd') <
      to_char(submit_time, 'yyyymmdd')


SELECT *
FROM info_silver.ods_history_deal


SELECT to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm')
FROM dual


SELECT *
FROM info_silver.tb_crm_ia
WHERE status = 1 AND group_id = 2

------------3月流转5月交易额
SELECT
  b.ia_id,
  sum(c.contqty)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.dw_user_account b
    ON a.user_id = b.crm_user_id AND b.partneR_id IN ('pmec', 'hht')
  JOIN info_silver.ods_history_deal c
    ON b.firm_id = c.firmid
WHERE to_Char(a.submit_time, 'yyyymm') = '201705'
      AND substr(c.fdate, 1, 6) = '201705'
      AND a.process IN (5, 6) AND a.valid = 1
      AND b.group_id IN (1, 7, 8, 111, 118)
GROUP BY b.ia_id


SELECT *
FROM info_silver.ods_history_deal
WHERE firmid IN ('163170424590176', '163000000017708')
      AND fdate BETWEEN 20170501 AND 20170531
SELECT crm_user_id
FROM info_silver.dw_user_account
WHERE useR_id = '207786522'


SELECT *
FROM info_silver.rpt_crm_transfer_user_stat
WHERE main_userid = 207786522

SELECT *
FROM info_silver.ods_crm_transfer_record
WHERE user_id = 207786522

SELECT *
FROM info_silver.ods_order_zcmoney
WHERE fdate = 20170428 AND partner_id IN ('pmec')


SELECT
  aa.ia_id,
  aa.ia_name,
  aa.group_id,
  sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm')
    THEN aa.activemoney END) AS 上上月激活资金,
  sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -1), 'mm'), 'yyyymm')
    THEN aa.activemoney END) AS 上月激活资金,
  sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
    THEN aa.activemoney END) AS 本月激活资金
FROM
  (SELECT
     dw.ia_id,
     dw.ia_name,
     dw.group_id,
     a.user_id,
     max(a.submit_time)                                       AS submit_time,
     max(CASE WHEN a.partner_id = 'hht'
       THEN a.hht_net_in_sub + a.hht_net_value_sub
         WHEN a.partner_id = 'pmec'
           THEN a.pmec_net_in_sub + a.pmec_net_value_sub END) AS activemoney
   FROM info_silver.ods_crm_transfer_record a
     JOIN (SELECT
             crm_user_id,
             partner_id,
             group_id,
             ia_name,
             ia_id
           FROM info_silver.dw_user_account
           WHERE group_id IN (1, 7, 8, 111, 118)) dw
       ON a.user_id = dw.crm_user_id
   WHERE
     to_char(a.submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm') AND to_char(
         sysdate - 1, 'yyyymm')
     AND a.PROCESS IN (5, 6) AND a.valid = 1
   GROUP BY dw.ia_id, dw.ia_name, dw.group_id, a.user_id) aa
GROUP BY aa.ia_id, aa.ia_name, aa.group_id


SELECT
  b.group_id,
  sum(c.net_zcmoney)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.dw_user_account b
    ON a.user_id = b.crm_user_id
       AND b.partner_id = 'pmec'
  JOIN info_silver.ods_order_zcmoney c
    ON b.firm_id = c.firm_id
       AND c.fdate = '20170421'
WHERE a.valid = 1 AND a.process IN (5, 6) AND to_char(a.submit_time, 'yyyymm') = 201704
GROUP BY b.group_id


SELECT
  b.group_id,
  sum(c.net_assets)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.dw_user_account b
    ON a.user_id = b.crm_user_id
       AND b.partner_id = 'hht'
  JOIN silver_njs.tb_silver_data_center@silver_std c
    ON b.firm_id = c.firmid
       AND c.hdate = '20170531'
WHERE a.valid = 1 AND a.process IN (5, 6) AND b.group_id IN (1, 7, 8, 111, 118)
      AND to_char(a.submit_time, 'yyyymm') = 201703
GROUP BY b.group_id


SELECT *
FROM info_silver.ods_history_deal
WHERE fdate = '20170531'


SELECT
  '点差'                                               AS type1,
  deal.fdate                                         AS date1,
  round(sum(CASE
            WHEN deal.wareid = 'LSAG100g'
              THEN deal.contnum * 8 END) / 10000, 2) AS money -- 点差
FROM info_silver.ods_history_deal deal
WHERE deal.partner_id = 'hht'
      AND ((deal.operation_src = 'open' AND buyorsal = 'B') OR (deal.operation_src = 'close' AND buyorsal = 'S'))
      --AND substr(deal.fdate, 1, 6) = to_char(SYSDATE - 1, 'yyyymm')
      AND deal.fdate BETWEEN '20170525' AND '20170531'
GROUP BY deal.fdate

SELECT
  '点差'                                               AS type1,
  deal.fdate                                         AS date1,
  round(sum(CASE
            WHEN deal.wareid = 'LSAG100g'
              THEN deal.contnum * 8 END) / 10000, 2) AS money -- 点差
FROM info_silver.ods_history_deal deal
WHERE deal.partner_id = 'hht' AND buyorsal = 'B'
      --AND ((deal.operation_src = 'open' AND buyorsal = 'B') OR (deal.operation_src = 'close' AND buyorsal = 'S'))
      --AND substr(deal.fdate, 1, 6) = to_char(SYSDATE - 1, 'yyyymm')
      AND deal.fdate BETWEEN '20170525' AND '20170531'
GROUP BY deal.fdate


SELECT sum(weight)
FROM NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE
WHERE --((position_direction=1 and position_operation=0) or (position_direction=2 and position_operation=1))
  position_operation = 0


SELECT
  -- 后端投顾维护用户净入金
  3                                       AS ID,
  sum(CASE WHEN inout.inorout = 'A'
    THEN inout.inoutmoney
      WHEN inout.inorout = 'B'
        THEN inout.inoutmoney * (-1) END) AS 后端用户净入金
FROM silver_njs.history_transfer@silver_std inout
  JOIN info_silver.dw_user_account dw
    ON inout.firmid = dw.firm_id
  JOIN info_silver.ods_crm_transfer_record trans
    ON dw.crm_user_id = trans.user_id
WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
      AND trans.process IN (5, 6) AND trans.valid = 1
      AND inout.partnerid IN ('hht')
      AND inout.fdate BETWEEN 20170501 AND 20170531
      AND inout.realdate >= trans.submit_time


SELECT *
FROM NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE
WHERE position_operation = 3


SELECT *
FROM info_silver.ods_history_deal
WHERE


SELECT
  sum(CASE WHEN partneR_id = 'hht'
    THEN trans.hht_net_value_sub + trans.hht_net_in_sub
      WHEN partner_id = 'pmec'
        THEN pmec_net_value_sub + pmec_net_in_sub END) AS 激活资金,
  count(DISTINCT trans.firm_id)                        AS 流转单数
FROM info_silver.ods_crm_transfer_record trans
WHERE to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170501 AND 20170531
      AND trans.process IN (5, 6) AND trans.valid = 1
      AND trans.fgroup_id IN (112, 113, 114);


SELECT
  to_char(flow.fdate, 'yyyymmdd'),
  sum(CASE WHEN flow.changetype IN (1, 3)
    THEN (-1) * flow.amount END) AS money -- 头寸+点差
FROM silver_njs.pmec_zj_flow@silver_std flow
WHERE to_char(flow.fdate, 'yyyymmdd') BETWEEN 20170501 AND 20170531
GROUP BY to_char(flow.fdate, 'yyyymmdd')

SELECT
  to_char(flow.fdate, 'yyyymmdd'),
  sum(CASE WHEN flow.changetype = 8
    THEN (-1) * flow.AMOUNT END) AS money -- 滞纳金
FROM silver_njs.pmec_zj_flow@silver_std flow
WHERE to_char(flow.fdate, 'yyyymmdd') BETWEEN 20170501 AND 20170531
GROUP BY to_char(flow.fdate, 'yyyymmdd')

SELECT
  deal.fdate,
  sum(deal.CONTQTY) * 0.00065 AS money -- 总交易额
FROM info_silver.ods_history_deal deal
WHERE deal.partner_id = 'pmec' AND deal.fdate BETWEEN 20170501 AND 20170531
GROUP BY deal.fdate


SELECT sum(c.contqty)
FROM info_silver.ods_crm_transfer_record a
  JOIN info_silver.dw_user_account b
    ON a.user_id = b.crm_user_id AND b.partner_id IN ('pmec', 'hht')
  JOIN info_silver.ods_history_deal c
    ON b.firm_id = c.firmid
WHERE --to_char(a.submit_time,'yyyymm') >= 201703 and
  substr(fdate, 1, 6) = '201705'
  AND c.trade_time > a.submit_time


SELECT count(firmid)
FROM info_silver.ods_history_deal
WHERE substr(fdate, 1, 6) = '201705' AND partner_id = 'njs'


SELECT
  round(count(DISTINCT CASE WHEN b.submit_time BETWEEN a.create_time AND a.create_time + 7
    THEN b.user_id END)/count(distinct a.id),5) AS 七天开单率,
  round(count(DISTINCT CASE WHEN b.submit_time BETWEEN a.create_time AND a.create_time + 14
    THEN b.user_id END)/count(distinct a.id),5) AS 十四天开单率
FROM info_silver.tb_crm_user a
  left JOIN info_silver.ods_crm_transfer_record b
    ON a.id = b.user_id
WHERE to_char(a.create_time, 'yyyymmdd') BETWEEN 20170520 AND 20170526
and fgroup_id in (2,3,4,5,6,9,10,11,12,105,106,116,117)





SELECT
  round(count(DISTINCT CASE WHEN fgroup_id in (2,3,4,5,6,9,10,11,12,105,106,116,117)
    THEN b.user_id END)/count(distinct a.id),5) AS 七天开单率
FROM info_silver.tb_crm_user a
  left JOIN info_silver.ods_crm_transfer_record b
    ON a.id = b.user_id
  and  b.submit_time BETWEEN a.create_time AND a.create_time + 7
WHERE to_char(a.create_time, 'yyyymmdd') BETWEEN 20170429 AND 20170505



SELECT
  round(count(DISTINCT CASE WHEN b.fgroup_id in (2,3,4,5,6,9,10,11,12,105,106,116,117)
    THEN b.user_id END)/count(distinct a.id),5) AS 十四天开单率
FROM info_silver.tb_crm_user a
  left JOIN info_silver.ods_crm_transfer_record b
    ON a.id = b.user_id
  and  b.submit_time BETWEEN a.create_time AND a.create_time + 14
WHERE to_char(a.create_time, 'yyyymmdd') BETWEEN 20170429 AND 20170505



SELECT
  count(DISTINCT CASE WHEN b.fgroup_id in (2,3,4,5,6,9,10,11,12,105,106,116,117)
    THEN b.user_id END) AS 十四天开单率,count(distinct a.id)
FROM info_silver.tb_crm_user a
  left JOIN info_silver.ods_crm_transfer_record b
    ON a.id = b.user_id
  and  b.submit_time BETWEEN a.create_time AND a.create_time + 14
WHERE to_char(a.create_time, 'yyyymmdd') BETWEEN 20170506 AND 20170512


select * from
(select * from silver_consult.v_tb_crm_user@consul_std
where to_char(create_time,'yyyymmdd') between 20170429 and 20170505) a left join
silver_consult.tb_crm_transfer_record@consul_std   b
on a.id=b.user_id and b.valid=1 and process in (5,6) and b.submit_time -a.create_time<14
where submit_time is not null






 SELECT
         sum(case when trans.cur_bgroup_id in (1,7,8,118) then deal.contqty end)   AS 电销龙商交易额,
         sum(case when trans.cur_bgroup_id in (111) then deal.contqty end) as 微销龙商交易额
       FROM info_silver.ods_history_deal deal
         JOIN info_silver.ods_crm_transfer_record trans
           ON deal.firmid = trans.firm_id
       WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
         and to_char(trans.submit_time,'yyyymmdd') <=20170526
             AND deal.fdate BETWEEN 20170520 AND 20170526
             AND deal.trade_time > trans.submit_time
             AND trans.process IN (5, 6) AND trans.valid = 1
             AND deal.partner_id = 'hht'

union all


SELECT
         sum(case when trans.cur_bgroup_id in (1,7,8,118) then deal.contqty end)   AS 电销新用户龙商交易额,
         sum(case when trans.cur_bgroup_id in (111) then deal.contqty end) as 微销新用户龙商交易额
       FROM info_silver.ods_history_deal deal
         JOIN info_silver.ods_crm_transfer_record trans
           ON deal.firmid = trans.firm_id
       WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
         and to_char(trans.submit_time,'yyyymmdd') between 20170520 and 20170526
             AND deal.fdate BETWEEN 20170520 AND 20170526
             AND deal.trade_time > trans.submit_time
             AND trans.process IN (5, 6) AND trans.valid = 1
             AND deal.partner_id = 'hht'

union all


SELECT
         sum(case when trans.cur_bgroup_id in (1,7,8,118) then zc.net_assets end)   AS 电销龙商净资产,
         sum(case when trans.cur_bgroup_id in (111) then zc.net_assets end) as 微销龙商净资产
       FROM silver_njs.tb_silver_data_center@silver_std zc
         JOIN info_silver.ods_crm_transfer_record trans
           ON zc.firmid = trans.firm_id
       WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
         and to_char(trans.submit_time,'yyyymmdd') <=20170526
             AND zc.hdate BETWEEN 20170520 AND 20170526
             AND trans.process IN (5, 6) AND trans.valid = 1
             AND zc.partner_id = 'hht'
and zc.hdate = 20170526

union all

select sum(case when cur_bgroup_id in (1,7,8,118) then hht_net_in_sub+hht_net_value_sub end) as 电销激活资金,
  sum(case when cur_bgroup_id in (111) then hht_net_in_sub+hht_net_value_sub end) as 微销激活资金
from info_silver.ods_crm_transfer_record where process  in (5,6) and valid=1
and to_char(submit_time,'yyyymmdd') between 20170520 and 20170526

union ALL
     SELECT round(sum(CASE WHEN aa.cur_bgroup_id IN (1, 7, 8, 118)
  THEN aa.首笔交易额 end) / sum(CASE WHEN aa.cur_bgroup_id IN (1, 7, 8, 118)
  THEN aa.激活资金 end)/12.5,4) as 电销资金利用率,
  round(sum(CASE WHEN aa.cur_bgroup_id IN (111)
  THEN aa.首笔交易额 end) / sum(CASE WHEN aa.cur_bgroup_id IN (111)
  THEN aa.激活资金 END)/12.5,4)  as 微销资金利用率

                       FROM
                       (SELECT
                          a.bia_id,
                          a.bname,
                          a.cur_bgroup_id,
                          a.firm_id,
                          a.user_id,
                          a.crm_name,
                          avg(a.hht_net_value_sub + a.hht_net_in_sub) AS 激活资金,
                          sum(e.contqty)                              AS 首笔交易额,
                          e.trade_time                                AS 首笔交易时间
                        FROM info_silver.ods_crm_transfer_record a
                          JOIN
                          (SELECT
                             b.firmid,
                             min(b.trade_time) AS mintime
                           FROM info_silver.ods_history_deal b
                             JOIN info_silver.ods_crm_transfer_record c
                               ON b.firmid = c.firm_id AND b.trade_time > c.submit_time
                           WHERE c.process IN (5, 6) AND c.valid = 1 AND b.operation_src = 'open'
                           GROUP BY b.firmid) d
                            ON A.firm_id = d.firmid
                          JOIN info_silver.ods_history_deal e ON d.firmid = e.firmid AND d.mintime = e.trade_time

                        WHERE A.process IN (5, 6) AND A.valid = 1 AND
                              to_char(submit_time, 'yyyymmdd') BETWEEN 20170520 AND 20170526
                              AND a.cur_bgroup_id IN (1, 7, 8, 118, 111)
                        GROUP BY a.bia_id,
                          a.bname,
                          a.cur_bgroup_id,
                          a.firm_id,
                          a.user_id,
                          a.crm_name, e.trade_time) aa




SELECT
  '新增资源数',
  count(DISTINCT b1.user_id)
FROM

  (SELECT
     a1.user_id,
     a1.firsttime
   FROM
     (SELECT
        dis.user_id,
        min(dis.create_time) AS firsttime
      FROM silver_consult.tb_crm_dispatch_his@consul_std dis
      WHERE dis.ia_id IS NOT NULL
      GROUP BY dis.user_id) a1
   WHERE to_char(a1.firsttime, 'yyyymmdd') BETWEEN 20170506 AND 20170512) b1

  JOIN silver_consult.tb_crm_dispatch_his@consul_std dis ON b1.user_id = dis.user_id AND b1.firsttime = dis.create_time
  JOIN silver_consult.tb_crm_ia@consul_std ia ON dis.ia_id = ia.id
  JOIN silver_consult.v_tb_crm_user@consul_std user1 ON b1.user_id = user1.id AND user1.id IS NOT NULL

WHERE ia.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 116, 117) -- 新增资源数



select add_months(trunc(sysdate, 'mm'), 1) from dual