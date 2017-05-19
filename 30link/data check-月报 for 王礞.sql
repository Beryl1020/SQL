SELECT
  sum(CASE WHEN PARTNER_ID = 'njs' AND fdate BETWEEN 20170401 AND 20170430
    THEN CONTQTY END),
  --pmec/njs/平台交易额
  sum(CASE WHEN PARTNER_ID = 'pmec' AND fdate BETWEEN 20170401 AND 20170430
    THEN CONTQTY END),
  sum(CASE WHEN PARTNER_ID = 'hht' AND fdate BETWEEN 20170401 AND 20170430
    THEN CONTQTY END),
  sum(CASE WHEN PARTNER_ID = 'sge' AND fdate BETWEEN 20170401 AND 20170430
    THEN CONTQTY END),
  sum(CASE WHEN fdate BETWEEN 20170401 AND 20170430
    THEN CONTQTY END)
FROM info_silver.ods_history_deal

SELECT *
FROM info_silver.ods_crm_transfer_record
WHERE to_char(submit_time, 'yyyymmdd') >= 20170426
SELECT *
FROM tb_silver_user_stat@silver_std
WHERE firm_id = 163000000434471
SELECT *
FROM silver_consult.tb_crm_transfer_record@consul_std trans
WHERE to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170401 AND 20170431
      AND trans.process IN (5, 6) AND trans.valid = 1 AND bia_group_id = 115


SELECT
  sum(CASE WHEN partnerid = 'pmec'
    THEN trans.pmec_net_value_sub + trans.pmec_net_in_sub
      WHEN partnerid = 'hht'
        THEN trans.hht_net_value_sub + trans.hht_net_in_sub END) AS 激活资金,
  count(DISTINCT trans.user_id)                                  AS 流转单数,
  count(DISTINCT trans.fia_id)
FROM silver_consult.tb_crm_transfer_record@consul_std trans
WHERE to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170401 AND 20170431
      AND trans.process IN (5, 6) AND trans.valid = 1
      AND bia_group_id IN (111)


select * from silver_consult.tb_crm_transfer_record@consul_std  where user_id=1000555208
select * from  info_silver.ods_crm_transfer_record where user_id=1000555208


SELECT
  '微销前端开单',
  count(DISTINCT user_id),
  sum(pmec_net_value_sub + pmec_net_in_sub)
FROM info_silver.ods_crm_transfer_record
WHERE process IN (5, 6) AND valid = 1
      AND to_char(submit_time, 'yyyymmdd') BETWEEN 20170101 AND 20170131
      AND fgroup_id IN
          (112, 113, 114, 106)                                                                                  --微销前端开单

SELECT
  2                                 AS subid,
  sum(CASE
      WHEN io.inorout = 'A' AND io.partnerid = 'hht'
        THEN inoutmoney
      WHEN io.inorout = 'B' AND io.partnerid = 'hht'
        THEN (-1) * inoutmoney END) AS moneypmec,
  sum(CASE WHEN io.inorout = 'A'
    THEN inoutmoney
      WHEN io.inorout = 'B'
        THEN (-1) * inoutmoney END) AS money --pmec、平台净入金
FROM silver_njs.history_transfer@silver_std io
WHERE io.fdate BETWEEN 20170401 AND 20170431
GROUP BY 2


SELECT
  '微销前端净入金',
  sum(CASE WHEN partnerid = 'pmec'
    THEN trans.pmec_net_value_sub + trans.pmec_net_in_sub
      WHEN partnerid = 'hht'
        THEN trans.hht_net_value_sub + trans.hht_net_in_sub END)
FROM silver_consult.tb_crm_transfer_record@consul_std trans
WHERE process IN (5, 6) AND valid = 1
      AND to_char(submit_time, 'yyyymmdd') BETWEEN 20170401 AND 20170431
      AND bia_group_id IN (1, 7, 8)


SELECT
  '电销前端净入金',
  sum(pmec_net_value_sub + pmec_net_in_sub)
FROM info_silver.ods_crm_transfer_record
WHERE process IN (5, 6) AND valid = 1
      AND to_char(submit_time, 'yyyymmdd') BETWEEN 20170401 AND 20170431
      AND fgroup_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105)


SELECT sum(CASE WHEN io.inorout = 'A'
  THEN inoutmoney
           WHEN io.inorout = 'B'
             THEN (-1) * inoutmoney END) AS money
FROM silver_njs.history_transfer@silver_std io
WHERE io.fdate BETWEEN 20170401 AND 20170431
      AND io.partnerid = 'pmec'


SELECT *
FROM silver_consult.tb_crm_transfer_record@consul_std

SELECT sum(CASE WHEN io.inorout = 'A'
  THEN inoutmoney
           WHEN io.inorout = 'B'
             THEN (-1) * inoutmoney END) AS money -- 后端电销净入金
FROM silver_consult.tb_crm_transfer_record@consul_std trans
  JOIN info_silver.dw_user_account u
    ON trans.user_id = u.crm_user_id
       AND u.partner_id IN ('pmec', 'hht')
  JOIN silver_njs.history_transfer@silver_std io
    ON u.firm_id = io.firmid
WHERE trans.process IN (5, 6) AND trans.valid = 1
      AND io.fdate BETWEEN 20170401 AND 20170431
      AND trans.bia_group_id IN (111)
      AND io.realdate > trans.submit_time
      AND io.partnerid IN ('pmec', 'hht')


SELECT sum(CASE WHEN io.inorout = 'A'
  THEN inoutmoney
           WHEN io.inorout = 'B'
             THEN (-1) * inoutmoney END) AS money -- 后端微销净入金
FROM info_silver.ods_crm_transfer_record trans
  JOIN silver_njs.history_transfer@silver_std io
    ON trans.firm_id = io.firmid
WHERE trans.process IN (5, 6) AND trans.valid = 1
      AND io.fdate BETWEEN 20170101 AND 20170131
      AND trans.cur_bgroup_id IN (111)
      AND io.realdate > trans.submit_time


SELECT count(DISTINCT fia_id)
FROM info_silver.ods_crm_transfer_record
WHERE process IN (5, 6) AND valid = 1 AND to_char(submit_time, 'yyyymmdd') BETWEEN 20170101 AND 20170131


SELECT count(DISTINCT case when --partner_id ='hht' and
             TO_CHAR(OPEN_ACCOUNT_TIME, 'yyyymmdd')                      --开户用户
             BETWEEN 20170401 AND 20170431 THEN user_id end)
FROM tb_silver_user_stat@silver_std


SELECT count(CASE WHEN aa.pid = 'hht' AND aa.mindate BETWEEN 20170401 AND 20170431
  THEN aa.firmid END) --新入金用户
FROM
  (SELECT
     trans.firmid,
     min(trans.fdate) AS mindate,
     trans.partnerid  AS pid
   FROM silver_njs.history_transfer@silver_std trans
   WHERE inorout = 'A'
   GROUP BY firmid, partnerid) aa


SELECT
  count(DISTINCT CASE WHEN to_char(bbb.date1, 'yyyymmdd') BETWEEN 20170401 AND 20170431
    THEN bbb.id END),
  0
FROM --有效新入金用户
  (SELECT
     aaa.firm_id          id,
     min(aaa.realdate) AS date1
   FROM
     (SELECT
        suba.firmid        AS firm_id,
        suba.realdate      AS realdate,
        sum(subb.summoney) AS money
      FROM
        (SELECT
           firmid,
           realdate
         FROM silver_njs.history_transfer@silver_std
         WHERE partnerid = 'hht') suba
        LEFT JOIN
        (SELECT
           firmid,
           (CASE WHEN inorout = 'A'
             THEN inoutmoney
            WHEN inorout = 'B'
              THEN (-1) * inoutmoney END) AS summoney,
           realdate
         FROM silver_njs.history_transfer@silver_std
         WHERE partnerid = 'hht') subb
          ON suba.firmid = subb.firmid AND suba.realdate >= subb.realdate

      GROUP BY suba.firmid, suba.realdate) aaa
   WHERE aaa.money >= 50
   GROUP BY aaa.firm_id) bbb
GROUP BY 0


SELECT
  --交易用户
  count(DISTINCT CASE
                 WHEN --deal.partner_id = 'pmec' AND
                      deal.fdate BETWEEN 20170401 AND 20170430
                   THEN deal.user_id END) AS cnt1,
  count(DISTINCT CASE
                 WHEN deal.partner_id = 'hht' AND deal.fdate BETWEEN 20170401 AND 20170430
                   THEN deal.firmid END) AS cnt2
FROM info_silver.ods_history_deal deal
WHERE deal.fdate BETWEEN 20170401 AND 20170430
--   and deal.ordersty<>151 --非强平


SELECT
  sub1.money,
  sub2.money,
  sub3.money,
  sub3.money * 0.00065 + sub4.money + sub5.money,
  sub3.money * 0.00065 + sub5.money + sub6.money
FROM

  (SELECT
     1                  AS subid,
     sum(io.inoutmoney) AS money -- 总入金
   FROM silver_njs.history_transfer@silver_std io
   WHERE io.inorout = 'A' AND io.partnerid = 'pmec' AND io.fdate BETWEEN 20170401 AND 20170431
   GROUP BY 1) sub1
  LEFT JOIN
  (SELECT
     2                                 AS subid,
     sum(CASE WHEN io.inorout = 'A'
       THEN inoutmoney
         WHEN io.inorout = 'B'
           THEN (-1) * inoutmoney END) AS money --净入金
   FROM silver_njs.history_transfer@silver_std io
   WHERE io.partnerid = 'pmec' AND io.fdate BETWEEN 20170401 AND 20170431
   GROUP BY 2) sub2
    ON sub1.subid <> sub2.subid
  LEFT JOIN
  (SELECT
     3                 AS subid,
     sum(deal.CONTQTY) AS money -- 总交易额
   FROM info_silver.ods_history_deal deal
   WHERE deal.partner_id = 'pmec' AND deal.fdate BETWEEN 20170401 AND 20170431

   GROUP BY 3) sub3
    ON sub1.subid <> sub3.subid
  LEFT JOIN
  (SELECT
     4                                         AS subid,
     sum(CASE
         WHEN deal.wareid = 'GDAG'
           THEN deal.contnum * 8
         WHEN deal.wareid = 'GDPD'
           THEN deal.contnum * 1000 * 0.48
         WHEN deal.wareid = 'GDPT'
           THEN deal.contnum * 1000 * 0.5 END) AS money -- 点差
   FROM info_silver.ods_history_deal deal
   WHERE deal.partner_id = 'pmec'
         AND deal.operation_src = 'open'
         AND deal.fdate BETWEEN 20170401 AND 20170431
   GROUP BY 4) sub4
    ON sub1.subid <> sub4.subid
  LEFT JOIN
  (SELECT
     5                              AS subid,
     sum(CASE WHEN flow.changetype = 8
       THEN (-1) * flow.AMOUNT END) AS money -- 滞纳金
   FROM silver_njs.pmec_zj_flow@silver_std flow
   WHERE to_char(flow.fdate, 'yyyymmdd') BETWEEN 20170401 AND 20170431
   GROUP BY 5) sub5
    ON sub1.subid <> sub5.subid
  LEFT JOIN
  (SELECT
     6                              AS subid,
     sum(CASE WHEN flow.changetype IN (9, 10)
       THEN (-1) * flow.amount END) AS money -- 头寸+点差
   FROM silver_njs.pmec_zj_flow@silver_std flow
   WHERE to_char(flow.fdate, 'yyyymmdd') BETWEEN 20170401 AND 20170431
   GROUP BY 6) sub6
    ON sub1.subid <> sub6.subid




SELECT
  type,
  sum(amount)
FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT
WHERE to_char(create_time - 0.25, 'yyyymmdd') BETWEEN 20170401 AND 20170430
GROUP BY type
ORDER BY type

-- 7 滞纳金 -5.-6 盈亏 -3.-4手续费↑


SELECT sum(weight) * 8
FROM info_silver.tb_nsip_t_filled_order
WHERE to_char(trade_time - 0.25, 'yyyymmdd') BETWEEN 20170501 AND 20170505 AND
      ((POSITION_DIRECTION = 1 AND POSITION_OPERATION = 0) OR (POSITION_DIRECTION = 2 AND POSITION_OPERATION = 1))


SELECT sum(a.trade_price * a.weight) * 0.00065
FROM info_silver.tb_nsip_t_filled_order a
WHERE to_char(a.trade_Date, 'yyyymmdd') BETWEEN 20170401 AND 20170431

select * from NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT