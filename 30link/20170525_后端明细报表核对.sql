--pmec + hht 手续费
SELECT
  a.crm_userid,
  to_char(a.stat_date, 'yyyymmdd'),
  sum(a.tmpmoney),
  sum(a.contqty)
FROM info_silver.rpt_crm_transfer_user_stat a
  JOIN info_silver.ods_crm_transfer_record c ON a.crm_userid = c.user_id
                                                AND c.process IN (5, 6) AND c.valid = 1
WHERE to_char(a.stat_date, 'yyyymmdd') BETWEEN 20170505 AND 20170505
      AND a.group_id IN (1, 7, 8, 111, 118)
      AND to_char(a.stat_date, 'yyyymmdd') >= to_char(a.submit_time, 'yyyymmdd')
GROUP BY to_char(a.stat_date, 'yyyymmdd'), a.crm_userid


SELECT *
FROM info_silver.rpt_crm_transfer_user_stat
WHERE crm_userid = 140296949 AND to_char(stat_date, 'yyyymmdd') = 20170505


SELECT *
FROM info_silver.ods_crm_transfer_record
WHERE user_id = 1000126948

SELECT
  *
FROM info_silver.ods_history_deal
WHERE partner_id IN ('pmec', 'hht') AND fdate = '20170505' AND firmid = '163000000011736'


--pmec手续费
SELECT
  b.crm_user_id,
  a.fdate,
  sum(a.contnum)
FROM info_silver.dw_user_account b
  LEFT JOIN (SELECT
               firmid,
               (CASE WHEN wareid = 'GDAG'
                 THEN contnum
                WHEN wareid = 'GDPD'
                  THEN contnum *30
                WHEN wareid = 'GDPT'
                  THEN contnum * 65
                WHEN wareid = 'LSAG100g'
                  THEN contnum END) AS contnum,
               fdate,
               trade_time
             FROM info_silver.ods_history_deal
             WHERE fdate BETWEEN '20170505' AND '20170505' AND partner_id IN ('pmec', 'hht')) a
    ON a.firmid = b.firm_id
  JOIN info_silver.ods_crm_transfer_record c
    ON b.crm_user_id = c.user_id
WHERE a.fdate BETWEEN 20170505 AND 20170505
      AND b.group_id IN (1, 7, 8, 111, 118)
      AND a.trade_time > c.submit_time AND c.process IN (5, 6) AND c.valid = 1
GROUP BY
  a.fdate, b.crm_user_id



--pmec+hht净入金
SELECT
  a.user_id,
  c.fdate,
  sum(CASE WHEN c.inorout = 'A'
    THEN inoutmoney
      WHEN c.inorout = 'B'
        THEN -inoutmoney END) AS 本月流转后净入金
FROM silver_njs.history_transfer@silver_std c
  JOIN info_silver.dw_user_account b
    ON c.firmid = b.firm_id
       AND b.partner_id IN ('hht', 'pmec')
  JOIN info_silver.ods_crm_transfer_record a
    ON b.crm_user_id = a.user_id
WHERE substr(c.fdate, 1, 6) = to_char(sysdate - 1, 'yyyymm')
      AND c.fdate = 20170524
      AND b.group_id IN (1, 7, 8, 111, 118)
      AND to_char(a.submit_time, 'yyyymmdd') BETWEEN 20170501 AND 20170524
      AND a.submit_time < c.realdate AND a.process IN (5, 6) AND a.valid = 1
GROUP BY c.fdate, a.user_id


SELECT

  to_char(a.stat_date, 'yyyymmdd'),
  sum(nvl(a.inmoney, 0) - nvl(a.outmoney, 0))
FROM info_silver.rpt_crm_transfer_user_stat a
  JOIN info_silver.ods_crm_transfer_record c ON a.crm_userid = c.user_id
                                                AND c.process IN (5, 6) AND c.valid = 1
WHERE to_char(a.stat_date, 'yyyymmdd') = 20170524
      AND a.group_id IN (1, 7, 8, 111, 118)
      AND to_char(a.stat_date, 'yyyymmdd') >= to_char(a.submit_time, 'yyyymmdd')
      AND to_char(a.submit_time, 'yyyymmdd') BETWEEN 20170501 AND 20170524
GROUP BY to_char(a.stat_date, 'yyyymmdd')


-- 净入金字段错误
SELECT *
FROM silver_njs.history_transfer@silver_std
WHERE fdate = 20170524 AND firmid = 163170426145970

SELECT *
FROM info_silver.rpt_crm_transfer_user_stat
WHERE crm_userid = 1000587375 AND to_char(stat_date, 'yyyymmdd') = 20170524


SELECT
  to_char(stat_date, 'yyyymmdd'),
  sum(tmpmoney),
  sum(contqty)
FROM info_silver.rpt_crm_transfer_user_stat
GROUP BY to_char(stat_date, 'yyyymmdd')


SELECT
  a.crm_userid,
  to_char(a.stat_date, 'yyyymmdd'),
  sum(a.netzcmoney)
FROM info_silver.rpt_crm_transfer_user_stat a
  JOIN info_silver.ods_crm_transfer_record c ON a.crm_userid = c.user_id
                                                AND c.process IN (5, 6) AND c.valid = 1
WHERE to_char(a.stat_date, 'yyyymmdd') BETWEEN 20170524 AND 20170524
      AND a.group_id IN (1, 7, 8, 111, 118)
      AND to_char(a.stat_date, 'yyyymmdd') >= to_char(a.submit_time, 'yyyymmdd')

GROUP BY a.crm_userid, to_char(a.stat_date, 'yyyymmdd')

SELECT *
FROM info_silver.ods_order_zcmoney
WHERE firm_id = '163000000153042'
SELECT *
FROM info_silver.dw_user_account
WHERE crm_user_id = 1000293005
SELECT *
FROM info_silver.rpt_crm_transfer_user_stat


SELECT
  a.crm_user_id,
  max(CASE WHEN b.partner_id = 'pmec'
    THEN b.pmec_net_in_sub + b.pmec_net_value_sub
      WHEN b.partner_id = 'hht'
        THEN b.hht_net_in_sub + b.hht_net_value_sub END)
FROM info_silver.dw_user_account a
  LEFT JOIN info_silver.ods_crm_transfer_record b
    ON a.crm_user_id = b.user_id
WHERE a.group_id IN (1, 7, 8, 118, 111)
GROUP BY a.crm_user_id


SELECT *
FROM info_silver.rpt_crm_transfer_user_stat
WHERE trunc(stat_date) = trunc(sysdate - 1) AND group_id IN (1, 7, 8, 111, 118)



SELECT
  b.crm_user_id,
  sum(c.net_zcmoney) AS 昨日净资产
FROM info_silver.dw_user_account b
  JOIN info_silver.ods_order_zcmoney c
    ON b.firm_id = c.firm_id
       AND b.partner_id = 'hht'
  JOIN info_silver.ods_crm_transfer_record a
    ON a.firm_id = b.firm_id
WHERE b.group_id IN (1, 7, 8, 111, 118)
      AND c.fdate = to_char(sysdate - 1, 'yyyymmdd') AND a.process IN (5, 6) AND a.valid = 1
GROUP BY b.crm_user_id


SELECT *
FROM info_silver.tb_nsip_t_filled_order
WHERE trader_id = 163170505380209

SELECT *
FROM info_silver.rpt_crm_transfer_user_stat

SELECT
  user_id,
  firm_id,
  partner_id,
  crm_user_id,
  ia_id,
  group_id,
  ia_name
FROM info_silver.dw_user_account
WHERE real_name = '顾清波'

SELECT *
FROM info_silver.ods_crm_transfer_record
WHERE user_id = 1000587375


-- deal 流转后点差
SELECT deal.fdate,dw.crm_user_id,
     sum(CASE
         WHEN deal.wareid = 'LSAG100g'
           THEN deal.contnum * 8 END) AS money
   FROM info_silver.ods_history_deal deal
     join info_silver.dw_user_account dw
     on deal.firmid = dw.firm_id
     join info_silver.ods_crm_transfer_record trans
     on dw.crm_user_id = trans.user_id
   WHERE deal.partner_id = 'hht'
         AND buyorsal = 'B'
         AND deal.fdate BETWEEN '20170515' AND '20170515' and trans.user_id = 1000159715
   and deal.trade_time>trans.submit_time and trans.valid = 1 and trans.process in(5,6)
and dw.group_id in (1,7,8,111,118)
group by deal.fdate,dw.crm_user_id

-- flow 流转后头寸+点差
SELECT to_char(flow.create_time-0.25,'yyyymmdd'),dw.crm_user_id,
     sum(CASE WHEN flow.type IN (5,6)
       THEN  flow.amount*(-1) END) AS money
   FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT flow
     join info_silver.dw_user_account dw
     on flow.fund_id = dw.firm_id
     join info_silver.ods_crm_transfer_record trans
     on dw.crm_user_id = trans.user_id
   WHERE to_char(flow.create_time - 0.25, 'yyyymmdd') BETWEEN 20170515 AND 20170515
     and trans.submit_time < flow.create_time and trans.user_id = 1000159715
and dw.group_id in (1,7,8,111,118) and trans.valid = 1 and trans.process in(5,6)
group by to_char(flow.create_time-0.25,'yyyymmdd'),dw.crm_user_id



SELECT
  to_char(a.stat_date, 'yyyymmdd'),
  sum(a.inmoney-a.outmoney) as 点差
FROM info_silver.rpt_crm_transfer_user_stat a
  JOIN info_silver.ods_crm_transfer_record c ON a.crm_userid = c.user_id
                                                AND c.process IN (5, 6) AND c.valid = 1
WHERE to_char(a.stat_date, 'yyyymmdd') BETWEEN 20170424 AND 20170525
      AND a.group_id IN (1, 7, 8, 111, 118)
      AND to_char(a.stat_date, 'yyyymmdd') >= to_char(a.submit_time, 'yyyymmdd')
GROUP BY to_char(a.stat_date, 'yyyymmdd')


select * from  info_silver.rpt_crm_transfer_user_stat where crm_userid = 1000476613
select * from info_silver.ods_history_deal where firmid =  '163170424087027' and fdate = 20170525
select * from NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT where fund_id = '163170424087027'
select * from info_silver.dw_user_account where crm_user_id =  '1000476613'
select * from NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE where trader_id = '163170424087027'



--tel 通话时长
select to_char(c.create_time,'yyyymmdd'),b.crm_user_id, sum(worksec) from info_silver.ods_crm_transfer_record a
  join info_silver.dw_user_account b
  on a.user_id = b.crm_user_id
  join info_silver.tb_crm_tel_record  c
  on a.user_id = c.user_id
where a.process in (5,6) and a.valid = 1
and b.group_id in (1,7,8,111,118)
and c.create_time>a.submit_time
and to_char(c.create_time,'yyyymmdd') between 20170501 and 20170525
group by to_char(c.create_time,'yyyymmdd'),b.crm_user_id



--rpt 通话时长
SELECT
  to_char(a.stat_date, 'yyyymmdd'),a.crm_userid,
  sum(a.telsec) as worksec
FROM info_silver.rpt_crm_transfer_user_stat a
  JOIN info_silver.ods_crm_transfer_record c ON a.crm_userid = c.user_id
                                                AND c.process IN (5, 6) AND c.valid = 1
WHERE to_char(a.stat_date, 'yyyymmdd') BETWEEN 20170501 AND 20170525
      AND a.group_id IN (1, 7, 8, 111, 118)
      AND to_char(a.stat_date, 'yyyymmdd') >= to_char(a.submit_time, 'yyyymmdd')
GROUP BY to_char(a.stat_date, 'yyyymmdd'),a.crm_userid

select * from info_silver.rpt_crm_transfer_user_stat where crm_userid = 1000159715

select * from info_silver.rpt_crm_transfer_user_stat where stat_date= to_date('20170515','yyyymmdd') and crm_userid = 1000159715

select * from info_silver.tb_crm_tel_record where user_id=1000159715 and to_char(create_time, 'yyyymmdd')=20170515

select * from info_silver.dw_user_account where crm_user_id = 1000159715

select * from info_silver.ods_crm_transfer_record where user_id =1000159715

SELECT *
FROM silver_njs.tb_silver_data_center@silver_std  where firmid = 163170424220318 and hdate ='20170515'
SELECT *
FROM silver_njs.history_transfer@silver_std  where firmid =
select * from info_silver.ods_history_deal where firmid ='163170424557623' and fdate ='20170515'

select * from info_silver.ods_crm_tel_record where user_id=1000159715 and to_char(create_time, 'yyyymmdd')=20170515



SELECT *
FROM info_silver.rpt_crm_transfer_user_stat
WHERE trunc(stat_date) = trunc(sysdate - 1) AND group_id IN (1, 7, 8, 111, 118)
