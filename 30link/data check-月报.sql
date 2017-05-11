SELECT
  partner_id,
  sum(CONTQTY) --各平台交易额
FROM info_silver.ods_history_deal
WHERE fdate BETWEEN 20170401 AND 20170430
GROUP BY partner_id


SELECT
  sum(CASE WHEN aa.fgroup_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105)
    THEN bb.contqty END),
  --电销、微销交易额
  sum(CASE WHEN aa.fgroup_id IN (106, 112, 113, 114)
    THEN bb.contqty END)
FROM
  (SELECT
     firm_id,
     fia_id,
     fgroup_id,
     submit_time
   FROM info_silver.ods_crm_transfer_record
   WHERE to_char(submit_time, 'yyyymmdd') <= 20170430 AND process IN (5, 6) AND valid = 1) aa
  JOIN info_silver.ods_history_deal bb
    ON aa.firm_id = bb.firmid
WHERE bb.fdate BETWEEN 20170401 AND 20170430
      AND aa.submit_time < bb.trade_time



/*
select sum(bb.contqty)
   from
(select firm_id,fia_id,fgroup_id,submit_time from info_silver.ods_crm_transfer_record
where to_char(submit_time,'yyyymmdd') <= 20170331 and process in(5,6) and valid=1) aa
    join info_silver.ods_history_deal bb
  on aa.firm_id=bb.firmid
where bb.fdate between 20170301 and 20170331
and aa.submit_time < bb.trade_time
*/



SELECT
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
      AND deal.fdate BETWEEN 20170401 AND 20170430
GROUP BY 4


SELECT
  5                              AS subid,
  sum(CASE WHEN flow.changetype = 8
    THEN (-1) * flow.AMOUNT END) AS money
FROM silver_njs.pmec_zj_flow@silver_std flow
WHERE to_char(flow.fdate, 'yyyymmdd') BETWEEN 20170401 AND 20170430 -- 滞纳金
GROUP BY 5


SELECT
  6                              AS subid,
  sum(CASE WHEN flow.changetype IN (9, 10)
    THEN (-1) * flow.amount END) AS money -- 头寸+点差
FROM silver_njs.pmec_zj_flow@silver_std flow
WHERE to_char(flow.fdate, 'yyyymmdd') BETWEEN 20170401 AND 20170430
GROUP BY 6

SELECT
  3                           AS subid,
  sum(deal.CONTQTY) * 0.00065 AS money -- 手续费
FROM info_silver.ods_history_deal deal
WHERE deal.partner_id = 'pmec' AND deal.fdate BETWEEN 20170401 AND 20170430
GROUP BY 3


SELECT
  count(DISTINCT c.user_id),
  count(DISTINCT deal.firmid),
  sum(deal.contqty) --5万以上用户交易额
FROM
  info_silver.ods_history_deal deal
  JOIN
  (
    SELECT
      firm_id          AS firmid,
      max(net_zcmoney) AS assets
    FROM info_silver.ods_order_zcmoney
    WHERE fdate BETWEEN 20170401 AND 20170430 AND partner_id IN ('hht', 'pmec') AND net_zcmoney IS NOT NULL
    GROUP BY firm_id
  ) ass
    ON deal.firmid = ass.firmid
  JOIN tb_silver_user_stat@silver_std c ON ass.firmid = c.firm_id
WHERE ass.assets >= 50000
      AND deal.partner_id IN ('hht', 'pmec')
      AND deal.fdate BETWEEN 20170401 AND 20170430


SELECT
  sum(CASE WHEN partnerid = 'njs' AND inorout = 'A'
    THEN inoutmoney ----广贵、南交净入金
      WHEN partnerid = 'njs' AND inorout = 'B'
        THEN (-1) * inoutmoney END),
  sum(CASE WHEN partnerid = 'pmec' AND inorout = 'A'
    THEN inoutmoney
      WHEN partnerid = 'pmec' AND inorout = 'B'
        THEN (-1) * inoutmoney END)
FROM silver_njs.history_transfer@silver_std
WHERE fdate BETWEEN 20170401 AND 20170430


SELECT
  count(DISTINCT aa.user_id),
  count(DISTINCT CASE WHEN aa.partner_id = 'njs'
    THEN aa.firmid END),
  --月新交易用户数
  count(DISTINCT CASE WHEN aa.partner_id = 'pmec'
    THEN aa.firmid END)
FROM
  (
    SELECT
      user_id,
      firmid,
      partner_id,
      MIN(fdate) AS mindate
    FROM info_silver.ods_history_deal
    GROUP BY firmid, partner_id, user_id
  ) aa
  JOIN
  (SELECT
     firmid,
     sum(CASE WHEN inorout = 'A'
       THEN inoutmoney END) AS inoutmoney
   FROM silver_njs.history_transfer@silver_std
   GROUP BY firmid) io
    ON aa.firmid = io.firmid AND io.inoutmoney > 0
WHERE aa.mindate BETWEEN 20170401 AND 20170430


SELECT
  count(DISTINCT user_id),
  count(DISTINCT CASE WHEN partner_id = 'njs'
    THEN firmid END),
  --月交易用户总数

count(DISTINCT CASE WHEN partner_id = 'pmec'
    THEN firmid END),
  count(DISTINCT CASE WHEN partner_id = 'hht'
    THEN firmid END)
FROM
  info_silver.ods_history_deal
WHERE fdate BETWEEN 20170401 AND 20170430


SELECT
  sum(aa.id) / 19,
  sum(aa.idn) / 19,
  sum(aa.idp) / 19,
  sum(aa.idh) / 5 --日均交易用户总数
FROM
  (
    SELECT
      fdate,
      count(DISTINCT user_id) AS id,
      count(DISTINCT CASE WHEN partner_id = 'njs'
        THEN firmid END)      AS idn,
      count(DISTINCT CASE WHEN partner_id = 'pmec'
        THEN firmid END)      AS idp,
      count(DISTINCT CASE WHEN partner_id = 'hht'
        THEN firmid END)      AS idh
    FROM
      info_silver.ods_history_deal
    WHERE fdate BETWEEN 20170401 AND 20170430
    GROUP BY fdate
  ) aa


SELECT
  count(firmid),
  count(CASE WHEN partner_id = 'njs'
    THEN firmid END),
  --月订单总数
  count(CASE WHEN partner_id = 'pmec'
    THEN firmid END),
  count(case when partner_id = 'hht' then firmid end)
FROM info_silver.ods_history_deal
WHERE fdate BETWEEN 20170401 AND 20170430


SELECT
  count(DISTINCT CASE
                 WHEN to_char(bbb.date1, 'yyyymmdd') BETWEEN 20170401 AND 20170430            --有效入金数
                      AND his.refer_1_type = 'Internal Channel'
                   THEN bbb.id END) / 19,
  count(DISTINCT CASE
                 WHEN to_char(bbb.date1, 'yyyymmdd') BETWEEN 20170401 AND 20170430
                      AND his.refer_1_type = 'External Channel'
                   THEN bbb.id END) / 19,
  count(DISTINCT CASE
                 WHEN to_char(bbb.date1, 'yyyymmdd') BETWEEN 20170401 AND 20170430
                      AND his.refer_1_type = 'Others'
                   THEN bbb.id END) / 19
FROM
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
         WHERE partnerid = 'pmec') suba
        LEFT JOIN
        (SELECT
           firmid,
           (CASE WHEN inorout = 'A'
             THEN inoutmoney
            WHEN inorout = 'B'
              THEN (-1) * inoutmoney END) AS summoney,
           realdate
         FROM silver_njs.history_transfer@silver_std
        ) subb
          ON suba.firmid = subb.firmid AND suba.realdate >= subb.realdate

      GROUP BY suba.firmid, suba.realdate) aaa
   WHERE aaa.money >= 50
   GROUP BY aaa.firm_id) bbb
  JOIN info_silver.ods_history_user his
    ON bbb.id = his.firm_id


SELECT
  count(DISTINCT CASE WHEN partner_id = 'pmec' AND TO_CHAR(OPEN_ACCOUNT_TIME, 'yyyymmdd') BETWEEN 20170401 AND 20170431
    THEN firm_id END) ,
  count(DISTINCT CASE WHEN partner_id = 'njs' AND  TO_CHAR(OPEN_ACCOUNT_TIME, 'yyyymmdd') BETWEEN 20170401 AND 20170431
    THEN firm_id END) ,
  count(DISTINCT CASE WHEN partner_id = 'hht' AND  TO_CHAR(OPEN_ACCOUNT_TIME, 'yyyymmdd') BETWEEN 20170401 AND 20170431
    THEN firm_id END),
  count(DISTINCT CASE WHEN  TO_CHAR(OPEN_ACCOUNT_TIME, 'yyyymmdd') BETWEEN 20170401 AND 20170431
    THEN user_id END)
FROM tb_silver_user_stat@silver_std


SELECT
  name,
  group_id
FROM info_silver.tb_crm_ia
WHERE group_id IN (112, 113, 114)
SELECT *
FROM info_silver.ods_history_deal
WHERE partner_id = 'pmec'

SELECT
  firm_id,
  refer_1_type
FROM info_silver.ods_history_user