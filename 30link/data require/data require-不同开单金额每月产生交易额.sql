SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30             ---3月之前流转单1个月内交易额
    THEN deal.contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    ---and bgroup_id in (1,7,8)
    and bgroup_id in (111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20170331
    and deal.trade_time > trans.submit_time
  and trans.money>=50000
GROUP BY trans.firm_id, trans.money



SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---2月之前流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---2月之前流转单第2个月内交易额
    THEN contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    and bgroup_id in (111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20170228
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money

SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---1月之前流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---1月之前流转单第2个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+60 AND trans.submit_time + 90                ---1月之前流转单第3个月内交易额
    THEN contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    and bgroup_id in (111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20170131
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money



SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---12月之前流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---12月之前流转单第2个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+60 AND trans.submit_time + 90                ---12月之前流转单第3个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+90 AND trans.submit_time + 120               ---12月之前流转单第4个月内交易额
    THEN contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    and bgroup_id in (1,7,8)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20161231
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money


SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---11月之前流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---11月之前流转单第2个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+60 AND trans.submit_time + 90                ---11月之前流转单第3个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+90 AND trans.submit_time + 120               ---11月之前流转单第4个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+120 AND trans.submit_time + 150               ---11月之前流转单第5个月内交易额
    THEN contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    and bgroup_id in (111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20161130
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money



SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---10月之前流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---10月之前流转单第2个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+60 AND trans.submit_time + 90                ---10月之前流转单第3个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+90 AND trans.submit_time + 120               ---10月之前流转单第4个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+120 AND trans.submit_time + 150               ---10月之前流转单第5个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+150 AND trans.submit_time + 180               ---10月之前流转单第6个月内交易额
    THEN contqty END)
FROM
  (
    SELECT
      user_id,fgroup_id,bgroup_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1 and to_char(submit_time,'yyyymmdd')<=20161030
    and bgroup_id in (111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20161030
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money










SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30             ---2月流转单1个月内交易额
    THEN deal.contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    --and bgroup_id in (1,7,8)
    and bgroup_id in (111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymm')=201702
    and deal.trade_time > trans.submit_time
  and trans.money>=50000
GROUP BY trans.firm_id, trans.money



SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---1月流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---1月流转单第2个月内交易额
    THEN contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    and bgroup_id in (1,7,8)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymm')=201701
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money

SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---12月流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---12月流转单第2个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+60 AND trans.submit_time + 90                ---12月流转单第3个月内交易额
    THEN contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    and bgroup_id in (1,7,8,111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymm')=201612
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money



SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---11月流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---11月流转单第2个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+60 AND trans.submit_time + 90                ---11月流转单第3个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+90 AND trans.submit_time + 120               ---11月流转单第4个月内交易额
    THEN contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    and bgroup_id in (1,7,8,111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymm')=201611
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money




/*每10天对比一次*/
SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 10                   ---流转10天内交易额
    THEN contqty END)
  --sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+11 AND trans.submit_time + 20
    --THEN contqty END)
  --sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+21 AND trans.submit_time + 30
    --THEN contqty END)
  --sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+31 AND trans.submit_time + 40
    --THEN contqty END)
  --sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+41 AND trans.submit_time + 50
    --THEN contqty END)
FROM
  (
    SELECT
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    and bgroup_id in (111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd') between 20170313 and 20170321
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money



/*每10天对比一次*/
  select
    count(case when ccc.a1>0 and ccc.a1 is not null then ccc.firm_id end) as amount1,
    count(case when ccc.a2>0 and ccc.a2 is not null then ccc.firm_id end) as amount2,
    count(case when ccc.a3>0 and ccc.a3 is not null then ccc.firm_id end) as amount3,
    count(case when ccc.a4>0 and ccc.a4 is not null then ccc.firm_id end) as amount4
    --count(case when ccc.a5>0 and ccc.a5 is not null then ccc.firm_id end) as amount5
    from
(SELECT
 trans.firm_id,
 trans.money,
 sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 10 ---流转10天内交易额
   THEN contqty END) as a1,
 sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+11 AND trans.submit_time + 20
 THEN contqty END) as a2,
 sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+21 AND trans.submit_time + 30
THEN contqty END) as a3,
 sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+31 AND trans.submit_time + 40
 THEN contqty END) as a4,
 sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+41 AND trans.submit_time + 50
 THEN contqty END) as a5
 FROM
 (
 SELECT
 --user_id,
 submit_time,
 firm_id,
 pmec_net_value_sub + pmec_net_in_sub AS money
 FROM info_silver.ods_crm_transfer_record
 WHERE PROCESS IN (5, 6) AND valid = 1
 AND bgroup_id IN (1,7,8)
 ) trans
 LEFT JOIN info_silver.ods_history_deal deal
 ON trans.firm_id = deal.firmid
 WHERE to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170201 AND 20170210
 AND trans.money>=50000
 AND deal.trade_time > trans.submit_time
 GROUP BY trans.firm_id, trans.money
) ccc

select * from info_silver.ods_crm_transfer_record
where to_char(submit_time, 'yyyymmdd') BETWEEN 20170313 AND 20170322 and bgroup_id IN (1,7,8) and PROCESS IN (5, 6) AND valid = 1