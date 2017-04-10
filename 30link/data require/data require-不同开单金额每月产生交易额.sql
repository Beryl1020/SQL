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
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 10                   ---流转第一周
    THEN contqty END)
  --sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+11 AND trans.submit_time + 20                ---11月之前流转单第2个月内交易额
    --THEN contqty END)
  --sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+21 AND trans.submit_time + 30                ---11月之前流转单第3个月内交易额
    --THEN contqty END)
  --sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+31 AND trans.submit_time + 40               ---11月之前流转单第4个月内交易额
    --THEN contqty END)
  --sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+41 AND trans.submit_time + 50               ---11月之前流转单第5个月内交易额
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



SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 15                   ---10月之前流转单第1个月内交易额
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
      -- user_id,fgroup_id,bgroup_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
    and bgroup_id in (111)
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20161030
  and trans.money>=50000
  and deal.trade_time > trans.submit_time
GROUP BY trans.firm_id, trans.money


select * from silver_consult.tb_crm_transfer_record
select * from info_silver.DW_CRM_TRANS_DETAIL_BF where user_id=1000277692
