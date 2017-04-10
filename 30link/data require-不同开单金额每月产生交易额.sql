SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30             ---2月之前流转单1个月内交易额
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
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20170228
  and trans.money>=50000
GROUP BY trans.firm_id, trans.money



SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---1月之前流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---1月之前流转单第2个月内交易额
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
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20170131
  and trans.money>=50000
GROUP BY trans.firm_id, trans.money

SELECT
  trans.firm_id,
  trans.money,
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time AND trans.submit_time + 30                   ---12月之前流转单第1个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+30 AND trans.submit_time + 60                ---12月之前流转单第2个月内交易额
    THEN contqty END),
  sum(CASE WHEN deal.trade_time BETWEEN trans.submit_time+60 AND trans.submit_time + 90                ---12月之前流转单第3个月内交易额
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
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20161231
  and trans.money>=50000
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
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20161130
  and trans.money>=50000
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
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20161031
  and trans.money>=50000
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
      --user_id,
      submit_time,
      firm_id,
      pmec_net_value_sub + pmec_net_in_sub AS money
    FROM info_silver.ods_crm_transfer_record
    WHERE process IN (5, 6) AND valid = 1
  ) trans
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
  where to_char(trans.submit_time,'yyyymmdd')<=20160930
  and trans.money>=50000
GROUP BY trans.firm_id, trans.money










select * from info_silver.ods_crm_transfer_record where firm_id=163000000073982







SELECT *
FROM info_silver.ods_crm_transfer_record

SELECT *
FROM info_silver.ods_history_deal


SELECT count(1)
FROM info_silver.tb_crm_tag_user_rel
where tag_id=41
