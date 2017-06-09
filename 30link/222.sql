SELECT
  refer_1_type,
  refer_2_type,
  to_char(open_account_time, 'yyyymmdd'),
  count(DISTINCT CASE WHEN partner_id = 'hht'
    THEN user_id END)
FROM info_silver.dw_user_account
WHERE to_char(open_account_time, 'yyyymm') = 201706
GROUP BY refer_1_type, sub_refer, refer_2_type, to_char(open_account_time, 'yyyymmdd')


SELECT

  1               AS id,
  sum(龙商交易额)      AS 投顾龙商交易额,
  --后端投顾的广贵交易人数，广贵交易额
  sum(龙商交易人数) / 5 AS 投顾龙商交易人数
FROM
  (
    SELECT
      deal.fdate,
      sum(deal.contqty)           AS 龙商交易额,
      count(DISTINCT deal.firmid) AS 龙商交易人数

    FROM info_silver.ods_history_deal deal
      JOIN info_silver.ods_crm_transfer_record trans
        ON deal.firmid = trans.firm_id
    WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
          AND deal.fdate BETWEEN '20170527' AND '20170602'
          AND deal.trade_time > trans.submit_time
          AND trans.process IN (5, 6) AND trans.valid = 1
          AND deal.partner_id = 'hht'
    GROUP BY deal.fdate
  )


SELECT
  sum(CASE WHEN trans.cur_bgroup_id IN (1, 7, 8, 118)
    THEN deal.contqty END) AS 电销龙商交易额,
  sum(CASE WHEN trans.cur_bgroup_id IN (111)
    THEN deal.contqty END) AS 微销龙商交易额
FROM info_silver.ods_history_deal deal
  JOIN info_silver.ods_crm_transfer_record trans
    ON deal.firmid = trans.firm_id
WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
      AND to_char(trans.submit_time, 'yyyymmdd') <= 20170602
      AND deal.fdate BETWEEN 20170527 AND 20170602
      AND deal.trade_time > trans.submit_time
      AND trans.process IN (5, 6) AND trans.valid = 1
      AND deal.partner_id = 'hht'

UNION ALL


SELECT
  sum(CASE WHEN trans.cur_bgroup_id IN (1, 7, 8, 118)
    THEN deal.contqty END) AS 电销新用户龙商交易额,
  sum(CASE WHEN trans.cur_bgroup_id IN (111)
    THEN deal.contqty END) AS 微销新用户龙商交易额
FROM info_silver.ods_history_deal deal
  JOIN info_silver.ods_crm_transfer_record trans
    ON deal.firmid = trans.firm_id
WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
      AND to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170527 AND 20170602
      AND deal.fdate BETWEEN 20170527 AND 20170602
      AND deal.trade_time > trans.submit_time
      AND trans.process IN (5, 6) AND trans.valid = 1
      AND deal.partner_id = 'hht'

UNION ALL


SELECT
  sum(CASE WHEN trans.cur_bgroup_id IN (1, 7, 8, 118)
    THEN zc.net_assets END) AS 电销龙商净资产,
  sum(CASE WHEN trans.cur_bgroup_id IN (111)
    THEN zc.net_assets END) AS 微销龙商净资产
FROM silver_njs.tb_silver_data_center@silver_std zc
  JOIN info_silver.ods_crm_transfer_record trans
    ON zc.firmid = trans.firm_id
WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
      AND to_char(trans.submit_time, 'yyyymmdd') <= 20170602
      AND zc.hdate BETWEEN 20170527 AND 20170602
      AND trans.process IN (5, 6) AND trans.valid = 1
      AND zc.partner_id = 'hht'
      AND zc.hdate = 20170602

UNION ALL

SELECT
  sum(CASE WHEN cur_bgroup_id IN (1, 7, 8, 118)
    THEN hht_net_in_sub + hht_net_value_sub END) AS 电销激活资金,
  sum(CASE WHEN cur_bgroup_id IN (111)
    THEN hht_net_in_sub + hht_net_value_sub END) AS 微销激活资金
FROM info_silver.ods_crm_transfer_record
WHERE process IN (5, 6) AND valid = 1
      AND to_char(submit_time, 'yyyymmdd') BETWEEN 20170527 AND 20170602

UNION ALL
SELECT
  round(sum(CASE WHEN aa.cur_bgroup_id IN (1, 7, 8, 118)
    THEN aa.首笔交易额 END) / sum(CASE WHEN aa.cur_bgroup_id IN (1, 7, 8, 118)
    THEN aa.激活资金 END) / 12.5, 4) AS 电销首笔仓位,
  round(sum(CASE WHEN aa.cur_bgroup_id IN (111)
    THEN aa.首笔交易额 END) / sum(CASE WHEN aa.cur_bgroup_id IN (111)
    THEN aa.激活资金 END) / 12.5, 4) AS 微销首笔仓位

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
         to_char(submit_time, 'yyyymmdd') BETWEEN 20170527 AND 20170602
         AND a.cur_bgroup_id IN (1, 7, 8, 118, 111)
   GROUP BY a.bia_id,
     a.bname,
     a.cur_bgroup_id,
     a.firm_id,
     a.user_id,
     a.crm_name, e.trade_time) aa


SELECT *
FROM tb_silver_user_stat@silver_std
WHERE user_name = 'fengxm100@163.com'


SELECT
  count(id),
  to_char(create_time, 'yyyymmdd')
FROM info_silver.ods_crm_tel_record
  where to_char(create_time, 'yyyymmdd')>=20170515 and ia_id is not null and billsec>60
GROUP BY to_char(create_time, 'yyyymmdd')



select count(*),sum(net_assets),sum(case when net_assets>0 then net_assets end ) from
  (select firmid from silver.tb_silver_account
where partner_id='njs' and epay_flag=1) a left join
silver_njs.tb_silver_data_center b
on a.firmid=b.firmid and b.hdate=20170519
