/*流转后首笔交易仓位=流转后首笔交易额/12.5/激活资金*/


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


                       SELECT * FROM info_silver.ods_crm_transfer_record WHERE
                       to_char(submit_time, 'yyyymmdd') = 20170417


                       SELECT *
                       FROM info_silver.ods_history_deal
                       WHERE firmid = '163000000098540'
                             AND to_char(trade_time, 'yyyymmdd') = '20170418'


                       SELECT
                       a.bia_id,
                       a.bname,
                       a.bgroup_id,
                       a.firm_id,
                       a.user_id,
                       a.crm_name,
                       a.hht_net_value_sub + a.hht_net_in_sub AS 激活资金,
                       sum(e.contqty) AS 首笔交易额,
                       concat(round(sum(e.contqty) / (a.hht_net_value_sub + a.hht_net_in_sub) / 12.5 * 100, 2), '%') AS
                       首笔开仓仓位,
                       a.submit_time 流转时间,
                       e.trade_time AS 首笔开仓时间
                       FROM info_silver.ods_crm_transfer_record a
                       left JOIN
                       (SELECT
                        b.firmid,
                        min(b.trade_time) AS mintime
                        FROM info_silver.ods_history_deal b JOIN info_silver.ods_crm_transfer_record C
                        ON b.firmid = C.firm_id AND b.trade_time > C.submit_time AND b.OPERATION_SRC='open'
                        WHERE C.process IN (5, 6) AND C.valid = 1
                        GROUP BY b.firmid) d
                       ON A.firm_id = d.firmid
                       left join info_silver.ods_history_deal e ON d.firmid = e.firmid AND d.mintime = e.trade_time

                       WHERE A.process IN (5, 6) AND A.valid = 1 AND
                             to_char(submit_time, 'yyyymmdd') BETWEEN '20170522' AND '20170526'
                             AND a.bgroup_id IN (1, 7, 8, 111, 118)
                       GROUP BY a.bia_id,
                       a.bname,
                       a.bgroup_id,
                       a.firm_id,
                       a.user_id,
                       a.crm_name, e.trade_time, a.hht_net_value_sub + a.hht_net_in_sub, a.submit_time
                       ORDER BY 首笔开仓仓位 DESC


                       SELECT * FROM info_silver.ods_history_deal WHERE firmid = '163170509216146'