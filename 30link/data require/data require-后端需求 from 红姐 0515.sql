SELECT to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd')
FROM dual


SELECT
  a1.ia_id,
  a1.group_id,
  a1.ia_name,
  a1.总接手资金,
  a2.本周激活资金,
  round(a3.当日流转当日达标率,2),
  round(a4.当日流转当日开仓率,2),
  round(a5.周流转后交易额/a5.周激活资金,2) as 本周资金利用率,
  round(a6.月流转后交易额/a6.月激活资金,2) as 本月资金利用率,
  a5.周流转后净入金,
  a6.本月净入金 as 月流转后净入金,
  round(a8.本周截至当日达标率,2),
  sum(a2.本周流转单) AS 流转单数
FROM
  (
    SELECT
      a.ia_id,
      a.group_id,
      a.ia_name,
      sum(CASE WHEN b.partner_id = 'pmec'
        THEN b.pmec_net_in_sub + b.pmec_net_value_sub
          WHEN b.partner_id = 'hht'
            THEN b.hht_net_in_sub + b.hht_net_in_sub END) AS 总接手资金,
      count(DISTINCT a.user_id)                           AS num
    FROM info_silver.dw_user_account a
      JOIN info_silver.ods_crm_transfer_record b
        ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
    WHERE a.group_id IN (1, 7, 8, 111,118) AND b.valid = 1 AND b.process IN (5, 6)
    GROUP BY a.ia_id, a.group_id, a.ia_name
  ) a1
  LEFT JOIN

  (SELECT
     a.ia_id,
     a.group_id,
     a.ia_name,
     sum(CASE WHEN b.partner_id = 'pmec'
       THEN b.pmec_net_in_sub + b.pmec_net_value_sub
         WHEN b.partner_id = 'hht'
           THEN b.hht_net_in_sub + b.hht_net_value_sub END) AS 本周激活资金,
    count(distinct b.user_id) as 本周流转单
   FROM info_silver.dw_user_account a
     JOIN info_silver.ods_crm_transfer_record b
       ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
   WHERE a.group_id IN (1, 7, 8, 111,118) AND
         to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
             sysdate - 1, 'yyyymmdd')
         AND b.valid = 1 AND b.process IN (5, 6)
   GROUP BY a.ia_id, a.group_id, a.ia_name
  ) a2
    ON a1.ia_id = a2.ia_id
  LEFT JOIN

  (SELECT
     bbb.ia_id,
     bbb.group_id,
     bbb.ia_name,
     round(sum(nvl(aaa.当日达标用户数, 0)) / sum(bbb.当日流转用户数), 4) AS 当日流转当日达标率
   FROM
     (SELECT
        a.ia_id,
        a.group_id,
        a.ia_name,
        count(b.user_id) AS 当日流转用户数
      FROM
        info_silver.dw_user_account a
        JOIN info_silver.ods_crm_transfer_record b
          ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
      WHERE a.group_id IN (1, 7, 8, 111,118) AND
            to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
                sysdate - 1, 'yyyymmdd')
            AND b.valid = 1 AND b.process IN (5, 6)
      GROUP BY a.ia_id, a.group_id, a.ia_name) bbb
     LEFT JOIN
     (SELECT
        aa.group_id,
        aa.ia_id,
        aa.ia_name,
        count(DISTINCT aa.user_id) AS 当日达标用户数
      FROM
        (SELECT
           trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name,
           max(trans.HHT_NET_VALUE_SUB + trans.HHT_NET_IN_SUB) AS num1,
           sum(CASE WHEN deal.wareid = 'LSAG100g'
             THEN deal.contnum END)                            AS num2
         FROM info_silver.ods_crm_transfer_record trans
           JOIN info_silver.dw_user_account a
             ON trans.user_id = a.crm_user_id
                AND a.partner_id = trans.partner_id
           JOIN (SELECT
                   wareid,
                   contnum,
                   fdate,
                   trade_time,
                   firmid
                 FROM info_silver.ods_history_deal
                 WHERE fdate >= '20160901' AND partner_id = 'hht') deal
             ON trans.firm_id = deal.firmid
         WHERE
           to_char(trans.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
               sysdate - 1, 'yyyymmdd')
           AND a.group_id IN (1, 7, 8, 111,118)
           AND trans.process IN (5, 6) AND trans.valid = 1
           AND (deal.trade_time > trans.submit_time)
           AND deal.fdate = to_char(trans.submit_time, 'yyyymmdd')
         GROUP BY trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name) aa
      WHERE (aa.num1 < 100000 AND aa.num2 >= 100)
            OR (aa.num1 < 200000 AND aa.num2 >= 200)
            OR (aa.num1 < 300000 AND aa.num2 >= 500)
            OR (aa.num1 < 500000 AND aa.num2 >= 1000)
            OR (aa.num1 < 1000000 AND aa.num2 >= 1500)
            OR (aa.num1 >= 1000000 AND aa.num2 >= 2000)

      GROUP BY aa.group_id,
        aa.ia_id,
        aa.ia_name) aaa
       ON aaa.ia_id = bbb.ia_id
    group by bbb.ia_id,
     bbb.group_id,
     bbb.ia_name
  ) a3
    ON a1.ia_id = a3.ia_id
  LEFT JOIN
  (SELECT
     bbb.ia_id,
     bbb.group_id,
     bbb.ia_name,
     round(sum(nvl(aaa.当日流转开仓用户数, 0)) / sum(bbb.当日流转用户数), 4) AS 当日流转当日开仓率
   FROM
     (SELECT
        a.ia_id,
        a.group_id,
        a.ia_name,
        count(b.user_id) AS 当日流转用户数
      FROM
        info_silver.dw_user_account a
        JOIN info_silver.ods_crm_transfer_record b
          ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
      WHERE a.group_id IN (1, 7, 8, 111,118) AND
            to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
                sysdate - 1, 'yyyymmdd')
            AND b.valid = 1 AND b.process IN (5, 6)
      GROUP BY a.ia_id, a.group_id, a.ia_name) bbb
     LEFT JOIN
     (SELECT
        aa.group_id,
        aa.ia_id,
        aa.ia_name,
        count(DISTINCT aa.user_id) AS 当日流转开仓用户数
      FROM
        (SELECT
           trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name,
           max(trans.HHT_NET_VALUE_SUB + trans.HHT_NET_IN_SUB) AS num1,
           sum(CASE WHEN deal.wareid = 'LSAG100g'
             THEN deal.contnum END)                            AS num2
         FROM info_silver.ods_crm_transfer_record trans
           JOIN info_silver.dw_user_account a
             ON trans.user_id = a.crm_user_id
                AND a.partner_id = trans.partner_id
           JOIN (SELECT
                   wareid,
                   contnum,
                   fdate,
                   trade_time,
                   firmid
                 FROM info_silver.ods_history_deal
                 WHERE fdate >= '20160901' AND partner_id = 'hht') deal
             ON trans.firm_id = deal.firmid
         WHERE
           to_char(trans.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
               sysdate - 1, 'yyyymmdd')
           AND a.group_id IN (1, 7, 8, 111,118)
           AND trans.process IN (5, 6) AND trans.valid = 1
           AND (deal.trade_time > trans.submit_time)
           AND deal.fdate = to_char(trans.submit_time, 'yyyymmdd')
         GROUP BY trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name) aa
      WHERE (aa.num1 < 100000 AND aa.num2 >= 30)
            OR (aa.num1 < 200000 AND aa.num2 >= 60)
            OR (aa.num1 < 300000 AND aa.num2 >= 120)
            OR (aa.num1 < 500000 AND aa.num2 >= 180)
            OR (aa.num1 < 1000000 AND aa.num2 >= 240)
            OR (aa.num1 < 2000000 AND aa.num2 >= 480)
            OR (aa.num1 >= 2000000 AND aa.num2 >= 720)
      GROUP BY aa.group_id,
        aa.ia_id,
        aa.ia_name) aaa
       ON aaa.ia_id = bbb.ia_id
    group by bbb.ia_id,
     bbb.group_id,
     bbb.ia_name
  ) a4
    ON a1.ia_id = a4.ia_id

  LEFT JOIN

  (SELECT
     bb.ia_id,
    bb.ia_name,
     bb.group_id,
     sum(cc.流转后交易额)                                AS 周流转后交易额,
     sum(aa.hht_net_value_sub + aa.hht_net_in_sub)     AS 周激活资金,
     sum(dd.流转后净入金)                                AS 周流转后净入金

   FROM
     (SELECT *
      FROM info_silver.ods_crm_transfer_record
      WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd')
            AND PROCESS IN (5, 6) AND valid = 1) aa
     JOIN info_silver.dw_user_account bb
       ON aa.user_id = bb.crm_user_id AND aa.partner_id = bb.partner_id
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(A.contqty) AS 流转后交易额
      FROM info_silver.ods_history_deal A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd') AND
            A.trade_time > b.submit_time
      GROUP BY A.firmid) cc
       ON aa.firm_id = cc.firmid
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(CASE WHEN A.inorout = 'A'
          THEN A.inoutmoney
            WHEN A.inorout = 'B'
              THEN -A.inoutmoney END) AS 流转后净入金
      FROM silver_njs.history_transfer@silver_std A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd') AND
            A.realdate > b.submit_time
      GROUP BY a.firmid) dd
       ON aa.firm_id = dd.firmid
   WHERE bb.group_id IN (1, 7, 8, 111, 118)
   GROUP BY
     bb.ia_id,
    bb.ia_name,
     bb.group_id
  ) a5
    ON a1.ia_id = a5.ia_id
  LEFT JOIN

  (SELECT
     bb.ia_id,
     bb.ia_name,
     bb.group_id,
    sum(cc.本月流转后交易额) as 月流转后交易额,
    sum(ee.上月底净资产) as 上月底净资产,
    sum(dd.本月净入金) as 本月净入金,
    sum(aa.hht_net_in_sub + aa.hht_net_value_sub) as 月激活资金
   FROM
     (SELECT *
      FROM info_silver.ods_crm_transfer_record
      WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'mm'),
                                                             'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
            AND PROCESS IN (5, 6) AND valid = 1) aa
     JOIN info_silver.dw_user_account bb
       ON aa.user_id = bb.crm_user_id and aa.partner_id = bb.partner_id
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(A.contqty) AS 本月流转后交易额
      FROM info_silver.ods_history_deal A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                                'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.trade_time > b.submit_time
      GROUP BY A.firmid) cc
       ON aa.firm_id = cc.firmid
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(CASE WHEN A.inorout = 'A'
          THEN A.inoutmoney
            WHEN A.inorout = 'B'
              THEN -A.inoutmoney END) AS 本月净入金
      FROM silver_njs.history_transfer@silver_std A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                                'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.realdate > b.submit_time
      GROUP BY a.firmid) dd
       ON aa.firm_id = dd.firmid
     LEFT JOIN
     (SELECT
        A.firm_id,
        sum(a.net_zcmoney) AS 上月底净资产
      FROM info_silver.ods_order_zcmoney A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                                'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firm_id = b.firm_id
      WHERE A.fdate = (SELECT CASE WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期日'
        THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 2, 'yyyymmdd')
                              WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期六'
                                THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 1, 'yyyymmdd')
                              ELSE to_char(trunc(sysdate - 1, 'mm') - 1, 'yyyymmdd') END
                       FROM dual)
      GROUP BY a.firm_id) ee
       ON aa.firm_id = ee.firm_id
   WHERE bb.group_id IN (1, 7, 8, 111,118)
   GROUP BY bb.ia_id, bb.ia_name, bb.group_id
  ) a6
    ON a1.ia_id = a6.ia_id
  left join
     (SELECT
     bbb.ia_id,
     bbb.group_id,
     bbb.ia_name,
     round(sum(nvl(aaa.当周达标用户数, 0)) / sum(bbb.当周流转用户数), 4) AS 本周截至当日达标率
   FROM
     (SELECT
        a.ia_id,
        a.group_id,
        a.ia_name,
        count(b.user_id) AS 当周流转用户数
      FROM
        info_silver.dw_user_account a
        JOIN info_silver.ods_crm_transfer_record b
          ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
      WHERE a.group_id IN (1, 7, 8, 111,118) AND
            to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
                sysdate - 1, 'yyyymmdd')
            AND b.valid = 1 AND b.process IN (5, 6)
      GROUP BY a.ia_id, a.group_id, a.ia_name) bbb
     LEFT JOIN
     (SELECT
        aa.group_id,
        aa.ia_id,
        aa.ia_name,
        count(DISTINCT aa.user_id) AS 当周达标用户数
      FROM
        (SELECT
           trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name,
           max(trans.HHT_NET_VALUE_SUB + trans.HHT_NET_IN_SUB) AS num1,
           sum(CASE WHEN deal.wareid = 'LSAG100g'
             THEN deal.contnum END)                            AS num2
         FROM info_silver.ods_crm_transfer_record trans
           JOIN info_silver.dw_user_account a
             ON trans.user_id = a.crm_user_id
                AND a.partner_id = trans.partner_id
           JOIN (SELECT
                   wareid,
                   contnum,
                   fdate,
                   trade_time,
                   firmid
                 FROM info_silver.ods_history_deal
                 WHERE fdate >= '20160901' AND partner_id = 'hht') deal
             ON trans.firm_id = deal.firmid
         WHERE
           to_char(trans.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
               sysdate - 1, 'yyyymmdd')
           AND a.group_id IN (1, 7, 8, 111,118)
           AND trans.process IN (5, 6) AND trans.valid = 1
           AND (deal.trade_time > trans.submit_time)
           AND deal.fdate <= to_char(sysdate - 1, 'yyyymmdd')
         GROUP BY trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name) aa
      WHERE (aa.num1 < 100000 AND aa.num2 >= 100)
            OR (aa.num1 < 200000 AND aa.num2 >= 200)
            OR (aa.num1 < 300000 AND aa.num2 >= 500)
            OR (aa.num1 < 500000 AND aa.num2 >= 1000)
            OR (aa.num1 < 1000000 AND aa.num2 >= 1500)
            OR (aa.num1 >= 1000000 AND aa.num2 >= 2000)

      GROUP BY aa.group_id,
        aa.ia_id,
        aa.ia_name) aaa
       ON aaa.ia_id = bbb.ia_id
       group by bbb.ia_id,
     bbb.group_id,
     bbb.ia_name
  ) a8
    ON a1.ia_id = a8.ia_id


GROUP BY a1.ia_id,
  a1.group_id,
  a1.ia_name,
  a1.总接手资金,
  a2.本周激活资金,
  a3.当日流转当日达标率,
  a4.当日流转当日开仓率,
  a5.周流转后交易额,
  a5.周激活资金,
  a6.月流转后交易额,
  a6.月激活资金,
  a5.周流转后净入金,
  a6.本月净入金,
  a8.本周截至当日达标率


















SELECT
  a1.group_id,
  a1.总接手资金,
  a2.本周激活资金,
  round(a3.当日流转当日达标率,2),
  round(a4.当日流转当日开仓率,2),
  round(a5.周流转后交易额/a5.周激活资金,2) as 本周资金利用率,
  round(a6.月流转后交易额/a6.月激活资金,2) as 本月资金利用率,
  a5.周流转后净入金,
  a6.本月净入金 as 月流转后净入金,
  round(a8.本周截至当日达标率,2),
  sum(a2.本周流转单) AS 流转单数
FROM
  (
    SELECT
      a.group_id,
      sum(CASE WHEN b.partner_id = 'pmec'
        THEN b.pmec_net_in_sub + b.pmec_net_value_sub
          WHEN b.partner_id = 'hht'
            THEN b.hht_net_in_sub + b.hht_net_in_sub END) AS 总接手资金,
      count(DISTINCT a.user_id)                           AS num
    FROM info_silver.dw_user_account a
      JOIN info_silver.ods_crm_transfer_record b
        ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
    WHERE a.group_id IN (1, 7, 8, 111,118) AND b.valid = 1 AND b.process IN (5, 6)
    GROUP BY a.group_id
  ) a1
  LEFT JOIN

  (SELECT
     a.group_id,
     sum(CASE WHEN b.partner_id = 'pmec'
       THEN b.pmec_net_in_sub + b.pmec_net_value_sub
         WHEN b.partner_id = 'hht'
           THEN b.hht_net_in_sub + b.hht_net_value_sub END) AS 本周激活资金,
    count(distinct b.user_id) as 本周流转单
   FROM info_silver.dw_user_account a
     JOIN info_silver.ods_crm_transfer_record b
       ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
   WHERE a.group_id IN (1, 7, 8, 111,118) AND
         to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
             sysdate - 1, 'yyyymmdd')
         AND b.valid = 1 AND b.process IN (5, 6)
   GROUP BY a.group_id
  ) a2
    ON a1.group_id = a2.group_id
  LEFT JOIN

  (SELECT
     bbb.group_id,
     round(sum(nvl(aaa.当日达标用户数, 0)) / sum(bbb.当日流转用户数), 4) AS 当日流转当日达标率
   FROM
     (SELECT
        a.ia_id,
        a.group_id,
        a.ia_name,
        count(b.user_id) AS 当日流转用户数
      FROM
        info_silver.dw_user_account a
        JOIN info_silver.ods_crm_transfer_record b
          ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
      WHERE a.group_id IN (1, 7, 8, 111,118) AND
            to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
                sysdate - 1, 'yyyymmdd')
            AND b.valid = 1 AND b.process IN (5, 6)
      GROUP BY a.ia_id, a.group_id, a.ia_name) bbb
     LEFT JOIN
     (SELECT
        aa.group_id,
        aa.ia_id,
        aa.ia_name,
        count(DISTINCT aa.user_id) AS 当日达标用户数
      FROM
        (SELECT
           trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name,
           max(trans.HHT_NET_VALUE_SUB + trans.HHT_NET_IN_SUB) AS num1,
           sum(CASE WHEN deal.wareid = 'LSAG100g'
             THEN deal.contnum END)                            AS num2
         FROM info_silver.ods_crm_transfer_record trans
           JOIN info_silver.dw_user_account a
             ON trans.user_id = a.crm_user_id
                AND a.partner_id = trans.partner_id
           JOIN (SELECT
                   wareid,
                   contnum,
                   fdate,
                   trade_time,
                   firmid
                 FROM info_silver.ods_history_deal
                 WHERE fdate >= '20160901' AND partner_id = 'hht') deal
             ON trans.firm_id = deal.firmid
         WHERE
           to_char(trans.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
               sysdate - 1, 'yyyymmdd')
           AND a.group_id IN (1, 7, 8, 111,118)
           AND trans.process IN (5, 6) AND trans.valid = 1
           AND deal.trade_time > trans.submit_time
           AND deal.fdate = to_char(trans.submit_time, 'yyyymmdd')
         GROUP BY trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name) aa
      WHERE (aa.num1 < 100000 AND aa.num2 >= 100)
            OR (aa.num1 < 200000 AND aa.num2 >= 200)
            OR (aa.num1 < 300000 AND aa.num2 >= 500)
            OR (aa.num1 < 500000 AND aa.num2 >= 1000)
            OR (aa.num1 < 1000000 AND aa.num2 >= 1500)
            OR (aa.num1 >= 1000000 AND aa.num2 >= 2000)

      GROUP BY aa.group_id,
        aa.ia_id,
        aa.ia_name) aaa
       ON aaa.ia_id = bbb.ia_id
    group by bbb.group_id
  ) a3
    ON a1.group_id = a3.group_id
  LEFT JOIN
  (SELECT
     bbb.group_id,
     round(sum(nvl(aaa.当日流转开仓用户数, 0)) / sum(bbb.当日流转用户数), 4) AS 当日流转当日开仓率
   FROM
     (SELECT
        a.ia_id,
        a.group_id,
        a.ia_name,
        count(b.user_id) AS 当日流转用户数
      FROM
        info_silver.dw_user_account a
        JOIN info_silver.ods_crm_transfer_record b
          ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
      WHERE a.group_id IN (1, 7, 8, 111,118) AND
            to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
                sysdate - 1, 'yyyymmdd')
            AND b.valid = 1 AND b.process IN (5, 6)
      GROUP BY a.ia_id, a.group_id, a.ia_name) bbb
     LEFT JOIN
     (SELECT
        aa.group_id,
        aa.ia_id,
        aa.ia_name,
        count(DISTINCT aa.user_id) AS 当日流转开仓用户数
      FROM
        (SELECT
           trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name,
           max(trans.HHT_NET_VALUE_SUB + trans.HHT_NET_IN_SUB) AS num1,
           sum(CASE WHEN deal.wareid = 'LSAG100g'
             THEN deal.contnum END)                            AS num2
         FROM info_silver.ods_crm_transfer_record trans
           JOIN info_silver.dw_user_account a
             ON trans.user_id = a.crm_user_id
                AND a.partner_id = trans.partner_id
           JOIN (SELECT
                   wareid,
                   contnum,
                   fdate,
                   trade_time,
                   firmid
                 FROM info_silver.ods_history_deal
                 WHERE fdate >= '20160901' AND partner_id = 'hht') deal
             ON trans.firm_id = deal.firmid
         WHERE
           to_char(trans.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
               sysdate - 1, 'yyyymmdd')
           AND a.group_id IN (1, 7, 8, 111,118)
           AND trans.process IN (5, 6) AND trans.valid = 1
           AND (deal.trade_time > trans.submit_time)
           AND deal.fdate = to_char(trans.submit_time, 'yyyymmdd')
         GROUP BY trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name) aa
      WHERE (aa.num1 < 100000 AND aa.num2 >= 30)
            OR (aa.num1 < 200000 AND aa.num2 >= 60)
            OR (aa.num1 < 300000 AND aa.num2 >= 120)
            OR (aa.num1 < 500000 AND aa.num2 >= 180)
            OR (aa.num1 < 1000000 AND aa.num2 >= 240)
            OR (aa.num1 < 2000000 AND aa.num2 >= 480)
            OR (aa.num1 >= 2000000 AND aa.num2 >= 720)
      GROUP BY aa.group_id,
        aa.ia_id,
        aa.ia_name) aaa
       ON aaa.ia_id = bbb.ia_id
    group by bbb.group_id
  ) a4
    ON a1.group_id = a4.group_id

  LEFT JOIN

  (SELECT
     bb.group_id,
     sum(cc.流转后交易额)                                AS 周流转后交易额,
     sum(aa.hht_net_value_sub + aa.hht_net_in_sub)     AS 周激活资金,
     sum(dd.流转后净入金)                                AS 周流转后净入金

   FROM
     (SELECT *
      FROM info_silver.ods_crm_transfer_record
      WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd')
            AND PROCESS IN (5, 6) AND valid = 1) aa
     JOIN info_silver.dw_user_account bb
       ON aa.user_id = bb.crm_user_id AND aa.partner_id = bb.partner_id
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(A.contqty) AS 流转后交易额
      FROM info_silver.ods_history_deal A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd') AND
            A.trade_time > b.submit_time
      GROUP BY A.firmid) cc
       ON aa.firm_id = cc.firmid
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(CASE WHEN A.inorout = 'A'
          THEN A.inoutmoney
            WHEN A.inorout = 'B'
              THEN -A.inoutmoney END) AS 流转后净入金
      FROM silver_njs.history_transfer@silver_std A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
          sysdate - 1, 'yyyymmdd') AND
            A.realdate > b.submit_time
      GROUP BY a.firmid) dd
       ON aa.firm_id = dd.firmid
   WHERE bb.group_id IN (1, 7, 8, 111, 118)
   GROUP BY
     bb.group_id
  ) a5
    ON a1.group_id= a5.group_id
  LEFT JOIN

  (SELECT
     bb.group_id,
    sum(cc.本月流转后交易额) as 月流转后交易额,
    sum(ee.上月底净资产) as 上月底净资产,
    sum(dd.本月净入金) as 本月净入金,
    sum(aa.hht_net_in_sub + aa.hht_net_value_sub) as 月激活资金
   FROM
     (SELECT *
      FROM info_silver.ods_crm_transfer_record
      WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'mm'),
                                                             'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
            AND PROCESS IN (5, 6) AND valid = 1) aa
     JOIN info_silver.dw_user_account bb
       ON aa.user_id = bb.crm_user_id and aa.partner_id = bb.partner_id
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(A.contqty) AS 本月流转后交易额
      FROM info_silver.ods_history_deal A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                                'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.trade_time > b.submit_time
      GROUP BY A.firmid) cc
       ON aa.firm_id = cc.firmid
     LEFT JOIN
     (SELECT
        A.firmid,
        sum(CASE WHEN A.inorout = 'A'
          THEN A.inoutmoney
            WHEN A.inorout = 'B'
              THEN -A.inoutmoney END) AS 本月净入金
      FROM silver_njs.history_transfer@silver_std A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                                'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firmid = b.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.realdate > b.submit_time
      GROUP BY a.firmid) dd
       ON aa.firm_id = dd.firmid
     LEFT JOIN
     (SELECT
        A.firm_id,
        sum(a.net_zcmoney) AS 上月底净资产
      FROM info_silver.ods_order_zcmoney A
        JOIN
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymmdd') BETWEEN to_char(trunc(add_months(sysdate - 1, -1), 'mm'),
                                                                'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
               AND PROCESS IN (5, 6) AND valid = 1) b
          ON A.firm_id = b.firm_id
      WHERE A.fdate = (SELECT CASE WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期日'
        THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 2, 'yyyymmdd')
                              WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期六'
                                THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 1, 'yyyymmdd')
                              ELSE to_char(trunc(sysdate - 1, 'mm') - 1, 'yyyymmdd') END
                       FROM dual)
      GROUP BY a.firm_id) ee
       ON aa.firm_id = ee.firm_id
   WHERE bb.group_id IN (1, 7, 8, 111,118)
   GROUP BY  bb.group_id
  ) a6
    ON a1.group_id = a6.group_id
  left join
     (SELECT
     bbb.group_id,
     round(sum(nvl(aaa.当周达标用户数, 0)) / sum(bbb.当周流转用户数), 4) AS 本周截至当日达标率
   FROM
     (SELECT
        a.ia_id,
        a.group_id,
        a.ia_name,
        count(b.user_id) AS 当周流转用户数
      FROM
        info_silver.dw_user_account a
        JOIN info_silver.ods_crm_transfer_record b
          ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
      WHERE a.group_id IN (1, 7, 8, 111,118) AND
            to_char(b.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
                sysdate - 1, 'yyyymmdd')
            AND b.valid = 1 AND b.process IN (5, 6)
      GROUP BY a.ia_id, a.group_id, a.ia_name) bbb
     LEFT JOIN
     (SELECT
        aa.group_id,
        aa.ia_id,
        aa.ia_name,
        count(DISTINCT aa.user_id) AS 当周达标用户数
      FROM
        (SELECT
           trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name,
           max(trans.HHT_NET_VALUE_SUB + trans.HHT_NET_IN_SUB) AS num1,
           sum(CASE WHEN deal.wareid = 'LSAG100g'
             THEN deal.contnum END)                            AS num2
         FROM info_silver.ods_crm_transfer_record trans
           JOIN info_silver.dw_user_account a
             ON trans.user_id = a.crm_user_id
                AND a.partner_id = trans.partner_id
           JOIN (SELECT
                   wareid,
                   contnum,
                   fdate,
                   trade_time,
                   firmid
                 FROM info_silver.ods_history_deal
                 WHERE fdate >= '20160901' AND partner_id = 'hht') deal
             ON trans.firm_id = deal.firmid
         WHERE
           to_char(trans.submit_time, 'yyyymmdd') BETWEEN to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd') AND to_char(
               sysdate - 1, 'yyyymmdd')
           AND a.group_id IN (1, 7, 8, 111,118)
           AND trans.process IN (5, 6) AND trans.valid = 1
           AND (deal.trade_time > trans.submit_time)
           AND deal.fdate <= to_char(sysdate - 1, 'yyyymmdd')
         GROUP BY trans.user_id,
           a.group_id,
           a.ia_id,
           a.ia_name) aa
      WHERE (aa.num1 < 100000 AND aa.num2 >= 100)
            OR (aa.num1 < 200000 AND aa.num2 >= 200)
            OR (aa.num1 < 300000 AND aa.num2 >= 500)
            OR (aa.num1 < 500000 AND aa.num2 >= 1000)
            OR (aa.num1 < 1000000 AND aa.num2 >= 1500)
            OR (aa.num1 >= 1000000 AND aa.num2 >= 2000)

      GROUP BY aa.group_id,
        aa.ia_id,
        aa.ia_name) aaa
       ON aaa.ia_id = bbb.ia_id
       group by bbb.group_id
  ) a8
    ON a1.group_id = a8.group_id


GROUP BY
  a1.group_id,
  a1.总接手资金,
  a2.本周激活资金,
  a3.当日流转当日达标率,
  a4.当日流转当日开仓率,
  a5.周流转后交易额,
  a5.周激活资金,
  a6.月流转后交易额,
  a6.月激活资金,
  a5.周流转后净入金,
  a6.本月净入金,
  a8.本周截至当日达标率
