SELECT to_char(trunc(sysdate - 1, 'd') + 1, 'yyyymmdd')
FROM dual


SELECT
  a1.ia_id,
  a1.group_id,
  a1.ia_name,
  sum(a2.本周激活资金),
  sum(a2.本周流转单)                                         AS 本周流转单,
  sum(round(a4.当日流转当日开仓率, 2))                           AS 当日流转当日开仓率,
  sum(round(a3.当日流转当日达标率, 2))                           AS 当日流转当日达标率,
  sum(round(a5.周流转后交易额 / a5.周激活资金, 2))                  AS 本周资金利用率,
  sum(round(a6.本月流转本月交易额 / a7.本月激活资金, 2))               AS 本月资金利用率,
  sum(CASE WHEN (a6.上月流转上月底资金 IS NULL) OR (a6.上月流转上月底资金 = 0)
    THEN NULL
      ELSE round(a6.上月流转本月交易额 / a6.上月流转上月底资金, 2) END)   AS 上月流转资金利用率,
  sum(CASE WHEN (a6.上上月流转上月底资金 IS NULL) OR (a6.上上月流转上月底资金 = 0)
    THEN NULL
      ELSE round(a6.上上月流转本月交易额 / a6.上上月流转上月底资金, 2) END) AS 上上月流转资金利用率,
  sum(a5.周流转后净入金)                                       AS 当周新单流转后净入金,
  sum(a6.本月流转本月净入金)                                     AS 当月新单流转后净入金,
  sum(round(a8.本周截至当日达标率, 2))                           AS 本周截至当日达标率,
  sum(a6.上月流转本月净入金)                                     AS 上月流转本月净入金,
  sum(a6.上上月流转本月净入金)                                    AS 上上月流转本月净入金,
  sum(a1.总接手资金)                                         AS 总接手资金
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
    WHERE a.group_id IN (1, 7, 8, 111, 118) AND b.valid = 1 AND b.process IN (5, 6)
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
     count(DISTINCT b.user_id)                              AS 本周流转单
   FROM info_silver.dw_user_account a
     JOIN info_silver.ods_crm_transfer_record b
       ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
   WHERE a.group_id IN (1, 7, 8, 111, 118) AND
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
      WHERE a.group_id IN (1, 7, 8, 111, 118) AND
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
           AND a.group_id IN (1, 7, 8, 111, 118)
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
   GROUP BY bbb.ia_id,
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
      WHERE a.group_id IN (1, 7, 8, 111, 118) AND
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
           AND a.group_id IN (1, 7, 8, 111, 118)
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
   GROUP BY bbb.ia_id,
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
     sum(aa.hht_net_value_sub + aa.hht_net_in_sub) AS 周激活资金,
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
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm')
       THEN cc.本月流转后交易额 END) AS 上上月流转本月交易额,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -1), 'mm'), 'yyyymm')
       THEN cc.本月流转后交易额 END) AS 上月流转本月交易额,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
       THEN cc.本月流转后交易额 END) AS 本月流转本月交易额,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm')
       THEN ee.上月底净资产 END)   AS 上上月流转上月底资金,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -1), 'mm'), 'yyyymm')
       THEN ee.上月底净资产 END)   AS 上月流转上月底资金,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm')
       THEN dd.本月净入金 END)    AS 上上月流转本月净入金,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -1), 'mm'), 'yyyymm')
       THEN dd.本月净入金 END)    AS 上月流转本月净入金,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
       THEN dd.本月净入金 END)    AS 本月流转本月净入金
   FROM
     (SELECT
        submit_time,
        user_id,
        firm_id,
        partner_id,
        CASE WHEN partner_id = 'hht'
          THEN hht_net_in_sub + hht_net_value_sub
        WHEN partner_id = 'pmec'
          THEN pmec_net_in_sub + pmec_net_value_sub END AS activemoney
      FROM info_silver.ods_crm_transfer_record
      WHERE
        to_char(submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm') AND to_char(
            sysdate - 1, 'yyyymm')
        AND PROCESS IN (5, 6) AND valid = 1) aa
     JOIN info_silver.dw_user_account bb
       ON aa.user_id = bb.crm_user_id AND bb.partner_id IN ('pmec', 'hht')
     LEFT JOIN
     (SELECT
        dw.firm_id,
        sum(A.contqty) AS 本月流转后交易额
      FROM
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'),
                                                              'yyyymm') AND to_char(sysdate - 1, 'yyyymm')
               AND PROCESS IN (5, 6) AND valid = 1) b
        JOIN (SELECT
                crm_user_id,
                partner_id,
                firm_id
              FROM info_silver.dw_user_account
              WHERE group_id IN (1, 7, 8, 111, 118)) dw
          ON b.user_id = dw.crm_user_id AND dw.partner_id IN ('pmec', 'hht')
        JOIN info_silver.ods_history_deal a
          ON A.firmid = dw.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.trade_time > b.submit_time
      GROUP BY dw.firm_id) cc
       ON bb.firm_id = cc.firm_id
     LEFT JOIN
     (SELECT
        dw.firm_id,
        sum(CASE WHEN A.inorout = 'A'
          THEN A.inoutmoney
            WHEN A.inorout = 'B'
              THEN -A.inoutmoney END) AS 本月净入金
      FROM
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'),
                                                              'yyyymm') AND to_char(sysdate - 1, 'yyyymm')
               AND PROCESS IN (5, 6) AND valid = 1) b
        JOIN (SELECT
                crm_user_id,
                partner_id,
                firm_id
              FROM info_silver.dw_user_account
              WHERE group_id IN (1, 7, 8, 111, 118)) dw
          ON b.user_id = dw.crm_user_id AND dw.partner_id IN ('pmec', 'hht')
        JOIN silver_njs.history_transfer@silver_std A
          ON A.firmid = dw.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.realdate > b.submit_time
      GROUP BY dw.firm_id) dd
       ON bb.firm_id = dd.firm_id
     LEFT JOIN
     (SELECT
        dw.firm_id,
        sum(a.net_zcmoney) AS 上月底净资产
      FROM
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'),
                                                              'yyyymm') AND to_char(sysdate - 1, 'yyyymm')
               AND PROCESS IN (5, 6) AND valid = 1) b
        JOIN (SELECT
                crm_user_id,
                partner_id,
                firm_id
              FROM info_silver.dw_user_account
              WHERE group_id IN (1, 7, 8, 111, 118)) dw
          ON b.user_id = dw.crm_user_id AND dw.partner_id IN ('pmec', 'hht')
        JOIN info_silver.ods_order_zcmoney A
          ON A.firm_id = dw.firm_id
      WHERE A.fdate = (SELECT CASE WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期日'
        THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 2, 'yyyymmdd')
                              WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期六'
                                THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 1, 'yyyymmdd')
                              ELSE to_char(trunc(sysdate - 1, 'mm') - 1, 'yyyymmdd') END
                       FROM dual)
      GROUP BY dw.firm_id) ee
       ON bb.firm_id = ee.firm_id
   WHERE bb.group_id IN (1, 7, 8, 111, 118)
   GROUP BY bb.ia_id, bb.ia_name, bb.group_id
  ) a6
    ON a1.ia_id = a6.ia_id
  LEFT JOIN
  (SELECT
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
        to_char(a.submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'),
                                                         'yyyymm') AND to_char(
            sysdate - 1, 'yyyymm')
        AND a.PROCESS IN (5, 6) AND a.valid = 1
      GROUP BY dw.ia_id, dw.ia_name, dw.group_id, a.user_id) aa
   GROUP BY aa.ia_id, aa.ia_name, aa.group_id) a7
    ON a1.ia_id = a7.ia_id
  LEFT JOIN
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
      WHERE a.group_id IN (1, 7, 8, 111, 118) AND
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
           AND a.group_id IN (1, 7, 8, 111, 118)
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
   GROUP BY bbb.ia_id,
     bbb.group_id,
     bbb.ia_name
  ) a8
    ON a1.ia_id = a8.ia_id


GROUP BY a1.ia_id,
  a1.group_id,
  a1.ia_name






















SELECT
  a1.group_id,
  sum(a2.本周激活资金),
  sum(a2.本周流转单)                                         AS 本周流转单,
  sum(round(a4.当日流转当日开仓率, 2))                           AS 当日流转当日开仓率,
  sum(round(a3.当日流转当日达标率, 2))                           AS 当日流转当日达标率,
  sum(round(a5.周流转后交易额 / a5.周激活资金, 2))                  AS 本周资金利用率,
  sum(round(a6.本月流转本月交易额 / a7.本月激活资金, 2))               AS 本月资金利用率,
  sum(CASE WHEN (a6.上月流转上月底资金 IS NULL) OR (a6.上月流转上月底资金 = 0)
    THEN NULL
      ELSE round(a6.上月流转本月交易额 / a6.上月流转上月底资金, 2) END)   AS 上月流转资金利用率,
  sum(CASE WHEN (a6.上上月流转上月底资金 IS NULL) OR (a6.上上月流转上月底资金 = 0)
    THEN NULL
      ELSE round(a6.上上月流转本月交易额 / a6.上上月流转上月底资金, 2) END) AS 上上月流转资金利用率,
  sum(a5.周流转后净入金)                                       AS 当周新单流转后净入金,
  sum(a6.本月流转本月净入金)                                     AS 当月新单流转后净入金,
  sum(round(a8.本周截至当日达标率, 2))                           AS 本周截至当日达标率,
  sum(a6.上月流转本月净入金)                                     AS 上月流转本月净入金,
  sum(a6.上上月流转本月净入金)                                    AS 上上月流转本月净入金,
  sum(a1.总接手资金)                                         AS 总接手资金
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
    WHERE a.group_id IN (1, 7, 8, 111, 118) AND b.valid = 1 AND b.process IN (5, 6)
    GROUP BY a.group_id
  ) a1
  LEFT JOIN

  (SELECT     a.group_id,
     sum(CASE WHEN b.partner_id = 'pmec'
       THEN b.pmec_net_in_sub + b.pmec_net_value_sub
         WHEN b.partner_id = 'hht'
           THEN b.hht_net_in_sub + b.hht_net_value_sub END) AS 本周激活资金,
     count(DISTINCT b.user_id)                              AS 本周流转单
   FROM info_silver.dw_user_account a
     JOIN info_silver.ods_crm_transfer_record b
       ON a.crm_user_id = b.useR_id AND a.partner_id = B.partner_id
   WHERE a.group_id IN (1, 7, 8, 111, 118) AND
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
      WHERE a.group_id IN (1, 7, 8, 111, 118) AND
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
           AND a.group_id IN (1, 7, 8, 111, 118)
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
   GROUP BY
     bbb.group_id
  ) a3
    ON a1.group_id = a3.group_id
  LEFT JOIN
  (SELECT     bbb.group_id,
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
      WHERE a.group_id IN (1, 7, 8, 111, 118) AND
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
           AND a.group_id IN (1, 7, 8, 111, 118)
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
   GROUP BY
     bbb.group_id
  ) a4
    ON a1.group_id = a4.group_id

  LEFT JOIN

  (SELECT
     bb.group_id,
     sum(cc.流转后交易额)                                AS 周流转后交易额,
     sum(aa.hht_net_value_sub + aa.hht_net_in_sub) AS 周激活资金,
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
    ON a1.group_id = a5.group_id
  LEFT JOIN

  (SELECT
     bb.group_id,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm')
       THEN cc.本月流转后交易额 END) AS 上上月流转本月交易额,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -1), 'mm'), 'yyyymm')
       THEN cc.本月流转后交易额 END) AS 上月流转本月交易额,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
       THEN cc.本月流转后交易额 END) AS 本月流转本月交易额,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm')
       THEN ee.上月底净资产 END)   AS 上上月流转上月底资金,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -1), 'mm'), 'yyyymm')
       THEN ee.上月底净资产 END)   AS 上月流转上月底资金,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm')
       THEN dd.本月净入金 END)    AS 上上月流转本月净入金,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(trunc(add_months(sysdate - 1, -1), 'mm'), 'yyyymm')
       THEN dd.本月净入金 END)    AS 上月流转本月净入金,
     sum(CASE WHEN to_char(aa.submit_time, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
       THEN dd.本月净入金 END)    AS 本月流转本月净入金
   FROM
     (SELECT
        submit_time,
        user_id,
        firm_id,
        partner_id,
        CASE WHEN partner_id = 'hht'
          THEN hht_net_in_sub + hht_net_value_sub
        WHEN partner_id = 'pmec'
          THEN pmec_net_in_sub + pmec_net_value_sub END AS activemoney
      FROM info_silver.ods_crm_transfer_record
      WHERE
        to_char(submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'), 'yyyymm') AND to_char(
            sysdate - 1, 'yyyymm')
        AND PROCESS IN (5, 6) AND valid = 1) aa
     JOIN info_silver.dw_user_account bb
       ON aa.user_id = bb.crm_user_id AND bb.partner_id IN ('pmec', 'hht')
     LEFT JOIN
     (SELECT
        dw.firm_id,
        sum(A.contqty) AS 本月流转后交易额
      FROM
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'),
                                                              'yyyymm') AND to_char(sysdate - 1, 'yyyymm')
               AND PROCESS IN (5, 6) AND valid = 1) b
        JOIN (SELECT
                crm_user_id,
                partner_id,
                firm_id
              FROM info_silver.dw_user_account
              WHERE group_id IN (1, 7, 8, 111, 118)) dw
          ON b.user_id = dw.crm_user_id AND dw.partner_id IN ('pmec', 'hht')
        JOIN info_silver.ods_history_deal a
          ON A.firmid = dw.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.trade_time > b.submit_time
      GROUP BY dw.firm_id) cc
       ON bb.firm_id = cc.firm_id
     LEFT JOIN
     (SELECT
        dw.firm_id,
        sum(CASE WHEN A.inorout = 'A'
          THEN A.inoutmoney
            WHEN A.inorout = 'B'
              THEN -A.inoutmoney END) AS 本月净入金
      FROM
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'),
                                                              'yyyymm') AND to_char(sysdate - 1, 'yyyymm')
               AND PROCESS IN (5, 6) AND valid = 1) b
        JOIN (SELECT
                crm_user_id,
                partner_id,
                firm_id
              FROM info_silver.dw_user_account
              WHERE group_id IN (1, 7, 8, 111, 118)) dw
          ON b.user_id = dw.crm_user_id AND dw.partner_id IN ('pmec', 'hht')
        JOIN silver_njs.history_transfer@silver_std A
          ON A.firmid = dw.firm_id
      WHERE A.fdate BETWEEN to_char(trunc(sysdate - 1, 'mm'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd') AND
            A.realdate > b.submit_time
      GROUP BY dw.firm_id) dd
       ON bb.firm_id = dd.firm_id
     LEFT JOIN
     (SELECT
        dw.firm_id,
        sum(a.net_zcmoney) AS 上月底净资产
      FROM
        (SELECT *
         FROM info_silver.ods_crm_transfer_record
         WHERE to_char(submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'),
                                                              'yyyymm') AND to_char(sysdate - 1, 'yyyymm')
               AND PROCESS IN (5, 6) AND valid = 1) b
        JOIN (SELECT
                crm_user_id,
                partner_id,
                firm_id
              FROM info_silver.dw_user_account
              WHERE group_id IN (1, 7, 8, 111, 118)) dw
          ON b.user_id = dw.crm_user_id AND dw.partner_id IN ('pmec', 'hht')
        JOIN info_silver.ods_order_zcmoney A
          ON A.firm_id = dw.firm_id
      WHERE A.fdate = (SELECT CASE WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期日'
        THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 2, 'yyyymmdd')
                              WHEN to_char(trunc(sysdate - 1, 'mm') - 1, 'day') = '星期六'
                                THEN to_char(trunc(sysdate - 1, 'mm') - 1 - 1, 'yyyymmdd')
                              ELSE to_char(trunc(sysdate - 1, 'mm') - 1, 'yyyymmdd') END
                       FROM dual)
      GROUP BY dw.firm_id) ee
       ON bb.firm_id = ee.firm_id
   WHERE bb.group_id IN (1, 7, 8, 111, 118)
   GROUP BY bb.group_id
  ) a6
    ON a1.group_id = a6.group_id
  LEFT JOIN
  (SELECT
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
        to_char(a.submit_time, 'yyyymm') BETWEEN to_char(trunc(add_months(sysdate - 1, -2), 'mm'),
                                                         'yyyymm') AND to_char(
            sysdate - 1, 'yyyymm')
        AND a.PROCESS IN (5, 6) AND a.valid = 1
      GROUP BY dw.ia_id, dw.ia_name, dw.group_id, a.user_id) aa
   GROUP BY aa.group_id) a7
    ON a1.group_id = a7.group_id
  LEFT JOIN
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
      WHERE a.group_id IN (1, 7, 8, 111, 118) AND
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
           AND a.group_id IN (1, 7, 8, 111, 118)
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
   GROUP BY
     bbb.group_id
  ) a8
    ON a1.group_id = a8.group_id
GROUP BY a1.group_id
