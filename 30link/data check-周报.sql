--  Part 1. 当周交易额

(SELECT
   sum(CASE WHEN PARTNER_ID = 'njs' AND fdate BETWEEN 20170506 AND 20170512
     THEN CONTQTY END) AS n1,
   sum(CASE WHEN PARTNER_ID = 'njs' AND fdate BETWEEN 20170501 AND 20170512
     THEN CONTQTY END) AS n2,
   sum(CASE WHEN PARTNER_ID = 'njs' AND fdate BETWEEN 20170401 AND 20170512
     THEN CONTQTY END) AS n3,
   sum(CASE WHEN PARTNER_ID = 'njs' AND fdate BETWEEN 20170101 AND 20170512
     THEN CONTQTY END) AS n4
 FROM info_silver.ods_history_deal)    --njs 周、月、季度、年总交易额
UNION

(SELECT
   sum(CASE WHEN PARTNER_ID = 'hht' AND fdate BETWEEN 20170506 AND 20170512
     THEN CONTQTY END) AS p1,
   sum(CASE WHEN PARTNER_ID = 'hht' AND fdate BETWEEN 20170501 AND 20170512
     THEN CONTQTY END) AS p2,
   sum(CASE WHEN PARTNER_ID = 'hht' AND fdate BETWEEN 20170401 AND 20170512
     THEN CONTQTY END) AS p3,
   sum(CASE WHEN PARTNER_ID = 'hht' AND fdate BETWEEN 20170101 AND 20170512
     THEN CONTQTY END) AS p4
 FROM info_silver.ods_history_deal)    --hht周、月、季度、年总交易额
UNION
(SELECT
   sum(CASE WHEN fdate BETWEEN 20170506 AND 20170512
     THEN CONTQTY END) AS p1,
   sum(CASE WHEN fdate BETWEEN 20170501 AND 20170512
     THEN CONTQTY END) AS p2,
   sum(CASE WHEN fdate BETWEEN 20170401 AND 20170512
     THEN CONTQTY END) AS p3,
   sum(CASE WHEN fdate BETWEEN 20170101 AND 20170512
     THEN CONTQTY END) AS p4
 FROM info_silver.ods_history_deal);   --平台周、月、季度、年总交易额


-- Part 2. 平台基础数据
SELECT
  sum(CASE WHEN PARTNER_ID = 'hht' AND fdate BETWEEN 20170506 AND 20170512
    THEN CONTQTY END) / 5,
  sum(CASE WHEN fdate BETWEEN 20170506 AND 20170512
    THEN CONTQTY END) / 5
FROM info_silver.ods_history_deal -- hht日均交易额，平台日均交易额

UNION ALL

SELECT
  sum(CASE WHEN partnerid = 'hht' AND fdate BETWEEN 20170506 AND 20170512 AND inorout = 'A'
    THEN inoutmoney
      WHEN partnerid = 'hht' AND fdate BETWEEN 20170506 AND 20170512 AND inorout = 'B'
        THEN (-1) * inoutmoney END),
  sum(CASE WHEN fdate BETWEEN 20170506 AND 20170512 AND inorout = 'A'
    THEN inoutmoney
      WHEN fdate BETWEEN 20170506 AND 20170512 AND inorout = 'B'
        THEN (-1) * inoutmoney END)
FROM silver_njs.history_transfer@silver_std -- hht净入金、平台净入金

UNION ALL

SELECT
  sum(CASE WHEN a.PARTNER_ID = 'hht'
    THEN a.NET_zcmoney END),
  sum(a.NET_zcmoney)
FROM info_silver.ods_order_zcmoney a
  JOIN info_silver.dw_user_account b
    ON a.firm_id = b.firm_id
WHERE a.fdate = 20170512 -- hht净资产、平台净资产

UNION ALL

SELECT
  sum(a.cnt1) / 5,
  sum(a.cnt2) / 5
FROM
  (SELECT
     deal.fdate,
     count(DISTINCT CASE WHEN deal.partner_id = 'hht' AND deal.fdate BETWEEN 20170506 AND 20170512
       THEN deal.firmid END) AS cnt1,
     count(DISTINCT CASE WHEN deal.fdate BETWEEN 20170506 AND 20170512
       THEN deal.firmid END) AS cnt2
   FROM info_silver.ods_history_deal deal
   WHERE deal.fdate BETWEEN 20170506 AND 20170512
   --   and deal.ordersty<>151 --非强平
   GROUP BY deal.fdate) a -- hht日均交易用户，平台日均交易用户


UNION ALL

SELECT
  count(DISTINCT CASE WHEN partner_id = 'hht' AND TO_CHAR(OPEN_ACCOUNT_TIME, 'yyyymmdd') BETWEEN 20170506 AND 20170512
    THEN firm_id END) / 5,
  count(DISTINCT CASE WHEN TO_CHAR(OPEN_ACCOUNT_TIME, 'yyyymmdd') BETWEEN 20170506 AND 20170512
    THEN firm_id END) / 5
FROM tb_silver_user_stat@silver_std -- hht日均开户用户，平台日均开户用户

UNION ALL

SELECT
  count(CASE WHEN aa.pid = 'hht' AND aa.mindate BETWEEN 20170506 AND 20170512
    THEN aa.firmid END) / 5,
  count(CASE WHEN aa.mindate BETWEEN 20170506 AND 20170512
    THEN aa.firmid END) / 5
FROM
  (SELECT
     trans.firmid,
     min(trans.fdate) AS mindate,
     trans.partnerid  AS pid
   FROM silver_njs.history_transfer@silver_std trans
   WHERE inorout = 'A'
   GROUP BY firmid, partnerid) aa -- hht日均首次入金用户，平台日均首次入金用户

UNION ALL
SELECT
  count(DISTINCT CASE WHEN to_char(bbb.date1, 'yyyymmdd') BETWEEN 20170506 AND 20170512
    THEN bbb.id END) / 5,
  0
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
GROUP BY 0; --hht新增有效净入金用户，第二列为空


----Part 3 投顾新单数据
SELECT
  sum(CASE WHEN partneR_id = 'hht'
    THEN trans.hht_net_value_sub + trans.hht_net_in_sub
      WHEN partner_id = 'pmec'
        THEN pmec_net_value_sub + pmec_net_in_sub END) AS 激活资金,
  count(DISTINCT trans.firm_id)                        AS 流转单数
FROM info_silver.ods_crm_transfer_record trans
WHERE to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170506 AND 20170512
      AND trans.process IN (5, 6) AND trans.valid = 1;



-- Part 4. 龙商华泰数据
SELECT
  sub1.money              AS 总入金,
  sub2.money              AS 净入金,
  sub3.money              AS 总交易额,
  sub3.money * 0.00065     AS 手续费,
  sub4.money              AS 点差,
  sub5.money              AS 滞纳金,
  sub6.money - sub4.money AS 头寸
FROM

  (SELECT
     1                  AS subid,
     sum(io.inoutmoney) AS money -- 总入金
   FROM silver_njs.history_transfer@silver_std io
   WHERE io.inorout = 'A' AND io.partnerid = 'hht' AND io.fdate BETWEEN 20170506 AND 20170512
   GROUP BY 1) sub1
  LEFT JOIN
  (SELECT
     2                                 AS subid,
     sum(CASE WHEN io.inorout = 'A'
       THEN inoutmoney
         WHEN io.inorout = 'B'
           THEN (-1) * inoutmoney END) AS money --净入金
   FROM silver_njs.history_transfer@silver_std io
   WHERE io.partnerid = 'hht' AND io.fdate BETWEEN 20170506 AND 20170512
   GROUP BY 2) sub2
    ON sub1.subid <> sub2.subid
  LEFT JOIN
  (SELECT
     3                 AS subid,
     sum(deal.CONTQTY) AS money -- 总交易额
   FROM info_silver.ods_history_deal deal
   WHERE deal.partner_id = 'hht' AND deal.fdate BETWEEN '20170506' AND '20170512'

   GROUP BY 3) sub3
    ON sub1.subid <> sub3.subid
  LEFT JOIN
  (SELECT
     4                                AS subid,
     sum(CASE
         WHEN deal.wareid = 'LSAG100g'
           THEN deal.contnum * 8 END) AS money -- 点差
   FROM info_silver.ods_history_deal deal
   WHERE deal.partner_id = 'hht'
         AND ((deal.operation_src = 'open' AND buyorsal = 'B') OR (deal.operation_src = 'close' AND buyorsal = 'S'))
         AND deal.fdate BETWEEN '20170506' AND '20170512'
   GROUP BY 4) sub4
    ON sub1.subid <> sub4.subid
  LEFT JOIN
  (SELECT
     5                       AS subid,
     sum(CASE WHEN flow.type = 7
       THEN flow.AMOUNT END) AS money -- 滞纳金
   FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT flow
   WHERE to_char(create_time - 0.25, 'yyyymmdd') BETWEEN 20170506 AND 20170512
   GROUP BY 5) sub5
    ON sub1.subid <> sub5.subid
  LEFT JOIN
  (SELECT
     6                              AS subid,
     sum(CASE WHEN flow.type IN (5, 6)
       THEN (-1) * flow.amount END) AS money -- 头寸+点差
   FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT flow
   WHERE to_char(create_time - 0.25, 'yyyymmdd') BETWEEN 20170506 AND 20170512
   GROUP BY 6) sub6
    ON sub1.subid <> sub6.subid;





----Part 5 投顾近5周交易额、交易人数、净入金、收入及其在广贵所占比

SELECT
  aaa.投顾龙商交易额,
  aaa.投顾龙商交易人数,
  bbb.后端用户平台收入,
  ccc.后端用户净入金
FROM
  (SELECT

     1               AS id,
     sum(龙商交易额)      AS 投顾龙商交易额,
     --后端投顾的广贵交易人数，广贵交易额
     sum(龙商交易人数) / 5 AS 投顾龙商交易人数
   FROM
     (
       SELECT

         sum(deal.contqty)           AS 龙商交易额,
         count(DISTINCT deal.firmid) AS 龙商交易人数,
         deal.fdate
       FROM info_silver.ods_history_deal deal
         JOIN info_silver.ods_crm_transfer_record trans
           ON deal.firmid = trans.firm_id
       WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
             AND deal.fdate BETWEEN 20170506 AND 20170512
             AND deal.trade_time > trans.submit_time
             AND trans.process IN (5, 6) AND trans.valid = 1
             AND deal.partner_id = 'hht'
       GROUP BY deal.fdate
     )
  ) aaa
  JOIN

  (
    SELECT
      -- 后端投顾维护用户产出的平台收入
      2                                  AS ID,
      sum(CASE WHEN flow.type IN (4, 3)
        THEN flow.amount / 8 * 6.5 * (-1)
          WHEN flow.type IN (7)
            THEN flow.amount
          WHEN flow.type IN (5, 6)
            THEN flow.amount * (-1) END) AS 后端用户平台收入
    FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT flow
      JOIN info_silver.ods_crm_transfer_record trans
        ON flow.fund_id = trans.firm_id
    WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
          AND trans.process IN (5, 6) AND trans.valid = 1
          AND trans.submit_time < flow.create_time
          AND to_char(flow.create_time - 0.25, 'yyyymmdd') BETWEEN 20170506 AND 20170512
  ) bbb
    ON aaa.id <> bbb.id

  JOIN
  (
    SELECT
      -- 后端投顾维护用户净入金
      3                                       AS ID,
      sum(CASE WHEN inout.inorout = 'A'
        THEN inout.inoutmoney
          WHEN inout.inorout = 'B'
            THEN inout.inoutmoney * (-1) END) AS 后端用户净入金
    FROM silver_njs.history_transfer@silver_std inout
      JOIN info_silver.ods_crm_transfer_record trans
        ON inout.firmid = trans.firm_id
    WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
          AND trans.process IN (5, 6) AND trans.valid = 1
          AND inout.partnerid = 'hht'
          AND inout.fdate BETWEEN 20170506 AND 20170512
  ) ccc
    ON aaa.id <> ccc.id;




-- Part 6 每周电销资源转化情况


SELECT
  '电销前端开单',
  count(DISTINCT user_id)
FROM info_silver.ods_crm_transfer_record
WHERE process IN (5, 6) AND valid = 1
      AND to_char(submit_time, 'yyyymmdd') BETWEEN 20170506 AND 20170512
      AND fgroup_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 116, 117, 106) --电销前端开单

UNION ALL

SELECT
  '电销当周开单当周有效开仓',
  count(aa.id)
FROM
  (SELECT
     DISTINCT
     trans.firm_id                                       AS id,
     max(trans.HHT_NET_VALUE_SUB + trans.HHT_NET_IN_SUB) AS num1,
     sum(CASE WHEN deal.wareid = 'LSAG100g'
       THEN deal.contnum END)                            AS num2
   FROM info_silver.ods_crm_transfer_record trans
     LEFT JOIN info_silver.ods_history_deal deal
       ON trans.firm_id = deal.firmid
   WHERE to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170506 AND 20170512
         AND trans.fgroup_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 116, 117)
         AND trans.process IN (5, 6) AND trans.valid = 1
         AND (deal.trade_time > trans.submit_time)
         AND deal.fdate <= 20170512
   GROUP BY trans.firm_id) aa
WHERE (aa.num1 < 100000 AND aa.num2 >= 30)
      OR (aa.num1 < 200000 AND aa.num2 >= 60)
      OR (aa.num1 < 300000 AND aa.num2 >= 120)
      OR (aa.num1 < 500000 AND aa.num2 >= 180)
      OR (aa.num1 < 1000000 AND aa.num2 >= 240)
      OR (aa.num1 < 2000000 AND aa.num2 >= 480)
      OR (aa.num1 >= 2000000 AND aa.num2 >= 720)                            ---  有效开仓

UNION ALL

SELECT
  '新增资源数',
  count(DISTINCT b1.user_id)
FROM

  (SELECT
     a1.user_id,
     a1.firsttime
   FROM
     (SELECT
        dis.user_id,
        min(dis.create_time) AS firsttime
      FROM silver_consult.tb_crm_dispatch_his@consul_std dis
      WHERE dis.ia_id IS NOT NULL
      GROUP BY dis.user_id) a1
   WHERE to_char(a1.firsttime, 'yyyymmdd') BETWEEN 20170506 AND 20170512) b1

  JOIN silver_consult.tb_crm_dispatch_his@consul_std dis ON b1.user_id = dis.user_id AND b1.firsttime = dis.create_time
  JOIN silver_consult.tb_crm_ia@consul_std ia ON dis.ia_id = ia.id
  JOIN silver_consult.v_tb_crm_user@consul_std user1 ON b1.user_id = user1.id AND user1.id IS NOT NULL

WHERE ia.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 116, 117) -- 新增资源数

UNION ALL

SELECT
  '新增A/B资源数',
  count(DISTINCT b1.user_id)
FROM

  (SELECT
     a1.user_id,
     a1.firsttime
   FROM
     (SELECT
        dis.user_id,
        min(dis.create_time) AS firsttime
      FROM silver_consult.tb_crm_dispatch_his@consul_std dis
      WHERE dis.ia_id IS NOT NULL
      GROUP BY dis.user_id) a1
   WHERE to_char(a1.firsttime, 'yyyymmdd') BETWEEN 20170506 AND 20170512) b1

  JOIN silver_consult.tb_crm_dispatch_his@consul_std dis ON b1.user_id = dis.user_id AND b1.firsttime = dis.create_time
  JOIN silver_consult.tb_crm_ia@consul_std ia ON dis.ia_id = ia.id
  JOIN silver_consult.v_tb_crm_user@consul_std user1 ON b1.user_id = user1.id AND user1.id IS NOT NULL

WHERE ia.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 116, 117) AND
      user1.grade IN ('A', 'A紧急', 'A暂缓', 'B'); -- 新增A/B类资源


-- Part 7.  每周微销资源转化情况

SELECT
  '微销前端开单',
  count(DISTINCT user_id)
FROM info_silver.ods_crm_transfer_record
WHERE process IN (5, 6) AND valid = 1
      AND to_char(submit_time, 'yyyymmdd') BETWEEN 20170506 AND 20170512
      AND fgroup_id IN (112, 113, 114) --微销前端开单

UNION ALL

SELECT
  '微销当周开单当周有效开仓',
  count(aa.id)
FROM
  (SELECT
     DISTINCT
     trans.firm_id                                       AS id,
     max(trans.hht_NET_VALUE_SUB + trans.hht_NET_IN_SUB) AS num1,
     sum(CASE WHEN deal.wareid = 'LSAG100g'
       THEN deal.contnum END)                            AS num2
   FROM info_silver.ods_crm_transfer_record trans
     LEFT JOIN info_silver.ods_history_deal deal
       ON trans.firm_id = deal.firmid
   WHERE to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170506 AND 20170512
         AND trans.fgroup_id IN (112, 113, 114)
         AND trans.process IN (5, 6) AND trans.valid = 1
         AND (deal.trade_time > trans.submit_time)
         AND deal.fdate <= 20170512
   GROUP BY trans.firm_id) aa
WHERE (aa.num1 < 100000 AND aa.num2 >= 30)
      OR (aa.num1 < 200000 AND aa.num2 >= 60)
      OR (aa.num1 < 300000 AND aa.num2 >= 120)
      OR (aa.num1 < 500000 AND aa.num2 >= 180)
      OR (aa.num1 < 1000000 AND aa.num2 >= 240)
      OR (aa.num1 < 2000000 AND aa.num2 >= 480)
      OR (aa.num1 >= 2000000 AND aa.num2 >= 720)      ;                      ---  有效开仓


-- Part 8.  后端维护用户资金、出入金及交易情况

SELECT
  b1.id,
  b1.总接手资金,
  b1.本周接收资金,
  b1.服务用户数,
  b1.本周新增服务用户数,
  b2.总入金量,
  b2.总出金量,
  b3.交易用户数,
  b4.有效开仓用户数,
  b5.维护间隔
FROM
  (
    SELECT

      trans.cur_bgroup_id                                        AS id,
      sum(case when partner_id = 'hht' then trans.hht_net_value_sub + trans.hht_net_in_sub
        when partner_id = 'pmec' then trans.pmec_net_value_sub +trans.pmec_net_in_sub end)        AS 总接手资金,
      sum(CASE WHEN to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170506 AND 20170512
        THEN trans.hht_net_value_sub + trans.hht_net_in_sub END) AS 本周接收资金,
      count(DISTINCT trans.user_id)                              AS 服务用户数,
      count(DISTINCT CASE WHEN to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170506 AND 20170512
        THEN trans.user_id END)                                  AS 本周新增服务用户数
    FROM info_silver.ods_crm_transfer_record trans
    WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
          AND trans.process IN (5, 6) AND trans.valid = 1
    -- and to_char(trans.submit_time,'yyyymmdd')<=20170323
    GROUP BY trans.cur_bgroup_id
  ) b1
  JOIN
  (
    SELECT

      trans.cur_bgroup_id          AS id,
      sum(CASE WHEN inout.inorout = 'A'
        THEN inout.inoutmoney END) AS 总入金量,
      sum(CASE WHEN inout.inorout = 'B'
        THEN inout.inoutmoney END) AS 总出金量
    FROM info_silver.ods_crm_transfer_record trans
      join (select firm_id ,crm_user_id,partner_id from info_silver.dw_user_account where partner_id in ('pmec','hht')) user1
      on trans.user_id = user1.crm_user_id
      JOIN silver_njs.history_transfer@silver_std inout
        ON user1.firm_id = inout.firmid
    WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
          AND trans.process IN (5, 6) AND trans.valid = 1
          AND inout.fdate BETWEEN 20170506 AND 20170512
          AND trans.submit_time < inout.realdate
    GROUP BY trans.cur_bgroup_id
  ) b2
    ON b1.id = b2.id
  JOIN
  (
    SELECT


      trans.cur_bgroup_id           AS id,
      count(DISTINCT trans.useR_id) AS 交易用户数
    FROM info_silver.ods_crm_transfer_record trans
      join (select firm_id ,crm_user_id,partner_id from info_silver.dw_user_account where partner_id in ('pmec','hht')) user1
      on trans.user_id = user1.crm_user_id
      JOIN info_silver.ods_history_deal deal
        ON user1.firm_id = deal.firmid
    WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
          AND trans.process IN (5, 6) AND trans.valid = 1
          AND trans.submit_time < deal.trade_time
          AND deal.fdate BETWEEN 20170506 AND 20170512
    GROUP BY trans.cur_bgroup_id
  ) b3
    ON b1.id = b3.id
  JOIN
  (
    SELECT
      aaa.cur_bgroup_id      AS id,
      count(aaa.crm_user_id) AS 有效开仓用户数                     ---有效开仓
    FROM
      (
        SELECT
          aa.crm_user_id                    AS crm_user_id,
          aa.cur_bgroup_id                  AS cur_bgroup_id,
          max(aa.fmoney)                    AS fmoney,
          sum(CASE WHEN deal.wareid = 'LSAG100g'
            THEN deal.contnum
              WHEN deal.wareid = 'GDAG'
                THEN deal.contnum
              WHEN deal.wareid = 'GDPD'
                THEN deal.contnum * 30
              WHEN deal.wareid = 'GDPT'
                THEN deal.contnum * 65 END) AS contnum
        FROM
          (
            SELECT
              trans.user_id                                  AS crm_user_id,
              trans.partner_id                               AS partner_id,
              trans.cur_bgroup_id                            AS cur_bgroup_id,
              case when partner_id = 'hht' then trans.hht_net_value_sub + trans.hht_net_in_sub
                when partner_id = 'pmec' then trans.pmec_net_value_sub + trans.pmec_net_in_sub end AS fmoney,
              trans.submit_time                              AS submit_time
            FROM info_silver.ods_crm_transfer_record trans
            WHERE trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
                  AND trans.process IN (5, 6) AND trans.valid = 1
          ) aa
          JOIN (SELECT
                  firm_id,
                  crm_user_id,
                  partner_id
                FROM info_silver.dw_user_account
                WHERE partner_id IN ('pmec', 'hht')) ff
            ON aa.crm_user_id = ff.crm_user_id
          JOIN (SELECT
                  firmid,
                  wareid,
                  contnum,
                  trade_time,
                  fdate
                FROM info_silver.ods_history_deal
                WHERE fdate >= '20160901') deal
            ON deal.firmid = ff.firm_id
               AND aa.submit_time < deal.trade_time
               AND trunc(to_Date(deal.fdate, 'yyyymmdd'), 'mm') <= add_months(trunc(aa.submit_time, 'mm'), 1)
        GROUP BY aa.crm_user_id, aa.cur_bgroup_id
      ) aaa
    WHERE (aaa.fmoney < 100000 AND aaa.contnum >= 30)
          OR (aaa.fmoney < 200000 AND aaa.contnum >= 60)
          OR (aaa.fmoney < 300000 AND aaa.contnum >= 120)
          OR (aaa.fmoney < 500000 AND aaa.contnum >= 180)
          OR (aaa.fmoney < 1000000 AND aaa.contnum >= 240)
          OR (aaa.fmoney < 2000000 AND aaa.contnum >= 480)
          OR (aaa.fmoney >= 2000000 AND aaa.contnum >= 720)
    GROUP BY aaa.cur_bgroup_id
  ) b4
    ON b1.id = b4.id
  JOIN
  (
    SELECT
      -- 维护间隔
      a.cur_bgroup_id AS id,
      avg(a.delta)    AS 维护间隔
    FROM
      (
        SELECT

          trans.firm_id,
          trans.cur_bgroup_id                           AS cur_bgroup_id,
          min(tel.create_time) - min(trans.submit_time) AS delta
        FROM info_silver.ods_crm_transfer_record trans
          JOIN info_silver.tb_crm_tel_record tel
            ON tel.user_id = trans.user_id
        WHERE tel.create_time > trans.submit_time
              AND to_char(trans.submit_time, 'yyyymmdd') BETWEEN 20170506 AND 20170512
              AND to_char(tel.create_time, 'yyyymmdd') = to_char(trans.submit_time, 'yyyymmdd')
              AND tel.ia_id = trans.bia_id
              AND trans.cur_bgroup_id IN (1, 7, 8, 111, 118)
              AND trans.process IN (5, 6) AND trans.valid = 1
              AND tel.worksec > 0
        GROUP BY trans.firm_id, trans.cur_bgroup_id
      ) a
    WHERE a.delta IS NOT NULL AND a.delta > 0
    GROUP BY a.cur_bgroup_id
  ) b5
    ON b1.id = b5.id
ORDER BY b1.id*1;



-- Part 9. 南交、广贵内外推折算交易额
SELECT
  sum(CASE WHEN deal.partner_id = 'njs' AND refer.refer_1_type = 'Internal Channel'
    THEN deal.CONTQTY END) AS njs内推,
  sum(CASE WHEN deal.partner_id = 'njs' AND refer.refer_1_type = 'External Channel'
    THEN deal.CONTQTY END) AS njs外推,
  sum(CASE WHEN deal.partner_id = 'njs' AND
                (refer.refer_1_type NOT IN ('Internal Channel', 'External Channel') OR refer.refer_1_type IS NULL)
    THEN deal.CONTQTY END) AS njs其他,
  sum(CASE WHEN deal.partner_id = 'hht' AND refer.refer_1_type = 'Internal Channel'
    THEN deal.CONTQTY END) AS hht内推,
  sum(CASE WHEN deal.partner_id = 'hht' AND refer.refer_1_type = 'External Channel'
    THEN deal.CONTQTY END) AS hht外推,
  sum(CASE WHEN deal.partner_id = 'hht' AND
                (refer.refer_1_type NOT IN ('Internal Channel', 'External Channel') OR refer.refer_1_type IS NULL)
    THEN deal.CONTQTY END) AS hht其他
FROM info_silver.ods_history_deal deal
  LEFT JOIN info_silver.dw_user_account refer
    ON deal.firmid = refer.firm_id
WHERE deal.fdate BETWEEN 20170506 AND 20170512;

