SELECT
  aaa.user_id     AS 主站id,
  aaa.user_name   AS 用户姓名,
  aaa.ia_id       AS 投顾id,
  aaa.ia_name     AS 投顾姓名,
  aaa.group_id    AS 组别,
  aaa.submit_time AS 流转时间
FROM
  (
    SELECT
      aa.user_id,
      aa.user_name,
      aa.ia_id,
      aa.ia_name,
      aa.group_id,
      aa.submit_time,
      CASE WHEN (aa.num1 < 100000 AND aa.num2 >= 30)
                OR (aa.num1 < 200000 AND aa.num2 >= 60)
                OR (aa.num1 < 300000 AND aa.num2 >= 120)
                OR (aa.num1 < 500000 AND aa.num2 >= 180)
                OR (aa.num1 < 1000000 AND aa.num2 >= 240)
                OR (aa.num1 < 2000000 AND aa.num2 >= 480)
                OR (aa.num1 >= 2000000 AND aa.num2 >= 720)
        THEN 1
      ELSE 0 END AS valid
    FROM
      (
        SELECT
          user1.ia_id,
          user1.ia_name,
          user1.group_id,
          user1.user_id,
          user1.user_name,
          trans.submit_time,
          sum(CASE WHEN deal.wareid = 'LSAG100g'
            THEN deal.contnum
              WHEN deal.wareid = 'GDAG'
                THEN deal.contnum
              WHEN deal.wareid = 'GDPD'
                THEN deal.contnum * 30
              WHEN deal.wareid = 'GDPT'
                THEN deal.contnum * 56 END)                              AS num2,
          avg(CASE WHEN trans.partner_id = 'pmec'
            THEN trans.pmec_net_value_sub + trans.pmec_net_in_sub
              WHEN trans.partner_id = 'hht'
                THEN trans.hht_net_value_sub + trans.hht_net_in_sub END) AS num1
        FROM
          (SELECT
             a.user_id   AS user_id,
             a.crm_user_id,
             a.real_name AS user_name,
             a.ia_id     AS ia_id,
             a.ia_name   AS ia_name,
             a.group_id  AS group_id,
             a.firm_id   AS firm_id
           FROM
             info_silver.dw_user_account a
           WHERE a.group_id IN (1, 7, 8, 111, 118)
                 AND a.partner_id IN ('hht', 'pmec')
          ) user1
          JOIN info_silver.ods_crm_transfer_record trans
            ON user1.crm_user_id = trans.user_id
               AND trans.process IN (5, 6) AND trans.valid = 1
          LEFT JOIN info_silver.ods_history_deal deal
            ON user1.firm_id = deal.firmid
               AND trans.submit_time < deal.trade_time
               AND to_char(deal.trade_time, 'yyyymm') <= to_char(trans.submit_time, 'yyyymm') + 1
        GROUP BY
          user1.ia_id,
          user1.ia_name,
          user1.group_id,
          user1.user_id,
          user1.user_name,
          trans.submit_time
      ) aa
  ) aaa
WHERE aaa.valid = 0
order by aaa.submit_time desc