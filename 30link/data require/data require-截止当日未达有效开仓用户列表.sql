select aaa.user_id,aaa.user_name,aaa.ia_id,aaa.ia_name,aaa.group_id
  FROM
    (
      SELECT
        aa.user_id,
        aa.user_name,
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
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
            sum(deal.contqty)                                     AS num2,
            avg(trans.pmec_net_value_sub + trans.pmec_net_in_sub) AS num1
          FROM
            (SELECT
               a.id        AS user_id,
               a.user_name AS user_name,
               b.id        AS ia_id,
               b.name      AS ia_name,
               b.group_id  AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8)
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
          GROUP BY
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            user1.user_id,
            user1.user_name
        ) aa
    ) aaa
where aaa.valid=0