SELECT
  a.id AS user_id,
  b.id,
  b.name,
  b.group_id --后端投顾用户列表
FROM
  info_silver.tb_crm_user a
  JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
WHERE b.group_id IN (1, 7, 8 )

SELECT
  distinct b.id,
  b.name,
  b.group_id --后端投顾列表
FROM
  info_silver.tb_crm_user a
  JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
WHERE b.group_id IN (1, 7, 8 )
and b.id  not in (466,473)




select bbb.ia_id,bbb.ia_name,bbb.group_id,
  b1.前日流转拨打间隔,b2.本周流转拨打间隔,b3.本月流转拨打间隔,
  b4.当日流转当日开仓率,b5.本周当日开仓率,b6.本月当日开仓率,
  b7.所有单有效开仓,b8.当月新单有效开仓率,b9.交易率,
  b10.日手续费转化,b11.周手续费转化,b12.月手续费转化
from
  (
    SELECT
  distinct b.id as ia_id,
  b.name as ia_name,
  b.group_id
FROM
  info_silver.tb_crm_user a
  JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
WHERE b.group_id IN (1, 7, 8 )
    and b.id  not in (466,473)
  ) bbb
left join
  (
    SELECT
      aa.ia_id,
      aa.ia_name,
      aa.group_id,
      round(avg(aa.delta), 4) * 24   as   前日流转拨打间隔
    FROM
      (
        SELECT
          user1.user_id,
          user1.ia_id,
          user1.ia_name,
          user1.group_id,
          min(tel.create_time) - trans.submit_time AS delta
        FROM
          (
            SELECT
              a.id   AS user_id,
              b.id   AS ia_id,
              b.name AS ia_name,
              b.group_id
            FROM
              info_silver.tb_crm_user a
              JOIN info_silver.tb_crm_ia b
                ON a.ia_id = b.id
            WHERE b.group_id IN (1, 7, 8 )
          ) user1
          JOIN info_silver.ods_crm_transfer_record trans
            ON user1.user_id = trans.user_id
          JOIN info_silver.tb_crm_tel_record tel
            ON user1.user_id = tel.user_id
               AND tel.create_time > trans.submit_time
        WHERE trans.valid = 1 AND trans.process IN (5, 6)
              AND to_char(trans.submit_time, 'yyyymmdd') = to_char(sysdate - 1, 'yyyymmdd')
        GROUP BY user1.user_id, trans.submit_time, user1.ia_id, user1.ia_name, user1.group_id
      ) aa
    GROUP BY aa.ia_id, aa.group_id, aa.ia_name
  ) b1
  on bbb.ia_id=b1.ia_id and bbb.ia_name=b1.ia_name and bbb.group_id=b1.group_id
left join

  (
    SELECT
      aa.ia_id,
      aa.ia_name,
      aa.group_id,
      round(avg(aa.delta), 4) * 24  as 本周流转拨打间隔
    FROM
      (
        SELECT
          user1.user_id,
          user1.ia_id,
          user1.ia_name,
          user1.group_id,
          min(tel.create_time) - trans.submit_time AS delta
        FROM
          (
            SELECT
              a.id   AS user_id,
              b.id   AS ia_id,
              b.name AS ia_name,
              b.group_id
            FROM
              info_silver.tb_crm_user a
              JOIN info_silver.tb_crm_ia b
                ON a.ia_id = b.id
            WHERE b.group_id IN (1, 7, 8 )
          ) user1
          JOIN info_silver.ods_crm_transfer_record trans
            ON user1.user_id = trans.user_id
          JOIN info_silver.tb_crm_tel_record tel
            ON user1.user_id = tel.user_id
               AND tel.create_time > trans.submit_time
        WHERE trans.valid = 1 AND trans.process IN (5, 6)
              AND to_char(trans.submit_time, 'yyyymmdd')
              BETWEEN to_char(trunc(sysdate - 1, 'd'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
        GROUP BY user1.user_id, trans.submit_time, user1.ia_id, user1.ia_name, user1.group_id
      ) aa
    GROUP BY aa.ia_id, aa.group_id, aa.ia_name
    ) b2
  on bbb.ia_id=b2.ia_id and  bbb.ia_name=b2.ia_name and bbb.group_id=b2.group_id

left join
    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(avg(aa.delta), 4) * 24 as 本月流转拨打间隔
      FROM
        (
          SELECT
            user1.user_id,
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            min(tel.create_time) - trans.submit_time AS delta
          FROM
            (
              SELECT
                a.id   AS user_id,
                b.id   AS ia_id,
                b.name AS ia_name,
                b.group_id
              FROM
                info_silver.tb_crm_user a
                JOIN info_silver.tb_crm_ia b
                  ON a.ia_id = b.id
              WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            JOIN info_silver.tb_crm_tel_record tel
              ON user1.user_id = tel.user_id
                 AND tel.create_time > trans.submit_time
          WHERE trans.valid = 1 AND trans.process IN (5, 6)
                AND to_char(trans.submit_time, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
          GROUP BY user1.user_id, trans.submit_time, user1.ia_id, user1.ia_name, user1.group_id
        ) aa
      GROUP BY  aa.ia_id, aa.group_id, aa.ia_name
      ) b3
    on bbb.ia_id=b3.ia_id and  bbb.ia_name=b3.ia_name and bbb.group_id=b3.group_id

left join
    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(count(CASE WHEN aa.num1 >= 30
          THEN aa.firm_id END) / count(aa.firm_id),5) as 当日流转当日开仓率
      FROM
        (
          SELECT
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id,
            sum(deal.contqty)                                     AS num1,
            avg(trans.pmec_net_value_sub + trans.pmec_net_in_sub) AS num2
          FROM
            (SELECT
               a.id       AS user_id,
               b.id       AS ia_id,
               b.name     AS ia_name,
               b.group_id AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
                 AND to_char(trans.submit_time, 'yyyymmdd') = to_char(deal.trade_time, 'yyyymmdd')
          WHERE to_char(trans.submit_time, 'yyyymmdd') = to_char(sysdate - 1, 'yyyymmdd')
          GROUP BY user1.ia_id, user1.ia_name, user1.group_id, trans.firm_id
        ) aa
      GROUP BY aa.ia_id, aa.ia_name, aa.group_id
      ) b4
    on bbb.ia_id=b4.ia_id and  bbb.ia_name=b4.ia_name and bbb.group_id=b4.group_id
left join

    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(count(CASE WHEN aa.num1 >= 30
          THEN aa.firm_id END) / count(aa.firm_id),5) as 本周当日开仓率
      FROM
        (
          SELECT
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id,
            sum(deal.contqty)                                     AS num1,
            avg(trans.pmec_net_value_sub + trans.pmec_net_in_sub) AS num2
          FROM
            (SELECT
               a.id       AS user_id,
               b.id       AS ia_id,
               b.name     AS ia_name,
               b.group_id AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
                 AND to_char(trans.submit_time, 'yyyymmdd') = to_char(deal.trade_time, 'yyyymmdd')
          WHERE to_char(trans.submit_time, 'yyyymmdd')
          BETWEEN to_char(trunc(sysdate - 1, 'd'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
          GROUP BY user1.ia_id, user1.ia_name, user1.group_id, trans.firm_id
        ) aa
      GROUP BY aa.ia_id, aa.ia_name, aa.group_id
      ) b5
    on bbb.ia_id=b5.ia_id and  bbb.ia_name=b5.ia_name and bbb.group_id=b5.group_id
left join

    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(count(CASE WHEN aa.num1 >= 30
          THEN aa.firm_id END) / count(aa.firm_id),5) as 本月当日开仓率
      FROM
        (
          SELECT
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id,
            sum(deal.contqty)                                     AS num1,
            avg(trans.pmec_net_value_sub + trans.pmec_net_in_sub) AS num2
          FROM
            (SELECT
               a.id       AS user_id,
               b.id       AS ia_id,
               b.name     AS ia_name,
               b.group_id AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
                 AND to_char(trans.submit_time, 'yyyymmdd') = to_char(deal.trade_time, 'yyyymmdd')
          WHERE to_char(trans.submit_time, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
          GROUP BY user1.ia_id, user1.ia_name, user1.group_id, trans.firm_id
        ) aa
      GROUP BY aa.ia_id, aa.ia_name, aa.group_id
      ) b6
    on bbb.ia_id=b6.ia_id and  bbb.ia_name=b6.ia_name and bbb.group_id=b6.group_id
left join

    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(count(CASE WHEN (aa.num1 < 100000 AND aa.num2 >= 30)
                        OR (aa.num1 < 200000 AND aa.num2 >= 60)
                        OR (aa.num1 < 300000 AND aa.num2 >= 120)
                        OR (aa.num1 < 500000 AND aa.num2 >= 180)
                        OR (aa.num1 < 1000000 AND aa.num2 >= 240)
                        OR (aa.num1 < 2000000 AND aa.num2 >= 480)
                        OR (aa.num1 >= 2000000 AND aa.num2 >= 720)
          THEN aa.firm_id END) / count(aa.firm_id),5) as 所有单有效开仓
      FROM
        (
          SELECT
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id,
            sum(deal.contqty)                                     AS num2,
            avg(trans.pmec_net_value_sub + trans.pmec_net_in_sub) AS num1
          FROM
            (SELECT
               a.id       AS user_id,
               b.id       AS ia_id,
               b.name     AS ia_name,
               b.group_id AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
                 AND trunc(to_Date(deal.fdate, 'yyyymmdd'), 'mm') <= add_months(trunc(trans.submit_time, 'mm'), 1)
          GROUP BY
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id
        ) aa
      GROUP BY aa.ia_id,
        aa.ia_name,
        aa.group_id
      ) b7
    on bbb.ia_id=b7.ia_id and  bbb.ia_name=b7.ia_name and bbb.group_id=b7.group_id
left join

    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(count(CASE WHEN (aa.num1 < 100000 AND aa.num2 >= 30)
                        OR (aa.num1 < 200000 AND aa.num2 >= 60)
                        OR (aa.num1 < 300000 AND aa.num2 >= 120)
                        OR (aa.num1 < 500000 AND aa.num2 >= 180)
                        OR (aa.num1 < 1000000 AND aa.num2 >= 240)
                        OR (aa.num1 < 2000000 AND aa.num2 >= 480)
                        OR (aa.num1 >= 2000000 AND aa.num2 >= 720)
          THEN aa.firm_id END) / count(aa.firm_id),5) as 当月新单有效开仓率
      FROM
        (
          SELECT
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id,
            sum(deal.contqty)                                     AS num2,
            avg(trans.pmec_net_value_sub + trans.pmec_net_in_sub) AS num1
          FROM
            (SELECT
               a.id       AS user_id,
               b.id       AS ia_id,
               b.name     AS ia_name,
               b.group_id AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
          WHERE to_char(trans.submit_time, 'yyyymm') = to_char(sysdate - 1, 'yyyymm')
          GROUP BY
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id
        ) aa
      GROUP BY aa.ia_id,
        aa.ia_name,
        aa.group_id
      ) b8
    on bbb.ia_id=b8.ia_id and bbb.ia_name=b8.ia_name and bbb.group_id=b8.group_id

left join


    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(count(DISTINCT CASE
                       WHEN aa.money IS NOT NULL AND aa.money >= 0
                         THEN aa.firm_id END) / count(DISTINCT aa.firm_id),5) as 交易率
      FROM
        (
          SELECT
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id,
            sum(deal.contqty) AS money
          FROM
            (SELECT
               a.id       AS user_id,
               b.id       AS ia_id,
               b.name     AS ia_name,
               b.group_id AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
          GROUP BY user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id
        ) aa
      GROUP BY aa.ia_id,
        aa.ia_name,
        aa.group_id
      ) b9
    on bbb.ia_id=b9.ia_id and bbb.ia_name=b9.ia_name and bbb.group_id=b9.group_id

left join

    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(sum(num2) * 0.00065 / sum(num1),5) as 日手续费转化
      FROM
        (
          SELECT
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id,
            avg(trans.pmec_net_value_sub + trans.pmec_net_in_sub) AS num1,
            sum(deal.contqty)                                     AS num2
          FROM
            (SELECT
               a.id       AS user_id,
               b.id       AS ia_id,
               b.name     AS ia_name,
               b.group_id AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
                 AND deal.fdate = to_char(sysdate - 1, 'yyyymmdd')
          GROUP BY user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id
        ) aa
      GROUP BY aa.ia_id,
        aa.ia_name,
        aa.group_id
      ) b10
    on bbb.ia_id=b10.ia_id and  bbb.ia_name=b10.ia_name and bbb.group_id=b10.group_id
left join


    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(sum(num2) * 0.00065 / sum(num1),5) as 周手续费转化
      FROM
        (
          SELECT
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id,
            avg(trans.pmec_net_value_sub + trans.pmec_net_in_sub) AS num1,
            sum(deal.contqty)                                     AS num2
          FROM
            (SELECT
               a.id       AS user_id,
               b.id       AS ia_id,
               b.name     AS ia_name,
               b.group_id AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
                 AND deal.fdate
                 BETWEEN to_char(trunc(sysdate - 1, 'd'), 'yyyymmdd') AND to_char(sysdate - 1, 'yyyymmdd')
          GROUP BY user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id
        ) aa
      GROUP BY aa.ia_id,
        aa.ia_name,
        aa.group_id
      ) b11
    on bbb.ia_id=b11.ia_id and  bbb.ia_name=b11.ia_name and bbb.group_id=b11.group_id
left join

    (
      SELECT
        aa.ia_id,
        aa.ia_name,
        aa.group_id,
        round(sum(num2) * 0.00065 / sum(num1),5) as 月手续费转化
      FROM
        (
          SELECT
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id,
            avg(trans.pmec_net_value_sub + trans.pmec_net_in_sub) AS num1,
            sum(deal.contqty)                                     AS num2
          FROM
            (SELECT
               a.id       AS user_id,
               b.id       AS ia_id,
               b.name     AS ia_name,
               b.group_id AS group_id
             FROM
               info_silver.tb_crm_user a
               JOIN info_silver.tb_crm_ia b
                 ON a.ia_id = b.id
             WHERE b.group_id IN (1, 7, 8 )
            ) user1
            JOIN info_silver.ods_crm_transfer_record trans
              ON user1.user_id = trans.user_id
            LEFT JOIN info_silver.ods_history_deal deal
              ON trans.firm_id = deal.firmid
                 AND trans.submit_time < deal.trade_time
                 AND substr(deal.fdate, 1, 6) = to_char(sysdate - 1, 'yyyymm')
          GROUP BY user1.ia_id,
            user1.ia_name,
            user1.group_id,
            trans.firm_id
        ) aa
      GROUP BY aa.ia_id,
        aa.ia_name,
        aa.group_id
      ) b12
    on bbb.ia_id=b12.ia_id and  bbb.ia_name=b12.ia_name and bbb.group_id=b12.group_id

order by bbb.group_id,bbb.ia_id









