
          SELECT
            user1.user_id as 用户id,
            user1.user_name as 用户姓名,
            user1.ia_id,
            user1.ia_name,
            user1.group_id,
            count(case when deal.fdate>=to_char(sysdate-7,'yyyymmdd')
              and deal.contqty>0 and deal.contqty is not null then deal.firmid end) AS 近7天交易次数,
            count(case when deal.fdate>=to_char(sysdate-30,'yyyymmdd')
              and deal.contqty>0 and deal.contqty is not null then deal.firmid end) as 近30天交易次数
          FROM
            (SELECT
               a.id       AS user_id,
              a.user_name as user_name,
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
            user1.user_id,
            user1.user_name,
            user1.ia_id,
            user1.ia_name,
            user1.group_id



select * from info_silver.ods_crm_transfer_record where process in (4,5,6) and valid=1 and to_char(submit_time,'yyyymmdd') between 20170401 and 20170403
          and bgroup_id<111