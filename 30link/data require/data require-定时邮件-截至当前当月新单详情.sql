SELECT *
FROM
  (
    SELECT
      to_char(b.submit_time,'yyyymmdd')  as 日期,
      a.group_id                               组别,
      a.name                                   投顾姓名,
      b.fia_id                                 投顾id,
      b.user_id                                用户id,
      b.user_name                              用户姓名,
      b.submit_time                            流转时间,
      nvl(b.net_zcmoney, 0) + nvl(b.net_in, 0) 激活资金
    FROM silver_consult.tb_crm_ia@consul_std a
      JOIN
      (
        SELECT
          a.fia_id,
          a.user_id,
          a.submit_time,
          e.user_name,
          b.net_zcmoney,
          sum(charge_amount) net_in
        FROM (SELECT *
              FROM silver_consult.tb_crm_transfer_record@consul_std
              WHERE trunc(submit_time) = trunc(sysdate) AND process IN (5, 6) ) a
          LEFT JOIN silver_consult.v_tb_crm_user@consul_std e ON a.user_id = e.id
          LEFT JOIN (SELECT *
                     FROM tb_silver_user_stat@silver_std
                     WHERE partner_id = 'hht') d ON e.fa_id = d.user_id
          LEFT JOIN (SELECT
                       firm_id,
                       net_zcmoney
                     FROM info_silver.ods_order_zcmoney
                     WHERE to_char(fdate) = (SELECT CASE WHEN to_char(trunc(sysdate), 'day') = '星期一'
                       THEN (to_char(trunc(sysdate) - 3, 'yyyymmdd'))
                                                    ELSE (to_char(trunc(sysdate) - 1, 'yyyymmdd')) END
                                             FROM dual)) b ON d.firm_id = b.firm_id
          LEFT JOIN NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT c
            ON d.firm_id = c.fund_id AND c.create_time < a.submit_time AND c.create_time > trunc(sysdate)
        GROUP BY a.fia_id, a.user_id, a.submit_time, e.user_name, b.net_zcmoney
      ) b ON a.id = b.fia_id
    UNION
    SELECT
      to_char(b.submit_time,'yyyymmdd') as 日期,
      a.group_id                         组别,
      a.name                             投顾姓名,
      b.fia_id                           投顾id,
      b.user_id                          用户id,
      c.user_name                        用户姓名,
      b.submit_time                      流转时间,
      HHT_NET_VALUE_SUB + HHT_NET_IN_SUB 激活资金
    FROM silver_consult.tb_crm_ia@consul_std a
      JOIN info_silver.ods_crm_transfer_record b ON a.id = b.fia_id
      JOIN silver_consult.v_tb_crm_user@consul_std c ON b.user_id = c.id
    WHERE b.process IN (5, 6) AND b.valid = 1 AND
          substr(to_char(b.submit_time, 'yyyymmdd'), 1, 6) = substr(to_char(sysdate, 'yyyymmdd'), 1, 6)
  )
ORDER BY 流转时间 ASC



select * from info_silver.ods_crm_transfer_record where to_char(submit_time,'yyyymmdd')=20170509 and process in (5,6) and valid=1
select * from silver_consult.tb_crm_transfer_record@consul_std where to_char(submit_time,'yyyymmdd')=20170509 and process in (5,6) and valid=1