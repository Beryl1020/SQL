SELECT
  a.*,
  b.name     投顾姓名,
  b.group_id 投顾组别
FROM
  (
    SELECT
      a.user_id,
      a.open_account_time,
      b.user_name,
      b.id,
      b.ia_id,
      b.fa_id,
      a.firm_id,
      min(c.create_time) 最早联系时间 FROM
      (SELECT
         user_id,
         open_account_time,
         firm_id
       FROM tb_silver_user_stat@silver_std
       WHERE
         --user_name = 'roynicfan@163.com'
         user_id=41887740
         AND partner_id = 'pmec'
      ) a
      LEFT JOIN
      silver_consult.v_tb_crm_user@consul_std b
        ON a.user_id = b.fa_id
      LEFT JOIN silver_consult.tb_crm_tel_record@consul_std c
        ON b.id = c.user_id AND worksec > 0
    GROUP BY a.user_id, a.open_account_time, b.fa_id, b.id, a.firm_id, b.ia_id, b.user_name) a
  LEFT JOIN silver_consult.tb_crm_ia@consul_std b
    ON a.ia_id = b.id;

select * from silver_consult.tb_crm_tel_record@consul_std where user_id=6403991
