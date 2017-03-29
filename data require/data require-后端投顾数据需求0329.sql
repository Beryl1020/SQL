select a.id as user_id,b.id,b.name,b.group_id                   --后端投顾用户列表
from
  tb_crm_user@silver_stat_urs_30_link a
join tb_crm_ia@silver_stat_urs_30_link b
  on a.ia_id=b.id
where b.group_id in (1,7,8,111)



select user1.user_id,
  min(tel.create_time)-trans.submit_time
  from
    (
      SELECT
        a.id AS user_id,
        b.id as ia_id,
        b.name as ia_name,
        b.group_id
      FROM
        tb_crm_user@silver_stat_urs_30_link a
        JOIN tb_crm_ia@silver_stat_urs_30_link b
          ON a.ia_id = b.id
      WHERE b.group_id IN (1, 7, 8, 111)
    ) user1
join info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
on user1.user_id=trans.user_id
join tb_crm_tel_record@silver_stat_urs_30_link tel
      on user1.user_id=tel.user_id
          and tel.create_time>trans.submit_time
group by user1.user_id,trans.submit_time
















select * from tb_crm_ia@silver_stat_urs_30_link
select * from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans