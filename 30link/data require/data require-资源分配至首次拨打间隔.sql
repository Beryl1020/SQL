SELECT
  id as 主站id,
  time1 as 分配时间,
  time2 as 接通时间
  from
  (select distinct dis.user_id as id, min(dis.create_time) as time1, min(tel.CREATE_TIME) as time2

  from silver_consult.tb_crm_dispatch_his@consul_std dis
  left join silver_consult.tb_crm_tel_record@consul_std tel
  on dis.user_id = tel.user_id

  left join silver_consult.tb_crm_tag_user_rel@consul_std tag1
  on dis.user_id = tag1.user_id

  where tag1.tag_id = 33

  group by dis.user_id)
where to_char(time1,'yyyymmdd') between 20170227 and 20170303


select * FROM  info_silver.ods_history_deal
