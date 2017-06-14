SELECT
  a.record_day,
  c.empid,
  a.user_id,
  a.billsec as 拨打时长,
  a.work_type as 接通状态,
  b.注册地,b.电话归属地,b.电话卡类型
FROM info_silver.ods_crm_tel_record a
join (select crm_user_id,
        max(certno_prov) as 注册地,
        max(phone_prov) as 电话归属地,
      max(phone_isp) as 电话卡类型
      from info_silver.dw_user_account
     where crm_user_id is not null
     group by crm_user_id) b
  on a.user_id = b.crm_user_id
join info_silver.tb_crm_ia c
  on a.ia_id = c.id
where a.record_day between 20170508 and 20170614






    select * from info_silver.ods_crm_tel_record where record_day between 20170508 and 20170614
select * from info_silver.ods_crm_tel_record where to_char(create_time,'yyyymmdd') between 20170508 and 20170614
