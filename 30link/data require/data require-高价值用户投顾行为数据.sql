
select tel.user_id,
  sum(case when tel.create_time<=tr.submit_time then tel.worksec end) as worksec,
      sum(case when tel.create_time<=tr.submit_time then tel.billsec end) as billsec,
          avg(tr.pmec_net_value_sub+tr.pmec_net_in_sub)
FROM silver_consult.tb_crm_transfer_record@consul_std tr
  JOIN
  silver_consult.tb_crm_tag_user_rel@consul_std tag
  on tr.user_id=tag.user_id
  left join silver_consult.tb_crm_tel_record@consul_std tel
  on tr.user_id=tel.user_id
where tag.tag_id=39
and tr.process in (5,6)
and tr.valid=1
  and tel.user_id is not null
group by tel.user_id  --高价值&流转客户拨打数据



select  distinct tel.user_id,
  count(distinct case
       when trunc(tel.create_time) between trunc(dis.create_time) and trunc(dis.create_time+3) then tel.id end)
  as 拨打次数,
  count(distinct case
       when trunc(tel.create_time) between trunc(dis.create_time) and trunc(dis.create_time+3)
         and tel.worksec=0 then tel.id end) as 未接通次数,
  count(distinct case
       when trunc(tel.create_time) between trunc(dis.create_time) and trunc(dis.create_time+3)
         and tel.billsec>=30 then tel.id end) as 未接通但超过30s次数,
  max(case
       when trunc(tel.create_time) between trunc(dis.create_time) and trunc(dis.create_time+3)
       and tel.worksec=0  then tel.billsec end) as 未接通最长通话时长,
  count(distinct case
       when trunc(tel.create_time) between trunc(dis.create_time) and trunc(dis.create_time+3)
         and tel.worksec>0 then tel.id end) as 接通次数,
  count(distinct case
       when trunc(tel.create_time) between trunc(dis.create_time) and trunc(dis.create_time+3)
         and tel.worksec>=240 then tel.id end) as 接通不少于4min次数
from
   silver_consult.tb_crm_tel_record@consul_std tel
join silver_consult.tb_crm_tag_user_rel@consul_std tag
  on tel.user_id=tag.user_id
  join
     (select min(create_time) as create_time,user_id from silver_consult.tb_crm_dispatch_his@consul_std dis
     where ia_id is not null group by user_id ) dis
  on tel.user_id=dis.user_id
where tag.tag_id=38
group by tel.user_id --高价值客户分配前三天拨打情况



select * from silver_consult.tb_crm_dispatch_his@consul_std -- 分配记录表
select * from silver_consult.tb_crm_tel_record@consul_std --电话表
select * from silver_consult.tb_crm_tag_user_rel@consul_std -- 高价值用户标签表
select * from silver_consult.tb_crm_transfer_record@consul_std -- 流转单表