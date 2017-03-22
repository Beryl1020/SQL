select b.user_id,a.type,b.pmec_net_value_sub+pmec_net_in_sub,b.submit_time,a.create_time

from tb_crm_transfer_record@consult_std b
  left join
  silver_consult.tb_crm_dispatch_his@consult_std a
  on a.user_id=b.user_id
  left join silver_consult.tb_crm_ia@consult_std c
  on b.fia_id = c.id
WHERE
  c.group_id=105
    and c.name <>'袁红'
and b.valid=1 and b.process in (5,6) --105组流转单资源分布









select c.user_id, c.mindistime,d.type from
(
SELECT a.id as id,a.user_id as user_id,a.ia_id as ia_id,min(a.create_time) as mindistime
from
  silver_consult.tb_crm_dispatch_his@consult_std a

  left join silver_consult.tb_crm_ia@consult_std b
  on  a.ia_id=b.id
where b.group_id=105
and to_char(a.create_time,'yyyymmdd') >20170101
group by a.id,a.user_id,a.ia_id) c
  left join silver_consult.tb_crm_dispatch_his@consult_std d
  on c.id=d.id and c.mindistime=d.create_time




select * from silver_consult.tb_crm_dispatch_his@consult_std where user_id = 1000426767
select * from silver_consult.tb_crm_ia@consult_std where id=267
select * from tb_crm_transfer_record@consult_std