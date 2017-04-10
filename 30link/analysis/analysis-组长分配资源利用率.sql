select count(distinct id),to_char(create_time,'yyyymmdd')                    ----所有3.1之后的njs导入资源
from silver_consult.tb_crm_dispatch_his@consul_std
where type=3
and to_char(create_time,'yyyymmdd')>=20170301
group by to_char(create_time,'yyyymmdd')



select count(distinct a.id),to_char(a.create_time,'yyyymmdd')                    ----所有3.1之后的njs导入资源
from silver_consult.tb_crm_dispatch_his@consul_std a
  join silver_consult.tb_crm_transfer_record@consul_std b
  on a.user_id=b.user_id
where a.type=3 and b.process in(5,6)
and to_char(a.create_time,'yyyymmdd')>=20170301
group by to_char(a.create_time,'yyyymmdd')
