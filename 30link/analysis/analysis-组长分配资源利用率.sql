select count(distinct id),to_char(create_time,'yyyymmdd')                    ----所有3.1之后的njs导入资源
from silver_consult.tb_crm_dispatch_his@consul_std
where type=3
and to_char(create_time,'yyyymmdd')>=20170301
group by to_char(create_time,'yyyymmdd')



select count(distinct a.id),to_char(a.create_time,'yyyymmdd')                    ----所有3.1之后的njs导入资源且流转单
from silver_consult.tb_crm_dispatch_his@consul_std a
  join silver_consult.tb_crm_transfer_record@consul_std b
  on a.user_id=b.user_id
where a.type=3 and b.process in(5,6)
and to_char(a.create_time,'yyyymmdd')>=20170301
group by to_char(a.create_time,'yyyymmdd')




select * from info_silver.tb_crm_dispatch_his where ia_id in
                                                    (647,151,657,131,678,530,202,732,735,259,287,792,261,610,361,374,621,421,927,596,767,167,731,899,608,884,885,663,656,883,733,153,529,160,528,924,219,791,154,640)
and to_char(create_time,'yyyymmdd')=20170331
select * from info_silver.tb_crm_ia where id in (151,131,530,732,259,261,361,621,927,596,167,899,884,663,883,153,529,160,528,924,640)
)

