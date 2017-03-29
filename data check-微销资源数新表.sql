select * from silver_consult.tb_crm_user_wechat_info@consult_std --where ia_alias='wyy4029';
select * from silver_consult.tb_crm_wechatinfo@consult_std;
select * from tb_crm_transfer_record@consult_std;
select * from silver_consult.tb_crm_ia@consult_std where id=608;
select * from silver_consult.tb_crm_dispatch_his@consult_std;
select * from tb_silver_user_stat;
select * from silver_consult.v_tb_crm_user@consult_std;

--每日没人 wk99518去掉
select '内推' flag,w.name 投顾,w.ia_alias,a.新增好友,b.总计好友,c.群个数,d.新增意向客户数,e.意向客户数,i.新增开户,h.新增出单,h.新增出单金额,j.累计出单,j.累计出单金额,j.累计单均净值,j.累计出单/b.总计好友 资源转化率
from
(select distinct a.iaid,a.weid ia_alias,b.name,b.group_id from silver_consult.tb_crm_wechatinfo@consult_std a join silver_consult.tb_crm_ia@consult_std b on a.iaid=b.id where b.group_id in(113,114) or a.weid='fc163168') w
left join
(select ia_alias,count(1) 新增好友 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'A%' or wechat_proto_comment like 'a%') and type=1 and trunc(create_time)=trunc(sysdate)-1 and ia_alias is not null group by ia_alias) a
on w.ia_alias=a.ia_alias
left join
(select ia_alias,count(1) 总计好友 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'A%' or wechat_proto_comment like 'a%') and type=1 and ia_alias is not null group by ia_alias) b
on w.ia_alias=b.ia_alias
left join
(select ia_alias,count(1) 群个数 from silver_consult.tb_crm_user_wechat_info@consult_std where wechat_nick_name like '%网易%交流群%' and type=3 and ia_alias is not null group by ia_alias) c
on w.ia_alias=c.ia_alias
left join
(select ia_alias,count(1) 新增意向客户数 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'A%意向%' or wechat_proto_comment like 'a%意向%') and type=1 and trunc(create_time)=trunc(sysdate)-1 and ia_alias is not null group by ia_alias) d
on w.ia_alias=d.ia_alias
left join
(select ia_alias,count(1) 意向客户数 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'A%意向%' or wechat_proto_comment like 'a%意向%') and type=1 group by ia_alias) e
on w.ia_alias=e.ia_alias
left join
(select fia_id,count(1) 新增出单,sum(PMEC_NET_VALUE_SUB)+sum(PMEC_NET_IN_SUB) 新增出单金额 from tb_crm_transfer_record@consult_std where trunc(submit_time)=trunc(sysdate)-1 and process in (5,6) group by fia_id ) h
on w.iaid=h.fia_id
left join
(select fia_id,count(1) 累计出单,sum(PMEC_NET_VALUE_SUB)+sum(PMEC_NET_IN_SUB) 累计出单金额, avg(PMEC_NET_VALUE_SUB)+avg(PMEC_NET_IN_SUB) 累计单均净值 from tb_crm_transfer_record@consult_std where trunc(submit_time)<=trunc(sysdate)-1 and process in (5,6) group by fia_id ) j
on w.iaid=j.fia_id
left join (select a.ia_id,count(1) 新增开户 from silver_consult.v_tb_crm_user@consult_std a join tb_silver_user_stat b on a.fa_id=b.user_id where trunc(b.open_account_time)=trunc(sysdate)-1 group by a.ia_id) i
on w.iaid=i.ia_id
union
select '外推' flag,w.name 投顾,w.ia_alias,a.新增好友,b.总计好友,c.群个数,d.新增意向客户数,e.意向客户数,i.新增开户,h.新增出单,h.新增出单金额,j.累计出单,j.累计出单金额,j.累计单均净值,j.累计出单/b.总计好友 资源转化率
from
(select distinct a.iaid,a.weid ia_alias,b.name,b.group_id from silver_consult.tb_crm_wechatinfo@consult_std a join silver_consult.tb_crm_ia@consult_std b on a.iaid=b.id where b.group_id in(112) and a.weid!='wk99518') w
left join
(select ia_alias,count(1) 新增好友 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'W%' or wechat_proto_comment like 'w%') and type=1 and trunc(create_time)=trunc(sysdate)-1 and ia_alias is not null group by ia_alias) a
on w.ia_alias=a.ia_alias
left join
(select ia_alias,count(1) 总计好友 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'W%' or wechat_proto_comment like 'w%') and type=1 and ia_alias is not null group by ia_alias) b
on w.ia_alias=b.ia_alias
left join
(select ia_alias,count(1) 群个数 from silver_consult.tb_crm_user_wechat_info@consult_std where wechat_nick_name like '%网易%交流群%' and type=3 and ia_alias is not null group by ia_alias) c
on w.ia_alias=c.ia_alias
left join
(select ia_alias,count(1) 新增意向客户数 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'W%意向%' or wechat_proto_comment like 'w%意向%') and type=1 and trunc(create_time)=trunc(sysdate)-1 and ia_alias is not null group by ia_alias) d
on w.ia_alias=d.ia_alias
left join
(select ia_alias,count(1) 意向客户数 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'W%意向%' or wechat_proto_comment like 'w%意向%') and type=1 group by ia_alias) e
on w.ia_alias=e.ia_alias
left join
(select fia_id,count(1) 新增出单,sum(PMEC_NET_VALUE_SUB)+sum(PMEC_NET_IN_SUB) 新增出单金额 from tb_crm_transfer_record@consult_std where trunc(submit_time)=trunc(sysdate)-1 and process in (5,6) group by fia_id ) h
on w.iaid=h.fia_id
left join
(select fia_id,count(1) 累计出单,sum(PMEC_NET_VALUE_SUB)+sum(PMEC_NET_IN_SUB) 累计出单金额, avg(PMEC_NET_VALUE_SUB)+avg(PMEC_NET_IN_SUB) 累计单均净值 from tb_crm_transfer_record@consult_std where trunc(submit_time)<=trunc(sysdate)-1 and process in (5,6) group by fia_id ) j
on w.iaid=j.fia_id
left join (select a.ia_id,count(1) 新增开户 from silver_consult.v_tb_crm_user@consult_std a join tb_silver_user_stat b on a.fa_id=b.user_id where trunc(b.open_account_time)=trunc(sysdate)-1 group by a.ia_id) i
on w.iaid=i.ia_id
;

--每日每组
select w.group_id,sum(a.新增好友) 新增好友,sum(b.总计好友) 总计好友,sum(c.群个数) 群个数,sum(d.新增意向客户数) 新增意向客户数,sum(e.意向客户数) 意向客户数,sum(i.新增开户) 新增开户,sum(h.新增出单) 新增出单,sum(h.新增出单金额) 新增出单金额,sum(j.累计出单) 累计出单,sum(j.累计出单金额) 累计出单金额,sum(j.累计单均净值) 累计单均净值,sum(j.累计出单)/sum(b.总计好友) 资源转化率
from
(select distinct a.iaid,a.weid ia_alias,b.name,b.group_id from silver_consult.tb_crm_wechatinfo@consult_std a join silver_consult.tb_crm_ia@consult_std b on a.iaid=b.id where b.group_id in(113,114)) w
left join
(select ia_alias,count(1) 新增好友 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'A%' or wechat_proto_comment like 'a%') and type=1 and trunc(create_time)=trunc(sysdate)-1 and ia_alias is not null group by ia_alias) a
on w.ia_alias=a.ia_alias
left join
(select ia_alias,count(1) 总计好友 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'A%' or wechat_proto_comment like 'a%') and type=1 and ia_alias is not null group by ia_alias) b
on w.ia_alias=b.ia_alias
left join
(select ia_alias,count(1) 群个数 from silver_consult.tb_crm_user_wechat_info@consult_std where wechat_nick_name like '%网易%交流群%' and type=3 and ia_alias is not null group by ia_alias) c
on w.ia_alias=c.ia_alias
left join
(select ia_alias,count(1) 新增意向客户数 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'A%意向%' or wechat_proto_comment like 'a%意向%') and type=1 and trunc(create_time)=trunc(sysdate)-1 and ia_alias is not null group by ia_alias) d
on w.ia_alias=d.ia_alias
left join
(select ia_alias,count(1) 意向客户数 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'A%意向%' or wechat_proto_comment like 'a%意向%') and type=1 group by ia_alias) e
on w.ia_alias=e.ia_alias
left join
(select fia_id,count(1) 新增出单,sum(PMEC_NET_VALUE_SUB)+sum(PMEC_NET_IN_SUB) 新增出单金额 from tb_crm_transfer_record@consult_std where trunc(submit_time)=trunc(sysdate)-1 and process in (5,6) group by fia_id ) h
on w.iaid=h.fia_id
left join
(select fia_id,count(1) 累计出单,sum(PMEC_NET_VALUE_SUB)+sum(PMEC_NET_IN_SUB) 累计出单金额, avg(PMEC_NET_VALUE_SUB)+avg(PMEC_NET_IN_SUB) 累计单均净值 from tb_crm_transfer_record@consult_std where trunc(submit_time)<=trunc(sysdate)-1 and process in (5,6) group by fia_id ) j
on w.iaid=j.fia_id
left join (select a.ia_id,count(1) 新增开户 from silver_consult.v_tb_crm_user@consult_std a join tb_silver_user_stat b on a.fa_id=b.user_id where trunc(b.open_account_time)=trunc(sysdate)-1 group by a.ia_id) i
on w.iaid=i.ia_id
group by w.group_id
union
select w.group_id,sum(a.新增好友),sum(b.总计好友),sum(c.群个数),sum(d.新增意向客户数),sum(e.意向客户数),sum(i.新增开户),sum(h.新增出单),sum(h.新增出单金额),sum(j.累计出单),sum(j.累计出单金额),sum(j.累计单均净值),sum(j.累计出单)/sum(b.总计好友) 资源转化率
from
(select distinct a.iaid,a.weid ia_alias,b.name,b.group_id from silver_consult.tb_crm_wechatinfo@consult_std a join silver_consult.tb_crm_ia@consult_std b on a.iaid=b.id where b.group_id in(112)) w
left join
(select ia_alias,count(1) 新增好友 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'W%' or wechat_proto_comment like 'w%') and type=1 and trunc(create_time)=trunc(sysdate)-1 and ia_alias is not null group by ia_alias) a
on w.ia_alias=a.ia_alias
left join
(select ia_alias,count(1) 总计好友 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'W%' or wechat_proto_comment like 'w%') and type=1 and ia_alias is not null group by ia_alias) b
on w.ia_alias=b.ia_alias
left join
(select ia_alias,count(1) 群个数 from silver_consult.tb_crm_user_wechat_info@consult_std where wechat_nick_name like '%网易%交流群%' and type=3 and ia_alias is not null group by ia_alias) c
on w.ia_alias=c.ia_alias
left join
(select ia_alias,count(1) 新增意向客户数 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'W%意向%' or wechat_proto_comment like 'w%意向%') and type=1 and trunc(create_time)=trunc(sysdate)-1 and ia_alias is not null group by ia_alias) d
on w.ia_alias=d.ia_alias
left join
(select ia_alias,count(1) 意向客户数 from silver_consult.tb_crm_user_wechat_info@consult_std where (wechat_proto_comment like 'W%意向%' or wechat_proto_comment like 'w%意向%') and type=1 group by ia_alias) e
on w.ia_alias=e.ia_alias
left join
(select fia_id,count(1) 新增出单,sum(PMEC_NET_VALUE_SUB)+sum(PMEC_NET_IN_SUB) 新增出单金额 from tb_crm_transfer_record@consult_std where trunc(submit_time)=trunc(sysdate)-1 and process in (5,6) group by fia_id ) h
on w.iaid=h.fia_id
left join
(select fia_id,count(1) 累计出单,sum(PMEC_NET_VALUE_SUB)+sum(PMEC_NET_IN_SUB) 累计出单金额, avg(PMEC_NET_VALUE_SUB)+avg(PMEC_NET_IN_SUB) 累计单均净值 from tb_crm_transfer_record@consult_std where trunc(submit_time)<=trunc(sysdate)-1 and process in (5,6) group by fia_id ) j
on w.iaid=j.fia_id
left join (select a.ia_id,count(1) 新增开户 from silver_consult.v_tb_crm_user@consult_std a join tb_silver_user_stat b on a.fa_id=b.user_id where trunc(b.open_account_time)=trunc(sysdate)-1 group by a.ia_id) i
on w.iaid=i.ia_id
group by w.group_id
;


