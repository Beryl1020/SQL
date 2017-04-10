select * from silver_njs.pmec_zj_flow@silver_std
select * from info_silver.tb_crm_user



select a.id,a.user_name,a.fa_id,b.firm_id,
  sum(case when c.changetype in (1,3) then c.amount end) as 手续费,
  sum(case when c.changetype in (8) then c.amount end) as 滞纳金,
  sum(case when c.changetype in (9,10) then c.amount end) as 平仓和结算盈亏
from info_silver.tb_crm_user a
  left join tb_silver_user_stat@silver_std b
  on a.fa_id=b.user_id
  left join silver_njs.pmec_zj_flow@silver_std c
  on b.firm_id=c.loginaccount
where a.user_name in ('蔡金粉','詹远巧','杨永禄','陈达忠','鞠晓刚','赵冬冬','许华')
group by a.id,a.user_name,a.fa_id,b.firm_id   -- 特定用户盈亏情况






select a.id,a.user_name,a.fa_id,b.firm_id,c.*
from info_silver.tb_crm_user a
  left join tb_silver_user_stat@silver_std b
  on a.fa_id=b.user_id
  left join silver_njs.pmec_zj_flow@silver_std c
  on b.firm_id=c.loginaccount
where a.user_name in ('蔡金粉','詹远巧','杨永禄','陈达忠','鞠晓刚','赵冬冬','许华')



select * from info_silver.ods_history_deal
where firmid in ('19921941''28861337''25836734''25836038''25884690''26620637''29911153''29995401''31815384''36658142''163000000241652''36656841''36653433''163000000235949''36693850''163000000275738''39938202''163000000391537''163000000433870')



