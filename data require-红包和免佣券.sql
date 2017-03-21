select * from ods_history_user@silver_stat_urs_30_link

select sum(case when substr(group_id,1,4)<>'task' then amount end) as redpacket,
  sum(case when substr(group_id,1,4)='task' then amount end) as 人工转账,
      to_char(update_time,'yyyymm') as yymm from tb_epay_transfer
  where status=1

group by to_char(update_time,'yyyymm') --其它所有转账（含红包）


select  to_char(update_time,'yyyymm'),sum(return_money),sum(trade_money) from  silver.tb_silver_freecoupon_detail
  where status=2
group by to_char(update_time,'yyyymm') --免佣券





-- 校验

select distinct substr(group_id,1,4)
from tb_epay_transfer
select * from silver.tb_silver_freecoupon_detail