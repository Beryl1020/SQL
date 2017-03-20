select * from ods_history_user@silver_stat_urs_30_link

select sum(amount),to_char(update_time,'yyyymm') as yymm from tb_epay_transfer
  where status=1
group by to_char(update_time,'yyyymm') -- 免佣券


select  to_char(update_time,'yyyymm'),sum(return_money),sum(trade_money) from  silver.tb_silver_freecoupon_detail
  where status=2
group by to_char(update_time,'yyyymm') --其它所有转账（含红包）







select distinct group_id from tb_epay_transfer
select * from silver.tb_silver_freecoupon_detail