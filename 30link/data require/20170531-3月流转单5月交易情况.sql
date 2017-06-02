select * fROM info_silver.rpt_crm_transfer_user_stat
where to_char(submit_time,'yyyymm') = 201703
and to_char(stat_date,'yyyymmdd') between 20170428 and 20170530


SELECT *
FROM info_silver.ods_history_deal
where fdate between '20170501' and '20170531' and  firmid IN ('163170424677247', '163000000034284')





---------------------------------------------------------------------------
select * from info_silver.dw_user_account where user_id = 223770730
