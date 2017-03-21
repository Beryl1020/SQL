select substr(a.birth_day,1,4),
  count(distinct  c.firm_id ) as 开户人数,
  sum(case
      when trunc(d.realdate)<=trunc(c.open_account_time)+30 and d.inorout='A' then d.inoutmoney
      when trunc(d.realdate)<=trunc(c.open_account_time)+30 and d.inorout='B' then d.inoutmoney*(-1) end) as 开户30天净入金
  -- avg(case
   --   when e.hdate<=to_char(c.open_account_time,'yyyymmdd')+30 then e.net_Assets end) as 开户30天平均净资产
  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
    left join silver_njs.history_transfer d
    on b.firmid=d.firmid
    --left join silver_njs.tb_silver_data_center e
    --on b.firmid=e.firmid
  where a.birth_day<=20170101
    and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
  group by substr(a.birth_day,1,4)
order by substr(a.birth_day,1,4) DESC


select /*+driving_site(a)*/ /*+driving_site(b)*/substr(a.birth_day,1,4),
  count(distinct c.firm_id) as 开户人数,
  --sum(case
    --  when d.fdate<=to_char(c.open_account_time,'yyyymmdd')+30 and d.inorout='A' then d.inoutmoney
    --  when d.fdate<=to_char(c.open_account_time,'yyyymmdd')+30 and d.inorout='B' then d.inoutmoney*(-1) end) as 开户30天净入金
 sum(case
   when e.hdate=20170320 then e.net_Assets end) as 开户30天平均净资产
  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
    --join silver_njs.history_transfer d
    --on b.firmid=d.firmid
   join silver_njs.tb_silver_data_center e
    on b.firmid=e.firmid
    and e.hdate>=20170101
  where a.birth_day<=20170101
    and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
  group by substr(a.birth_day,1,4)
order by substr(a.birth_day,1,4) DESC


select /*+driving_site(a)*/ /*+driving_site(b)*/ /*+driving_site(d)*/
  substr(a.birth_day,1,4),
  count(distinct  c.firm_id ) as 开户人数,
  sum(case
      when trunc(d.trade_time)<=trunc(c.open_account_time)+30  then d.contqty end) as 开户30天交易额

  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
    join ods_history_deal@silver_stat_urs_30_link  d
    on b.firmid=d.firmid
  where a.birth_day<=20170101
    and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
  group by substr(a.birth_day,1,4)
order by substr(a.birth_day,1,4) DESC










select * from ods_history_deal@silver_stat_urs_30_link
select * from tb_crm_user@silver_stat_urs_30_link
select * from  info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK -- 取客户编码（巨长那个）
select * from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK --取用户年龄
select * from silver_njs.tb_silver_data_center
select * from silver_njs.history_transfer