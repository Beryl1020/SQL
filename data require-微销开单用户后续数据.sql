select
  aaa.id,  -- 用户交易商id
  aaa.user_id, -- 用户id
  aaa.name, -- 用户姓名
  max(aaa.asset), -- 开单时激活资金
  sum(case
      when inout.inorout='A' and (inout.fdate > to_char(trans.submit_time,'yyyymmdd')
      or
      (inout.fdate = to_char(trans.submit_time,'yyyymmdd') and inout.ftime > to_char(trans.submit_time,'hh24miss')))
    then inout.inoutmoney
      when inout.inorout='B' and (inout.fdate > to_char(trans.submit_time,'yyyymmdd')
      or
      (inout.fdate = to_char(trans.submit_time,'yyyymmdd') and inout.ftime > to_char(trans.submit_time,'hh24miss')))
        then inout.inoutmoney*(-1) end),-- 开单后净入金
  max(asset.NET_ASSETS), -- 现净资产
  max(user30.sub_refer), -- 内推/外推
  max(user1.OPEN_ACCOUNT_TIME) -- 开户时间

from
  (select distinct trans.firm_id as id,trans.user_id as user_id,trans.crm_name as name,
     trans.PMEC_NET_VALUE_SUB+trans.PMEC_NET_IN_SUB as asset
    from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
    where trans.process in (5,6) and trans.valid=1 and trans.fNAME<>'孟雪莹'
  and trans.FGROUP_ID=106) aaa
  left join
  info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
  on aaa.id=trans.firm_id
  left JOIN
  silver_njs.history_transfer inout
  on trans.firm_id=inout.firmid

  left JOIN
  silver_njs.tb_silver_data_center asset
  on trans.firm_id=asset.firmid
  and hdate=20170314
  left JOIN
  ods_history_user@silver_stat_urs_30_link user30
  on trans.firm_id=user30.firm_id
  left JOIN
  tb_silver_user_stat user1
  on trans.firm_id=user1.firm_id


where trans.process in (5,6) and trans.valid=1 and trans.fNAME<>'孟雪莹'
  and trans.FGROUP_ID=106

group by
  aaa.id,
  aaa.user_id,
  aaa.name -- 其他字段


select
  trans.firm_id,
  sum(case
      when (deal.fdate > to_char(trans.submit_time,'yyyymmdd')
      or
      (deal.fdate = to_char(trans.submit_time,'yyyymmdd') and deal.ftime > to_char(trans.submit_time,'hh24miss')))

    then deal.contqty end)
from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link trans
left JOIN
  ods_history_deal@silver_stat_urs_30_link deal
  on trans.firm_id=deal.firmid
where trans.process in (5,6) and trans.valid=1
  and trans.FGROUP_ID=106 and trans.fname<>'孟雪莹'
group  by trans.firm_id -- 总交易额


select * from ods_history_user@silver_stat_urs_30_link


select * from cx_tmp2@silver_stat_urs_30_link