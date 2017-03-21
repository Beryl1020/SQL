select a.*,create_time,(后端承接时间-b.create_time)*24*60 承接间隔 from
(select a.*,b.contnum 首日有效交易公斤 from
(select id,crm_name 用户姓名,user_id,submit_time 流转时间,
dispatch_time 后端承接时间,
round((dispatch_time-submit_time)*24*60,1) 承接间隔,
jhzj 激活资金,contnum 交易公斤数,
sum(case when inorout='A' then inoutmoney end )总入金,
sum(case when inorout='B' then inoutmoney end )总出金,
min(case when inorout='B' then realdate end )首次出金时间,
min(case when inorout='A' then realdate end )首次入金时间
 from
   (SELECT
      a.*,
      b.contnum
    FROM
      (
        SELECT /*+driving_site(a)*/ /*+driving_site(b)*/
          id,
          crm_name,
          user_id,
          fname,
          submit_time,
          dispatch_time,
          pmec_net_in_sub + pmec_net_value_sub jhzj,
          firm_id
        FROM info_silver.ods_crm_transfer_record@silver_stat_urs_30_link a
        WHERE fgroup_id in (112,113,114) AND fia_id <> 154 AND process IN (5, 6) AND valid = 1
              AND submit_time > to_date('20170208 14:00:00', 'yyyymmdd hh24:mi:ss')
      ) a LEFT JOIN DW_CRM_TRANS_DETAIL_BF@silver_stat_urs_30_link b
        ON a.user_id = b.user_id AND b.stat_date = trunc(sysdate - 1)
   )a left join silver_njs.history_transfer b
on a.firm_id=b.firmid
and b.realdate>a.submit_time
and b.fdate<=to_char(sysdate-1,'yyyymmdd')
group by id,crm_name,user_id,fname,submit_time,dispatch_time,jhzj,firm_id,contnum)a left join
 DW_CRM_TRANS_DETAIL_BF@SILVER_STAT_URS_30_LINK b
 on a.user_id=b.user_id and trunc(a.流转时间)=b.stat_date)a
 left join tb_crm_transfer_record_his@SILVER_STAT_URS_30_LINK b
 on b.process=3 and  a.id=b.transfer_record_id

select * from DW_CRM_TRANS_DETAIL_BF@silver_stat_urs_30_link













select /*+driving_site(a)*/ /*+driving_site(b)*/ id,crm_name 用户姓名,user_id,submit_time 流转时间,
dispatch_time 后端承接时间,
round((dispatch_time-submit_time)*24*60,1) 承接间隔,
jhzj 激活资金,contnum 交易公斤数,
sum(case when b.inorout='A' then b.inoutmoney end )总入金,
sum(case when b.inorout='B' then b.inoutmoney end )总出金,
min(case when b.inorout='B' then b.realdate end )首次出金时间,
min(case when b.inorout='A' then b.realdate end )首次入金时间
 from
   (SELECT
     /*+driving_site(a)*/ /*+driving_site(b)*/
      a.*,
      b.contnum
    FROM
      (
        SELECT /*+driving_site(a)*/ /*+driving_site(b)*/
          id,
          crm_name,
          user_id,
          fname,
          submit_time,
          dispatch_time,
          pmec_net_in_sub + pmec_net_value_sub jhzj,
          firm_id
        FROM info_silver.ods_crm_transfer_record@silver_stat_urs_30_link a
        WHERE fgroup_id in (112,113,114) AND fia_id <> 154 AND process IN (5, 6) AND valid = 1
              AND submit_time > to_date('20170208 14:00:00', 'yyyymmdd hh24:mi:ss')
      ) a LEFT JOIN DW_CRM_TRANS_DETAIL_BF@silver_stat_urs_30_link b
        ON a.user_id = b.user_id AND b.stat_date = trunc(sysdate - 1)
   )a left join silver_njs.history_transfer b
on a.firm_id>b.firmid
and b.realdate>a.submit_time
and b.fdate<=to_char(sysdate-1,'yyyymmdd')
group by id,crm_name,user_id,fname,submit_time,dispatch_time,jhzj,firm_id,contnum

select * from silver_njs.history_transfer


select * from silver_njs.history_transfer where  firmid = 163000000409344