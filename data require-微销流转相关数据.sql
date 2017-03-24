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
      /*+driving_site(b)*/
      a.*,
      b.contnum
    FROM
      (
        SELECT /*+driving_site(b)*/
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




select /*+driving_site(a)*/ * from DW_CRM_TRANS_DETAIL_BF@silver_stat_urs_30_link a  where a.stat_date = trunc(sysdate - 1)
where user_id in (1000055783,1000447623,153977236,1000447413,1000519768,1000501276,1000512315,1000486172,1000491703,1000465512,1000517278,1000525999,1000525777,1000507354,1000447579,1000507428,1000509892,1000523151,1000539077,1000430659,1000437048,1000534212,1000539115,1000461114,1000465470,1000353888,1000525821,1000517103,1000506191,1000454737,1000447485,1000524702,1000485748,1000479315,1000471514,1000483381,1000480130,1000480413,1000394843,1000506644,1000137110,1000472134,1000450486,1000062967,1000468834,1000458353,1000522746,1000491867,1000553941,1000528793,1000507196,1000450531,1000322258,1000525814,1000392564,1000459042,1000452214,1000512513,1000532976,1000472049,1000433678,1000552266,1000448696,1000494457,1000517826,1000453378,1000510171,1000486263,1000490961,1000453373,1000494303,1000495424,1000076480,1000536252,1000462528,1000504295,1000462533,1000486415,1000486017,1000513429,1000475305,1000548872,1000476734,1000509116,1000438947,1000483634,1000498325,1000553143)
and stat_date = trunc(sysdate - 1)

select * from silver_njs.history_transfer where  firmid = 163000000409344