SELECT trans.cur_bia_id,trans.cur_bgroup_id,
  sum(CASE WHEN io.inorout = 'A'
  THEN inoutmoney
           WHEN io.inorout = 'B'
             THEN (-1) * inoutmoney END) AS money
FROM info_silver.ods_crm_transfer_record trans
  JOIN silver_njs.history_transfer@silver_std io
    ON trans.firm_id = io.firmid
  LEFT JOIN info_silver.tb_crm_user user1
    ON trans.user_id = user1.id
WHERE io.fdate BETWEEN 20170401 AND 20170419
      AND trans.process IN (5, 6) AND trans.valid = 1
      AND io.realdate > trans.submit_time
      AND trans.cur_bia_id IN (249)
group by tran.scur_bia_id,trans.cur_bgroup_id;



GROUP BY trans.user_id, trans.firm_id, user1.user_name, trans.submit_time, trans.cur_bgroup_id,trans.cur_bname




select * from info_silver.ods_crm_transfer_record


select  firm_id,submit_time,crm_name,cur_bgroup_id,cur_bname,max(pmec_net_value_sub+pmec_net_in_sub),
sum( case when fdate between 20170327 and 20170331 then (case when inorout='A'then inoutmoney else -inoutmoney end) end ) ,
sum( case when fdate between 20170401 and 20170419 then (case when inorout='A'then inoutmoney else -inoutmoney end) end )
from
(select  firm_id,submit_time,crm_name,cur_bgroup_id,cur_bname,pmec_net_value_sub,pmec_net_in_sub from info_silver.ods_crm_transfer_record a
where valid=1 and process in (5,6) and cur_bgroup_id in (1,7,8,111)
and submit_time<to_date('20170401','yyyymmdd'))a
left join silver_njs.history_transfer@silver_std  b
on a.firm_id=b.firmid  and submit_time<realdate
  where cur_bname in ('孙诗易','王杰君','拓智泷','寇龙','李松波','程凯新','尹钟','王利韦','房志强','王晓燕')
group by firm_id,submit_time,crm_name,cur_bgroup_id,cur_bname

