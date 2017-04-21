/*流转后首笔交易仓位=流转后首笔交易额/12.5/激活资金*/



SELECT
  a.bia_id,
  a.bname,
  a.bgroup_id,
  a.firm_id,
  a.user_id,
  a.crm_name,
  a.pmec_net_value_sub+a.pmec_net_in_sub as 激活资金,
  sum(e.contqty) as 首笔交易额,
  sum(e.contqty)/(a.pmec_net_value_sub+a.pmec_net_in_sub)/12.5 as 首笔交易仓位,
  e.trade_time as 首笔交易时间
FROM info_silver.ods_crm_transfer_record a
  left JOIN
  (SELECT
     b.firmid,
     min(b.trade_time) as mintime
   FROM info_silver.ods_history_deal b JOIN info_silver.ods_crm_transfer_record c
       ON b.firmid = c.firm_id AND b.trade_time > c.submit_time
   WHERE c.process IN (5, 6) AND c.valid = 1
   GROUP BY b.firmid) d
  ON A.firm_id = d.firmid
  left join info_silver.ods_history_deal e on d.firmid=e.firmid and d.mintime=e.trade_time

WHERE A.process IN (5, 6) AND A.valid = 1 AND to_char(submit_time, 'yyyymmdd') = to_char(SYSDATE - 1, 'yyyymmdd')
  and a.bgroup_id in (1,7,8)
group by a.bia_id,
  a.bname,
  a.bgroup_id,
  a.firm_id,
  a.user_id,
  a.crm_name,e.trade_time,a.pmec_net_value_sub,a.pmec_net_in_sub
order by 首笔交易仓位 desc



select * from info_silver.ods_crm_transfer_record where to_char(submit_time,'yyyymmdd')=20170417


SELECT *
FROM info_silver.ods_history_deal
where firmid='163000000098540'
and to_char(trade_time,'yyyymmdd')='20170418'
