SELECT
  aa.date1,
  sum(CASE WHEN cc.trade_time >= aa.submit_time AND
                cc.fdate BETWEEN to_char(aa.submit_time, 'yyyymmdd') AND to_char(aa.submit_time, 'yyyymmdd')
    THEN nvl(cc.contqty,0) END) AS 流转当天交易额,
  sum(CASE WHEN cc.trade_time >= aa.submit_time AND
                cc.fdate BETWEEN to_char(aa.submit_time, 'yyyymmdd') AND to_char(aa.submit_time + 6, 'yyyymmdd')
    THEN nvl(cc.contqty,0) END) AS 流转后2天交易额,
  sum(CASE WHEN cc.trade_time >= aa.submit_time AND
                cc.fdate BETWEEN to_char(aa.submit_time, 'yyyymmdd') AND to_char(aa.submit_time + 13, 'yyyymmdd')
    THEN nvl(cc.contqty,0) END) AS 流转后14天交易额,
  sum(CASE WHEN cc.trade_time >= aa.submit_time AND
                cc.fdate BETWEEN to_char(aa.submit_time, 'yyyymmdd') AND to_char(aa.submit_time + 14, 'yyyymmdd')
    THEN nvl(cc.contqty,0) END) AS 流转15天交易额
FROM
  (SELECT
    submit_time,
     to_char(submit_time, 'yyyymmdd') AS date1,
     firm_id,
     user_id,
     CASE WHEN partner_id = 'hht'
       THEN (hht_net_in_sub + hht_net_value_sub)
     WHEN partner_id = 'pmec'
       THEN (pmec_net_in_sub + pmec_net_value_sub) END as money
   FROM info_silver.ods_crm_transfer_record
   WHERE process IN (5, 6) AND valid = 1
  ) aa
  LEFT JOIN
  info_silver.dw_user_account bb
    ON aa.useR_id = bb.crm_user_id
       AND bb.partner_id IN ('pmec', 'hht')
  LEFT JOIN
  info_silver.ods_history_deal cc
    ON bb.firm_id = cc.firmid
  where aa.date1>='20170101'
group by aa.date1
order by aa.date1


SELECT
     to_char(submit_time, 'yyyymmdd') AS date1,
     sum(CASE WHEN partner_id = 'hht'
       THEN (hht_net_in_sub + hht_net_value_sub)
     WHEN partner_id = 'pmec'
       THEN (pmec_net_in_sub + pmec_net_value_sub) END) as money
   FROM info_silver.ods_crm_transfer_record
   WHERE process IN (5, 6) AND valid = 1
and to_char(submit_time,'yyyymmdd')>=20170101
group by to_char(submit_time,'yyyymmdd')

select to_char(sysdate-1,'day') from dual






SELECT *
FROM info_silver.ods_crm_transfer_record

