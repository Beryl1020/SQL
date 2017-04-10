select * from silver_consult.tb_crm_ia@consul_std


select firm_id,fia_id,fgroup_id,bgroup_id,submit_time from info_silver.ods_crm_transfer_record
where to_char(submit_time,'yyyymmdd') <= 20170331 and process in(5,6) and valid=1
and fgroup_id is null and bgroup_id is NULL


select * from info_silver.ods_crm_transfer_record aa where aa.firm_id=1000525448
join info_silver.ods_history_deal bb
  on aa.firm_id=bb.firmid
where info_silver.ods_history_deal bb

select * from info_silver.ods_history_deal bb where  bb.firmid=1000525448
select * from silver_njs.history_transfer@silver_std where firmid=163000000043701
select * from info_silver.ods_crm_transfer_record aa where aa.firm_id=163000000043701


SELECT
                                                      -- 后端投顾维护用户产出的平台收入
        2                                  AS ID,
        sum(CASE WHEN flow.changetype IN (1, 3)
          THEN flow.amount / 8 * 6.5 * (-1)
            WHEN flow.changetype IN (8)
              THEN flow.amount * (-1)
            WHEN flow.changetype IN (9, 10)
              THEN flow.amount * (-1) END) AS 后端用户平台收入

      FROM silver_njs.pmec_zj_flow@silver_std flow
        JOIN info_silver.ods_crm_transfer_record trans
          ON flow.loginaccount = trans.firm_id
      WHERE trans.cur_bgroup_id IN (1, 7, 8, 111)
            and trans.process IN (5, 6) AND trans.valid = 1
            AND trans.submit_time < flow.createdate
            AND to_char(flow.fdate, 'yyyymmdd') BETWEEN 20170301 AND 20170331

SELECT sum(case when PARTNER_ID = 'pmec' and fdate between 20170325 and 20170331 then CONTQTY END)/5,
  sum(case when fdate between 20170325 and 20170331 then CONTQTY END)/5
  from info_silver.ods_history_deal


select sub1.money,sub2.money,sub3.money, sub3.money*0.00065+sub4.money+sub5.money,sub3.money*0.00065+sub5.money+sub6.money
    FROM

  (select 1 as subid,sum(io.inoutmoney) as money -- 总入金
    from silver_njs.history_transfer@silver_std io
    where io.inorout = 'A' and io.partnerid='pmec' and io.fdate between 20170301and 20170331
    group by 1) sub1
  left join
  (select 2 as subid,sum(case when io.inorout='A' then inoutmoney when io.inorout='B' then (-1)*inoutmoney end ) as money --净入金
   from silver_njs.history_transfer@silver_std io
   where io.partnerid='pmec' and io.fdate between 20170301and 20170331
   group by 2) sub2
  on sub1.subid<>sub2.subid
  left join
  (select    3 as subid,sum(deal.CONTQTY) as money -- 总交易额
   from info_silver.ods_history_deal deal
   where deal.partner_id='pmec' and deal.fdate between 20170301and 20170331

   group by 3) sub3
  on sub1.subid<>sub3.subid
  left join
   (select   4 as subid,
   sum(case
       when deal.wareid = 'GDAG' then deal.contnum*8
       when deal.wareid = 'GDPD' then deal.contnum*1000*0.48
       when deal.wareid = 'GDPT' then deal.contnum*1000*0.5 end) as money -- 点差
  from info_silver.ods_history_deal deal
  where deal.partner_id ='pmec'
  and deal.operation_src = 'open'
  and deal.fdate between 20170301and 20170331
  group by 4) sub4
    ON sub1.subid<>sub4.subid
  left join
    (select 5 as subid, sum(case when flow.changetype=8 then (-1)*flow.AMOUNT end) as money -- 滞纳金
     from silver_njs.pmec_zj_flow@silver_std flow
     where to_char(flow.fdate,'yyyymmdd') between 20170301and 20170331
     group by 5) sub5
    ON sub1.subid<>sub5.subid
  left JOIN
    (select 6 as subid, sum(case when flow.changetype in (9,10) then (-1)*flow.amount end) as money -- 头寸+点差
     from silver_njs.pmec_zj_flow@silver_std flow
      where to_char(flow.fdate,'yyyymmdd') between 20170301and 20170331
      group by 6) sub6
    on sub1.subid<>sub6.subid