


select a.firmid,
  sum(case when a.inorout='A' then inoutmoney end) as moneyin,
  sum(case when a.inorout='B' then inoutmoney*(-1) end) as moneyout

from silver_njs.history_transfer@silver_std a
  join  info_silver.ods_crm_transfer_record   b
  on a.firmid=b.firm_id

where a.fdate between 20170325 and 20170331

and a.partnerid='pmec'
and a.realdate>b.submit_time
group by a.firmid

-- 后端通话情况各月份对比

select b.group_id,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170304 and 20170310 then a.id end) as a,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170311 and 20170317 then a.id end) as a,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170318 and 20170324 then a.id end) as a,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170325 and 20170331 then a.id end) as b,
count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170401 and 20170407 then a.id end) as c,
  count(distinct case when to_char(a.create_time,'yyyymmdd') between 20170408 and 20170414 then a.id end) as d
from info_silver.tb_crm_tel_record a
join info_silver.tb_crm_ia  b
  on a.ia_id=b.id and b.group_id in(1,7,8,111)
where to_char(a.create_time,'yyyymmdd') between 20170301 and 20170415
  and a.worksec>=240
group by b.group_id





select count(distinct a.firmid),to_char(c.submit_time,'yyyymm')
  from
    (select aaa.firmid, min(aaa.realdate) as mindate
  from
  (select suba.firmid as firmid, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate

  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firmid
    )a

    join info_silver.ods_crm_transfer_record c
      on a.firmid=c.firm_id and c.process in (5,6) and c.valid=1
where
  to_char(a.mindate, 'yyyymm') = '201609'         /*首次入金时间*/
group by to_char(c.submit_time,'yyyymm')





select count(distinct a.firmid),sum(b.contqty),to_char(a.mindate, 'yyyymm')
  from
    (select aaa.firmid, min(aaa.realdate) as mindate
  from
  (select suba.firmid as firmid, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate

  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firmid
    )a
    join info_silver.ods_history_deal b
    on a.firmid=b.firmid
    and substr(b.fdate,1,6) = '201609' /*交易时间*/
AND
    (a.firmid NOT IN
     (SELECT firm_id
      FROM info_silver.ods_crm_transfer_record
      WHERE process IN (5, 6) AND valid = 1
            AND to_char(submit_time, 'yyyymm') < '201610') /*流转时间*/
    )
group by to_char(a.mindate, 'yyyymm');





select count(distinct a.firmid),sum(b.contqty),to_char(a.mindate, 'yyyymm')
  from
    (select aaa.firmid, min(aaa.realdate) as mindate
  from
  (select suba.firmid as firmid, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate

  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firmid
    )a
    join info_silver.ods_history_deal b
    on a.firmid=b.firmid
    and substr(b.fdate,1,6) = '201609'                           /*交易时间*/
    join info_silver.ods_crm_transfer_record c
      on a.firmid=c.firm_id and c.process in (5,6) and c.valid=1
    and to_char(c.submit_time,'yyyymm') = '201609'               /*流转时间*/

and c.submit_time > b.trade_time
group by to_char(a.mindate, 'yyyymm') ;                          /*首次入金时间*/









select * from silver_consult.DM_IA_USER_TRANSFER_STAT@consul_std

select * from info_silver.ods_history_deal where firmid='163000000071516'
select * from silver_njs.history_transfer@silver_std  where firmid='163000000071516'
select * from info_silver.ods_crm_transfer_record where firm_id='163000000071516'
SELECT *
FROM silver_njs.tb_silver_data_center@silver_std where  firmid='163000000071516'




