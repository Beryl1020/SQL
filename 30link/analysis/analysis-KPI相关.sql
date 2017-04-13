select ia.id, ia.name, ia.group_id,
  sum(case when trans.submit_time between ia.create_time and ia.create_time+45 then                           --投顾入职后一个月开单数
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end)
from info_silver.tb_crm_ia ia
left join  info_silver.ods_crm_transfer_record trans
  on trans.fia_id=ia.id
where to_char(ia.create_time,'yyyymmdd')<=20170131
  and ia.name  not like '%资源%'
and ia.group_id in (2,3,4,5,6,9,10,11,12,105)
  and ia.status=1
group by ia.id, ia.name, ia.group_id


select ia.id, ia.name, ia.group_id,
  sum(case when trans.submit_time between ia.create_time and ia.create_time+45 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+46 and ia.create_time+75 then                          --投顾入职后1、2个月开单数
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end)
from info_silver.tb_crm_ia ia
left join  info_silver.ods_crm_transfer_record trans
  on trans.fia_id=ia.id
where to_char(ia.create_time,'yyyymmdd')<=20170115
  and ia.name  not like '%资源%'
and ia.group_id in (2,3,4,5,6,9,10,11,12,105)
       and ia.status=1
group by ia.id, ia.name, ia.group_id


select ia.id, ia.name, ia.group_id,
  sum(case when trans.submit_time between ia.create_time and ia.create_time+45 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+46 and ia.create_time+75 then                          --投顾入职后1.2.3个月开单数
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+76 and ia.create_time+105 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end)
from info_silver.tb_crm_ia ia
left join  info_silver.ods_crm_transfer_record trans
  on trans.fia_id=ia.id
where to_char(ia.create_time,'yyyymmdd')<=20161215
  and ia.name  not like '%资源%'
and ia.group_id in (2,3,4,5,6,9,10,11,12,105)
     and ia.status=1
group by ia.id, ia.name, ia.group_id



select ia.id, ia.name, ia.group_id,
  sum(case when trans.submit_time between ia.create_time and ia.create_time+45 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+46 and ia.create_time+75 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+76 and ia.create_time+105 then                          --投顾入职后1.2.3.4个月开单数
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+106 and ia.create_time+135 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end)
from info_silver.tb_crm_ia ia
left join  info_silver.ods_crm_transfer_record trans
  on trans.fia_id=ia.id
where to_char(ia.create_time,'yyyymmdd')<=20161115
  and ia.name  not like '%资源%'
and ia.group_id in (2,3,4,5,6,9,10,11,12,105)
   and ia.status=1
group by ia.id, ia.name, ia.group_id



select ia.id, ia.name, ia.group_id,
  sum(case when trans.submit_time between ia.create_time and ia.create_time+45 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+46 and ia.create_time+75 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+76 and ia.create_time+105 then                          --投顾入职后1.2.3.4.5个月开单数
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+106 and ia.create_time+135 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when trans.submit_time between ia.create_time+136 and ia.create_time+165 then
    trans.pmec_net_value_sub+trans.pmec_net_in_sub end)
from info_silver.tb_crm_ia ia
left join  info_silver.ods_crm_transfer_record trans
  on trans.fia_id=ia.id
where to_char(ia.create_time,'yyyymmdd')<=20161015
  and ia.name  not like '%资源%'
  and ia.status=1
and ia.group_id in (2,3,4,5,6,9,10,11,12,105)
group by ia.id, ia.name, ia.group_id


select to_char(submit_time,'yyyymm'), to_char(add_months(submit_time,-1),'yyyymm') from info_silver.ods_crm_transfer_record

select ia.id, ia.name, ia.group_id,
    sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,0),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),              --本月开单投顾入职时间分布
  sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,-1),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,-2),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,-3),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,-4),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,-5),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,-6),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,-7),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,-8),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when to_char(ia.create_time,'yyyymm') = to_char(add_months(trans.submit_time,-9),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end),
  sum(case when to_char(ia.create_time,'yyyymm') < to_char(add_months(trans.submit_time,-9),'yyyymm') then trans.pmec_net_value_sub+trans.pmec_net_in_sub end)
  from info_silver.tb_crm_ia ia
left join  info_silver.ods_crm_transfer_record trans
  on trans.fia_id=ia.id
where to_char(trans.submit_time,'yyyymmdd') between 20170101 and 20170131
  and ia.name  not like '%资源%'
  and ia.status=1
--and ia.group_id in (2,3,4,5,6,9,10,11,12,105)
    and ia.group_id in (106,112,113,114)
group by ia.id, ia.name, ia.group_id


select distinct fia_id from info_silver.ods_crm_transfer_record
where to_char(submit_time,'yyyymmdd') between 20170101 and 20170131
and fgroup_id in (2,3,4,5,6,9,10,11,12,105)


select * from info_silver.tb_crm_ia
--where group_id in (2,3,4,5,6,9,10,11,12,105)
  where group_id in(106,111,112,113)
and name  not like '%资源%'
and status=1
and to_char(create_time,'yyyymmdd') <=20170228

select * from info_silver.ods_crm_transfer_record where fia_id=154


SELECT sum(deal.contqty)
FROM info_silver.ods_crm_transfer_record trans                                             --2016年开单用户2017年各月份交易额
  JOIN info_silver.ods_history_deal deal
    ON trans.firm_id = deal.firmid
       AND
       deal.fdate BETWEEN 20170301 AND 20170331
WHERE to_char(trans.submit_time, 'yyyymmdd') <20170101
      AND trans.process IN (5, 6) AND trans.valid = 1

SELECT sum(trans.pmec_net_in_sub+trans.pmec_net_value_sub),
  sum(case when io.inorout='A'  then io.inoutmoney when io.inorout='B' then io.inoutmoney*(-1) end)
FROM info_silver.ods_crm_transfer_record trans
join silver_njs.history_transfer@silver_std io
  on trans.firm_id=io.firmid and io.fdate < 20170101                                                 --2016年开单用户激活资金、净入金
WHERE to_char(trans.submit_time, 'yyyymmdd') <20170101
      AND trans.process IN (5, 6) AND trans.valid = 1




SELECT count(distinct trans.user_id),count(distinct trans.fia_id),sum(trans.pmec_net_in_sub+trans.pmec_net_value_sub)
FROM info_silver.ods_crm_transfer_record trans                                                              --微销开单激活资金
WHERE trans.fgroup_id in (114,112,113,106)
      AND trans.process IN (5, 6) AND trans.valid = 1
and to_char(trans.submit_time,'yyyymmdd') between 20170201 and 20170231


select * from info_silver.ods_crm_transfer_record where fgroup_id in (112,113,114,106) and bgroup_id not in(111)
and to_char(submit_time,'yyyymmdd') between 20170201 and 20170231;

select * from info_silver.ods_crm_transfer_record where fgroup_id  not in (112,113,114,106) and bgroup_id  in (111)
and to_char(submit_time,'yyyymmdd') between 20170201 and 20170231;



SELECT count(distinct trans.user_id),count(distinct trans.fia_id),sum(trans.pmec_net_in_sub+trans.pmec_net_value_sub)
FROM info_silver.ods_crm_transfer_record trans                                                              --微销开单激活资金
WHERE trans.bgroup_id in (111)
      AND trans.process IN (5, 6) AND trans.valid = 1
and to_char(trans.submit_time,'yyyymmdd') between 20170201 and 20170231


SELECT count(distinct trans.user_id),count(distinct trans.fia_id),sum(trans.pmec_net_in_sub+trans.pmec_net_value_sub)
FROM info_silver.ods_crm_transfer_record trans                                                               --电销开单激活资金
WHERE trans.fgroup_id in (2,3,4,5,6,9,10,11,12,105)
      AND trans.process IN (5, 6) AND trans.valid = 1
and to_char(trans.submit_time,'yyyymmdd') between 20170301 and 20170331







select --case when to_char(bbb.date1,'yyyymmdd')<20170101 then bbb.id end,
sum(case when to_char(bbb.date1,'yyyymmdd')<20170101 and io.inorout='A'  then io.inoutmoney
    when io.inorout='B' and to_char(bbb.date1,'yyyymmdd')<20170101 then io.inoutmoney*(-1) end),                      --2016年有效新入金用户2016年净入金
count(distinct case when to_char(bbb.date1,'yyyymmdd')<20170101 then  bbb.id end)                                                                                                  --2016年有效新入金用户2016年净入金
from
  (select aaa.firm_id id, min(aaa.realdate) as date1
  from
  (select suba.firmid as firm_id, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate

  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firm_id) bbb
join silver_njs.history_transfer@silver_std io
  on bbb.id=io.firmid and io.fdate < 20170101
-- group by case when to_char(bbb.date1,'yyyymmdd')<20170101 then bbb.id end







select  sum(case when io.inorout='A'  then io.inoutmoney when io.inorout='B' then io.inoutmoney*(-1) end)              --2016年有效新入金用户且2017年1月开单用户2016年净入金
from
  (select aaa.firm_id id, min(aaa.realdate) as date1
  from
  (select suba.firmid as firm_id, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate
  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firm_id) bbb
join info_silver.ods_crm_transfer_record trans
    on bbb.id=trans.firm_id
join silver_njs.history_transfer@silver_std io
  on bbb.id=io.firmid and io.fdate < 20170101
where to_char(trans.submit_time,'yyyymmdd') < 20170101
and trans.process in (5,6) and trans.valid=1



select a.firmid,a.mindate
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
where to_char(a.mindate,'yyyymmdd') < 20170101                            --2016年有入金用户及首次入金时间




select sum(case when b.inorout='A'  then b.inoutmoney when b.inorout='B' then b.inoutmoney*(-1) end)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std                               --2016年有入金用户2016年净入金总
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
    join silver_njs.history_transfer@silver_std b
    on a.firmid=b.firmid
    and b.fdate < 20170101 /*交易时间*/
where to_char(a.mindate,'yyyymmdd') < 20170101 /*首次入金时间*/







select sum(case when b.inorout='A'  then b.inoutmoney when b.inorout='B' then b.inoutmoney*(-1) end)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std                           --2016年有入金且2016年未开单用户2016年净入金总
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
    join silver_njs.history_transfer@silver_std b
    on a.firmid=b.firmid
    and b.fdate < 20170101 /*交易时间*/
where to_char(a.mindate,'yyyymmdd') < 20170101 /*首次入金时间*/
and a.firmid not IN
    (select firm_id from info_silver.ods_crm_transfer_record
    where process in(5,6) and valid=1
    and to_char(submit_time,'yyyymmdd')<20170301) /*流转时间*/






select sum(b.contqty)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std                           --2016年有入金且2016年未开单用户1月交易额
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
    join info_silver.ods_history_deal b
    on a.firmid=b.firmid
    and b.fdate between 20170101 and 20170131 /*交易时间*/
where
  to_char(a.mindate, 'yyyymmdd') < 20170101 /*首次入金时间*/
AND
    (a.firmid NOT IN
     (SELECT firm_id
      FROM info_silver.ods_crm_transfer_record
      WHERE process IN (5, 6) AND valid = 1
            AND to_char(submit_time, 'yyyymmdd') < 20170101) /*流转时间*/
    )





select sum(b.contqty)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std                           --2016年有入金且2017年1月开单用户流转后交易额（需扣除）
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
    join info_silver.ods_history_deal b
    on a.firmid=b.firmid
    and b.fdate between 20170101 and 20170131 /*交易时间*/
    join info_silver.ods_crm_transfer_record c
      on a.firmid=c.firm_id
    and to_char(c.submit_time,'yyyymmdd') between 20170101 and 20170131 /*流转时间*/
where
  to_char(a.mindate, 'yyyymmdd') < 20170101 /*首次入金时间*/
and c.submit_time < b.trade_time








select sum(b.contqty)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std                           --2016年有入金且2016年Jan未开单用户2月交易额
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
    join info_silver.ods_history_deal b
    on a.firmid=b.firmid
    and b.fdate between 20170101 and 20170131 /*交易时间*/
where to_char(a.mindate,'yyyymmdd') < 20170101 /*首次入金时间*/
and a.firmid not IN
    (select firm_id from info_silver.ods_crm_transfer_record
    where process in(5,6) and valid=1
    and to_char(submit_time,'yyyymmdd')<20170101) /*流转时间*/






select count (distinct case
                       when partner_id ='pmec'
                            and TO_CHAR(OPEN_ACCOUNT_TIME,'yyyymmdd') between 20170301 and 20170331    --广贵新增开户数
                         then firm_id end)
    from tb_silver_user_stat@silver_std



select sum(b.contqty),count(distinct a.id) from
  (select aaa.firm_id id, min(aaa.realdate) as date1
  from
  (select suba.firmid as firm_id, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate    --1月有效新入金用户且1月未流转1月交易额
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate
  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firm_id) a
  join info_silver.ods_history_deal b
    on a.id=b.firmid
    and b.fdate between 20170301 and 20170331 /*交易时间*/
  where to_char(a.date1,'yyyymmdd') between 20170301 and 20170331 /*有效新入金时间*/
and a.id not IN
    (select firm_id from info_silver.ods_crm_transfer_record
    where process in(5,6) and valid=1
    and to_char(submit_time,'yyyymmdd')<=20170331) /*流转时间*/




select sum(b.contqty),count(distinct a.id) from
  (select aaa.firm_id id, min(aaa.realdate) as date1
  from
  (select suba.firmid as firm_id, suba.realdate as realdate, sum(subb.summoney) as money
  from
  (select firmid, realdate FROM silver_njs.history_transfer@silver_std where partnerid='pmec') suba
  left JOIN
  (select firmid, (case when inorout='A' then inoutmoney when inorout='B' then (-1)*inoutmoney end) as summoney, realdate    --1月有效新入金且1月流转用户1月流转前交易额
  from silver_njs.history_transfer@silver_std where partnerid='pmec') subb
  on suba.firmid=subb.firmid and suba.realdate>=subb.realdate
  group by suba.firmid, suba.realdate) aaa
  where aaa.money >=50 group by aaa.firm_id) a
  join info_silver.ods_history_deal b
    on a.id=b.firmid
    and b.fdate between 20170301 and 20170331 /*交易时间*/
  join info_silver.ods_crm_transfer_record c
      on a.id=c.firm_id
    and to_char(c.submit_time,'yyyymmdd') between 20170301 and 20170331 /*流转时间*/
   and c.process in (5,6) and c.valid=1
  where to_char(a.date1,'yyyymmdd') between 20170201 and 20170228 /*有效新入金时间*/
and b.trade_time < c.submit_time



select * from info_silver.pmec_zj_flow



select count(distinct a.firmid)
  from
    (SELECT
       firmid,
       min(CASE WHEN inorout = 'A'
         THEN realdate END) AS mindate
     FROM silver_njs.history_transfer@silver_std                           --广贵新增入金用户数
     WHERE partnerid = 'pmec'
      GROUP BY firmid
    )a
where to_char(a.mindate,'yyyymmdd')  between 20170201 and 20170228





select * from info_silver.tb_crm_user user1
select * from info_silver.ods_crm_transfer_record




/* 各月份投顾人数 */
select distinct c.firm_id from info_silver.ods_crm_transfer_record c
where  to_char(c.submit_time,'yyyymm') = 201703 /*流转时间*/
   and c.process in (5,6) and c.valid=1
 and c.bgroup_id in (1,7,8)

select * from info_silver.ods_crm_transfer_record where fia_id=154

select * from info_silver.tb_crm_ia where status=1 and group_id in (106,112,113,114) and name not like '%资源%'

select * from info_silver.ods_crm_transfer_record



