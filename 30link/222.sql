


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


select * fROM  NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT

select * fROM info_silver.ods_history_deal where partner_id='hht' and substr(fdate,1,6) ='201704'

SELECT aaa.user_id,aaa.firm_id,aaa.id1,aaa.id2
FROM (
       SELECT
         a.user_id,
         a.firm_id,
         a.ia_id as id1,
         a.ia_name,
         b.ia_id as id2,
         (CASE WHEN a.group_id - b.group_id <> 0
           THEN a.group_id - b.group_id END) AS t
       FROM info_silver.dw_user_account a
         JOIN info_silver.tb_crm_user b ON a.crm_user_id = b.id
     ) aaa
WHERE aaa.t IS NOT NULL



SELECT
                 --a.fund_id,
                   sum(charge_amount) AS netinmoney,to_char(a.trade_date,'yyyymmdd')
                 FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT a
                   join tb_silver_user_stat@silver_std b
                   on a.fund_id = b.firm_id
                 WHERE to_char(a.trade_date,'yyyymmdd') > 20170422
                       AND a.ORDER_STATUS = 3 AND a.RECONC_STATUS = 2 group by to_char(a.trade_date,'yyyymmdd')
                -- GROUP BY a.fund_id




SELECT
  a.refer_1_type as 内外推,
  a.sub_refer as 渠道号,
  count(distinct a.firm_id) as 广贵21日用户数,
  count(case when b.firm_id is not null then b.firm_id end) as hht开户用户数,
  sum(c.net_assets) as pmec21日净资产,
  sum(case when b.firm_id is not null then d.hhtasset end) as hht昨日净资产,
  sum(e.net_assets) as pmec昨日净资产
FROM
  (SELECT

     DISTINCT
     user_id,
     firm_id,
     refer_1_type,
     sub_refer
   FROM info_silver.dw_user_account
   WHERE to_char(open_account_time, 'yyyymmdd') <= 20170421 AND partner_id = 'pmec') a
  LEFT JOIN
  (SELECT DISTINCT
     user_id,
     firm_id
   FROM info_silver.dw_user_account
   WHERE partner_id = 'hht'
  and to_char(open_account_time, 'yyyymmdd') <= to_char(sysdate - 1, 'yyyymmdd')) b
    ON a.user_id = b.user_id
  LEFT JOIN
  (SELECT
     firmid,
     net_assets
   FROM silver_njs.tb_silver_data_center@silver_std
   WHERE hdate = 20170421) c
    ON a.firm_id = c.firmid
  LEFT JOIN
  (SELECT
     fund_id,
     sum(last_capital) AS hhtasset
   FROM NSIP_ACCOUNT.TB_NSIP_A_FUNDS_AFTER_SETTLE@LINK_NSIP_ACCOUNT
   WHERE to_char(trade_date, 'yyyymmdd') = to_char(SYSDATE - 1, 'yyyymmdd')
   GROUP BY fund_id) d
    ON b.firm_id = d.fund_id
  LEFT JOIN
    (SELECT
     firmid,
     net_assets
   FROM silver_njs.tb_silver_data_center@silver_std
   WHERE hdate = to_char(sysdate - 1, 'yyyymmdd')) e
    on a.firm_id = e.firmid
group by a.sub_refer,a.refer_1_type


select *
from silver_consult.tb_crm_transfer_record@consul_std trans
where to_char(trans.submit_time,'yyyymmdd') between 20170424 and 20170428
and trans.process in(5,6) and trans.valid=1
and bia_group_id in (1,7,8,111)



