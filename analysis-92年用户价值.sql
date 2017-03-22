select aa.date1,sum(aa.开户人数),sum(aa.开户30天净入金) from
(select substr(a.birth_day,1,4) as date1, c.user_id as user_id,
  count(distinct c.firm_id ) as 开户人数,
  sum(case
      when trunc(d.realdate)<=trunc(c.open_account_time)+30 and d.inorout='A' then d.inoutmoney
      when trunc(d.realdate)<=trunc(c.open_account_time)+30 and d.inorout='B' then d.inoutmoney*(-1) end) as 开户30天净入金
  -- avg(case
   --   when e.hdate<=to_char(c.open_account_time,'yyyymmdd')+30 then e.net_Assets end) as 开户30天平均净资产
  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
    left join silver_njs.history_transfer d
    on b.firmid=d.firmid
    --left join silver_njs.tb_silver_data_center e
    --on b.firmid=e.firmid
  where a.birth_day<=20170101
    and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
  group by substr(a.birth_day,1,4),c.user_id) aa
 left join
(select user_id,sum(worksec) as worksec from tb_crm_tel_record@silver_stat_urs_30_link group by user_id) bb
on aa.user_id=bb.user_id
where bb.worksec is null or bb.worksec=0
group by aa.date1




select /*+driving_site(a)*/ /*+driving_site(b)*/substr(a.birth_day,1,4),
  count(distinct c.firm_id) as 开户人数,
  --sum(case
    --  when d.fdate<=to_char(c.open_account_time,'yyyymmdd')+30 and d.inorout='A' then d.inoutmoney
    --  when d.fdate<=to_char(c.open_account_time,'yyyymmdd')+30 and d.inorout='B' then d.inoutmoney*(-1) end) as 开户30天净入金
 sum(case
   when e.hdate=20170320 then e.net_Assets end) as 开户30天平均净资产
  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
    --join silver_njs.history_transfer d
    --on b.firmid=d.firmid
   join silver_njs.tb_silver_data_center e
    on b.firmid=e.firmid
    and e.hdate>=20170101
  where a.birth_day<=20170101
    and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
  group by substr(a.birth_day,1,4)
order by substr(a.birth_day,1,4) DESC


select /*+driving_site(a)*/ /*+driving_site(b)*/ /*+driving_site(d)*/
  substr(a.birth_day,1,4),
  count(distinct  c.firm_id ) as 开户人数,
  sum(case
      when trunc(d.trade_time)<=trunc(c.open_account_time)+30  then d.contqty end) as 开户30天交易额

  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
    join ods_history_deal@silver_stat_urs_30_link  d
    on b.firmid=d.firmid
  where a.birth_day<=20170101
    and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
  group by substr(a.birth_day,1,4)
order by substr(a.birth_day,1,4) DESC



-- 93年用户情况
select /*+driving_site(a)*/ /*+driving_site(b)*/ /*+driving_site(d)*/
  b.firmid,
  count(distinct  c.firm_id ) as 开户人数,
  sum(case
      when trunc(d.trade_time)<=trunc(c.open_account_time)+30  then d.contqty end) as 开户30天交易额

  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
    join ods_history_deal@silver_stat_urs_30_link  d
    on b.firmid=d.firmid
  where a.birth_day<=20170101
    and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
    and substr(a.birth_day)
  group by substr(a.birth_day,1,4)
order by substr(a.birth_day,1,4) DESC





----不同年龄开户数
select substr(a.birth_day,1,4), count(distinct c.firm_id) as 开户数
  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
where substr(a.birth_day,1,4)<=20170101
and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
group by substr(a.birth_day,1,4)


----不同年龄的开户明细
select c.user_id,substr(a.birth_day,1,4), count(distinct c.firm_id) as 开户数
  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
where substr(a.birth_day,1,4)<=20170101
and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
group by substr(a.birth_day,1,4),c.user_id

----不同年龄未打通电话的开户明细

select /*+driving_site(a)*/ /*+driving_site(b)*/ aa.user_id,aa.date1,aa.开户数
  from
(
    SELECT /*+driving_site(A)*/ /*+driving_site(b)*/
      C.user_id as user_id, substr(A.birth_day, 1, 4) as date1, count(DISTINCT c.firm_id) AS 开户数
    FROM info_silver.src_certno_info@SILVER_STAT_URS_30_LINK A
    JOIN info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    ON A.certno=b.certno
    JOIN tb_silver_user_stat C
    ON b.firmid= C.firm_id
    WHERE substr(A.birth_day, 1, 4)<=20170101
    AND to_char(C.OPEN_ACCOUNT_TIME, 'yyyymmdd')>='20170101'
    GROUP BY substr(A.birth_day, 1, 4), c.user_id
) aa
where /*+driving_site(a)*/ aa.user_id in
      (
        SELECT user_id
        FROM
          (
            SELECT
              user_id      AS user_id,
              sum(billsec) AS billsec
            FROM tb_crm_tel_record@silver_stat_urs_30_link a
            WHERE to_char(CONNECT_TIME, 'yyyymmdd') >= 20170101
            GROUP BY user_id
          )
        WHERE
          billsec > 0
      )

----不同年龄的出入金明细
select c.user_id,substr(a.birth_day,1,4), count(distinct c.firm_id) as 开户数,
sum(case
      when trunc(d.realdate)<=trunc(c.open_account_time)+30 and d.inorout='A' then d.inoutmoney
      when trunc(d.realdate)<=trunc(c.open_account_time)+30 and d.inorout='B' then d.inoutmoney*(-1) end) as 开户30天净入金
  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
    join silver_njs.history_transfer d
    on b.firmid=d.firmid
where substr(a.birth_day,1,4)<=20170101
and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
group by substr(a.birth_day,1,4),c.user_id



----不同年龄的交易额明细
select /*+driving_site(d)*/ /*+driving_site(a)*/ /*+driving_site(b)*/
  c.user_id,substr(a.birth_day,1,4), --count(distinct c.firm_id) as 开户数,
sum(case
      when trunc(d.trade_time)<=trunc(c.open_account_time)+30  then d.contqty end) as 开户30天交易额
  from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK a
  join info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK b
    on a.certno=b.certno
    join tb_silver_user_stat c
    on b.firmid=c.firm_id
    join ods_history_deal@silver_stat_urs_30_link d
    on b.firmid=d.firmid
where substr(a.birth_day,1,4)<=20170101
and to_char(c.OPEN_ACCOUNT_TIME,'yyyymmdd')>='20170101'
group by substr(a.birth_day,1,4),c.user_id




-- 电话表打通的客户名单
SELECT id1,worksec
  FROM
(
    SELECT b.fa_id AS id1, sum(a.worksec) AS worksec
    FROM tb_crm_tel_record@silver_stat_urs_30_link a
      join tb_crm_user@silver_stat_urs_30_link b
      on a.user_id=b.id
    WHERE to_char(a.CONNECT_TIME, 'yyyymmdd') >= 20170101
    GROUP BY b.fa_id
)
where worksec>0








select * from
select * from ods_history_deal@silver_stat_urs_30_link
select * from tb_crm_user@silver_stat_urs_30_link
select * from  info_silver.tb_silver_account@SILVER_STAT_URS_30_LINK -- 取客户编码（巨长那个）
select * from info_silver.src_certno_info@SILVER_STAT_URS_30_LINK --取用户年龄
select * from silver_njs.tb_silver_data_center
select * from silver_njs.history_transfer