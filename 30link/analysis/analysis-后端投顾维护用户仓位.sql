SELECT
  a.user_name           客户姓名,
  a.id                  客户ID,
  a.group_id            投顾组别,
  d.name                投顾姓名,
  max(c.fdate)          日期,
  sum(CASE WHEN c.commoditycode = 'GDAG'
    THEN c.holdqty END) 白银公斤数,
  sum(CASE WHEN c.commoditycode = 'GDPT'
    THEN c.holdqty END) 铂金公斤数,
  sum(CASE WHEN c.commoditycode = 'GDPD'
    THEN c.holdqty END) 钯金公斤数,
  min(e.net_assets)     净资产,
  min(f.um)             交易准备金,
  min(f.cw)             仓位
FROM
  info_silver.tb_crm_user a
  JOIN info_silver.ods_history_user b
    ON a.fa_id = b.user_id
  LEFT JOIN (SELECT *
             FROM info_silver.pmec_hold_position_order
             WHERE fdate = trunc(sysdate) - 3
             ORDER BY loginaccount) c
    ON b.firm_id = c.loginaccount
  LEFT JOIN info_silver.tb_crm_ia d
    ON a.ia_id = d.id
  LEFT JOIN (SELECT *
             FROM silver_njs.tb_silver_data_center@silver_std
             WHERE hdate = to_char(sysdate-3,'yyyymmdd') AND partner_id = 'pmec') e
    ON b.firm_id = e.firmid
  LEFT JOIN (SELECT
               loginaccount,
               used_margin / netvalue cw,
               used_margin            um
             FROM info_silver.pmec_account_info
             WHERE fdate = trunc(sysdate) - 3 AND netvalue > 0) f
    ON b.firm_id = f.loginaccount

--where c.fdate=trunc(sysdate)-1 and
WHERE a.group_id IN (1, 7, 8)
GROUP BY a.user_name, a.id, a.group_id, d.name



select sum(case when g.inorout='A' then g.inoutmoney when g.inorout='B' then g.inoutmoney*(-1) end)
FROM
  info_silver.tb_crm_user a
  JOIN info_silver.dw_user_account b
    ON a.fa_id = b.user_id
  join silver_njs.history_transfer@silver_std g
    on b.firm_id = g.firmid
      and g.partnerid='pmec'
  and g.fdate = to_char(sysdate-1,'yyyymmdd')
WHERE a.group_id IN (1, 7, 8)




select a.fa_id,a.firm_id,a.cur_bia_id,a.cur_bgroup_id,
  sum(case when b.inorout='A' then b.inoutmoney when b.inorout='B' then b.inoutmoney*(-1) end)
from
  info_silver.ods_crm_transfer_record a
join silver_njs.history_transfer@silver_std b
  on a.firm_id = b.firmid
where a.cur_bgroup_id in (1,7,8)
and b.fdate = to_char(sysdate-1,'yyyymmdd')
and a.process in (5,6) and a.valid=1
and b.realdate>a.submit_time
group by a.fa_id, a.firm_id,a.cur_bia_id,a.cur_bgroup_id




select c.id,c.group_id,b.firm_id,sum(case when g.inorout='A' then g.inoutmoney when g.inorout='B' then g.inoutmoney*(-1) end)
  from silver_njs.history_transfer@silver_std g
    join info_silver.dw_user_account b
    on g.firmid=b.firm_id
    join info_silver.tb_crm_ia c
    on b.ia_id=c.id
where g.fdate = to_char(sysdate-1,'yyyymmdd') and g.partnerid='pmec' and c.group_id in (1,7,8)
group by b.firm_id,c.id,c.group_id






select * from info_silver.dw_user_account where firm_id='163000000000641'
select * from









select * from info_silver.pmec_account_info where loginaccount=163000000119188
and  to_char(fdate,'yyyymmdd')=20170330
select * from info_silver.pmec_hold_position_order where loginaccount=163000000119188
and  to_char(fdate,'yyyymmdd')=20170330