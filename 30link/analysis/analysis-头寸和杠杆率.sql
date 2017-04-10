SELECT
  c.*,
  avg(d.used_margin / d.netvalue) 平均持仓率,
  max(d.used_margin / d.netvalue) 最大持仓率,
  avg(d.netvalue)                 平均净值
FROM
  (
    SELECT

      a.user_id,
      a.pmec_net_in_sub + a.pmec_net_value_sub jhzj,
      a.firm_id,
      a.submit_time,
      sum(b.contqty)                           jye
    FROM
      info_silver.ods_crm_transfer_record a
      LEFT JOIN info_silver.ods_history_deal b
        ON a.firm_id = b.firmid
    WHERE a.pmec_net_in_sub + a.pmec_net_value_sub >= 50000
          AND b.trade_time > a.submit_time AND a.process IN (5, 6) AND
          to_char(b.trade_time, 'yyyymm') = '201703'
    GROUP BY a.user_id, a.pmec_net_in_sub + a.pmec_net_value_sub, a.firm_id, a.submit_time
  ) c
  JOIN pmec_account_info d ON c.firm_id = d.loginaccount
WHERE d.fdate > trunc(c.submit_time) AND netvalue > 50000 AND
      d.fdate BETWEEN to_date('20170301', 'yyyymmdd') AND to_date('20170328', 'yyyymmdd')
      AND d.used_margin > 0
GROUP BY c.user_id, c.jhzj, c.firm_id, c.submit_time, c.jye


SELECT                                  --平均仓位
  deal.firmid,
  sum(deal.contqty),
  avg(acc.used_margin / acc.netvalue)
FROM
  (SELECT

     firmid,
     sum(contqty) AS contqty
   FROM
     info_silver.ods_history_deal
     where fdate >=20170101
     and partner_id='pmec'
   GROUP BY firmid
  ) deal
  JOIN pmec_account_info acc
    ON deal.firmid = acc.loginaccount
WHERE deal.contqty > 0
      AND acc.used_margin <> 0
      and acc.fdate >= to_date(20170101,'yyyymmdd')
GROUP BY deal.firmid





SELECT                                  --盈亏合计
  deal.firmid,
  sum(case when changetype in(1,3,8,9,10) then amount end)
FROM
  (SELECT

     firmid,
     sum(contqty) AS contqty
   FROM
     info_silver.ods_history_deal
     where fdate >=20170101
     and partner_id='pmec'
   GROUP BY firmid
  ) deal
  JOIN silver_njs.pmec_zj_flow@silver_std acc
    ON deal.firmid = acc.loginaccount
WHERE deal.contqty > 0
      and acc.fdate >= to_date(20170101,'yyyymmdd')
GROUP BY deal.firmid




SELECT                                  --净入金合计
  deal.firmid,
  sum(case when inorout='A' then inoutmoney
      when inorout='B' then inoutmoney*(-1) end)
FROM
  (SELECT

     firmid,
     sum(contqty) AS contqty
   FROM
     info_silver.ods_history_deal
     where fdate >=20170101
     and partner_id='pmec'
   GROUP BY firmid
  ) deal
  JOIN silver_njs.history_transfer@silver_std acc
    ON deal.firmid = acc.firmid
WHERE deal.contqty > 0
  and acc.firmid='163000000201567'
     -- and acc.fdate >= 20170101
GROUP BY deal.firmid



select fdate,sum(contqty)                                                          --每日广贵交易额
  from info_silver.ods_history_deal
  where fdate between 20170101 and 20170330 and partner_id='pmec'
  group by fdate






select acc.fdate,avg(acc.margin) as 仓位                                             --每日仓位
  from
(select loginaccount,to_char(fdate,'yyyymmdd') as fdate,used_margin/netvalue as margin
  from info_silver.pmec_account_info
where --used_margin > 0 and
      to_char(fdate,'yyyymmdd') between 20170101 and 20170330
      and netvalue>0) acc
-- where acc.margin > 0
group by acc.fdate
order by acc.fdate



select to_char(acc.fdate,'yyyymmdd'),acc.used_margin/acc.netvalue as margin
  from
(
select  fdate,sum(used_margin) as used_margin,sum(netvalue) as netvalue
  from info_silver.pmec_account_info
where to_char(fdate,'yyyymmdd') between 20170101 and 20170330
group by fdate) acc






select hdate,sum(net_assets)                                                          --每日净资产
from
  silver_njs.tb_silver_data_center@silver_std
  where hdate between 20170101 and 20170330 and partner_id='pmec'
group by hdate
order by hdate












select * from




select * from info_silver.pmec_account_info




select * from silver_njs.history_transfer@silver_std where firmid=163000000201567