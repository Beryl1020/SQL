SELECT
  c.*,
  avg(d.used_margin / d.netvalue) 平均持仓率,
  max(d.used_margin / d.netvalue) 最大持仓率,
  avg(d.netvalue)                 平均净值
FROM
  (
    SELECT
      /*+driving_site(a)*/
      a.user_id,
      a.pmec_net_in_sub + a.pmec_net_value_sub jhzj,
      a.firm_id,
      a.submit_time,
      sum(b.contqty)                           jye
    FROM
      info_silver.ods_crm_transfer_record@silver_stat_urs_30_link a
      LEFT JOIN ods_history_deal@silver_stat_urs_30_link b
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


SELECT /*driving_site(deal)*/                                --平均仓位
  deal.firmid,
  sum(deal.contqty),
  avg(acc.used_margin / acc.netvalue)
FROM
  (SELECT
     /*driving_site(deal)*/
     firmid,
     sum(contqty) AS contqty
   FROM
     ods_history_deal@silver_stat_urs_30_link
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





SELECT /*driving_site(deal)*/                                --盈亏合计
  deal.firmid,
  sum(case when changetype in(1,3,8,9,10) then amount end)
FROM
  (SELECT
     /*driving_site(deal)*/
     firmid,
     sum(contqty) AS contqty
   FROM
     ods_history_deal@silver_stat_urs_30_link
     where fdate >=20170101
     and partner_id='pmec'
   GROUP BY firmid
  ) deal
  JOIN silver_njs.pmec_zj_flow acc
    ON deal.firmid = acc.loginaccount
WHERE deal.contqty > 0
      and acc.fdate >= to_date(20170101,'yyyymmdd')
GROUP BY deal.firmid




SELECT /*driving_site(deal)*/                                --净入金合计
  deal.firmid,
  sum(case when inorout='A' then inoutmoney
      when inorout='B' then inoutmoney*(-1) end)
FROM
  (SELECT
     /*driving_site(deal)*/
     firmid,
     sum(contqty) AS contqty
   FROM
     ods_history_deal@silver_stat_urs_30_link
     where fdate >=20170101
     and partner_id='pmec'
   GROUP BY firmid
  ) deal
  JOIN silver_njs.history_transfer acc
    ON deal.firmid = acc.firmid
WHERE deal.contqty > 0
  and acc.firmid='163000000201567'
     -- and acc.fdate >= 20170101
GROUP BY deal.firmid



select * from silver_njs.history_transfer where firmid=163000000201567