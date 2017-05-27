SELECT
  a.firmid,
  a.hdate,
  a.net_assets,
  b.cont
FROM
  (SELECT *
   FROM silver_njs.tb_silver_data_center@silver_std
   WHERE partner_id = 'njs'
         AND hdate IN ('20170519', '20170522')
  ) a
  JOIN
  (SELECT
     firmid,
     sum(contqty) AS cont
   FROM info_silver.ods_history_deal
   WHERE fdate BETWEEN '20170519' AND '20170522' AND wareid = 'EU2O3'
  group by firmid) b
    ON a.firmid = b.firmid