SELECT
  aaa.loginaccount AS firmid,
  aaa.partner_id,
  aaa.user_id      AS 主站id,
  aaa.crm_user_id  AS CRM系统id,
  aaa.real_name    AS 用户姓名,
  aaa.ia_id        AS 投顾id,
  aaa.ia_name      AS 投顾姓名,
  aaa.group_id     AS 投顾组别,
  aaa.amount       AS 亏损额,
  bbb.assets       AS 广贵历史最高净资产
FROM
  (SELECT
     a.loginaccount,
     a.partner_id,
     b.user_id,
     b.crm_user_id,
     b.real_name,
     b.ia_id,
     b.ia_name,
     b.group_id,
     sum(a.amount) AS amount
   FROM info_silver.pmec_zj_flow a
     JOIN info_silver.dw_user_account b
       ON a.loginaccount = b.firm_id
   WHERE a.changetype IN (1, 3, 8, 9, 10) AND a.partner_id = 'pmec'
   GROUP BY a.loginaccount, a.partner_id, b.user_id, b.crm_user_id, b.real_name, b.ia_id, b.ia_name, b.group_id) aaa
  LEFT JOIN
  (SELECT
     firmid,
     max(net_assets) AS assets
   FROM silver_njs.tb_silver_data_center@silver_std
   WHERE partner_id = 'pmec'
   GROUP BY firmid) bbb
    ON aaa.loginaccount = bbb.firmid
  LEFT JOIN
  (SELECT)
WHERE aaa.amount <= -10000 AND
      aaa.amount / bbb.assets <= -0.5

SELECT *
FROM info_silver.pmec_zj_flow
WHERE loginaccount = 163000000000761
      AND changetype IN (9, 10)


SELECT

  aaa.user_id as 主站id,
  aaa.crm_user_id as crm系统id,
  aaa.real_name as 用户姓名,
  aaa.ia_id as 投顾id,
  aaa.ia_name as 投顾姓名,
  aaa.group_id as 投顾组别,
  aaa.sumpl    AS 亏损额,
  aaa.hhtpl as 龙商亏损额,
  aaa.pmecpl as 广贵亏损额,
  aaa.maxasset AS 历史最高净资产
FROM
  (SELECT
    aa.real_name,
    aa.ia_name,
    aa.ia_id,
    aa.group_id,
     aa.user_id,
     aa.crm_user_id,
     aa.sumpl,
    aa.hhtpl,
    aa.pmecpl,
     CASE WHEN (aa.pmecasset > aa.hhtasset OR aa.hhtasset IS NULL)
       THEN aa.pmecasset
     WHEN (aa.pmecasset < aa.hhtasset OR aa.pmecasset IS NULL)
       THEN aa.hhtasset END AS maxasset
   FROM
     (SELECT
       a.real_name,
        a.ia_id,
       a.ia_name,
       a.group_id,
        a.user_id,
        a.crm_user_id,
        max(b.pmecasset)                        AS pmecasset,
        max(c.hhtasset)                         AS hhtasset,
        sum(nvl(d.pmecpl, 0) + nvl(e.hhtpl, 0)) AS sumpl,
        max(d.pmecpl) as pmecpl,
       max(e.hhtpl ) as hhtpl
      FROM info_silver.dw_user_account a
        LEFT JOIN
        (SELECT
           firmid          AS firm_id,
           max(net_assets) AS pmecasset
         FROM (SELECT
                 firmid,
                 Net_Assets,
                 partner_id,
                 hdate
               FROM silver_njs.tb_silver_data_center@silver_std
               WHERE partner_id = 'pmec')
         WHERE partner_id = 'pmec'
         GROUP BY firmid) b
          ON a.firm_id = b.firm_id
        LEFT JOIN
        (SELECT
           firmid          AS firm_id,
           max(net_assets) AS hhtasset
         FROM silver_njs.tb_silver_data_center@silver_std
         WHERE partner_id = 'hht'
         GROUP BY firmid) c
          ON a.firm_id = c.firm_id
        LEFT JOIN
        (SELECT
           loginaccount AS firm_id,
           sum(amount)  AS pmecpl
         FROM info_silver.pmec_zj_flow
         WHERE changetype IN (1, 3, 8, 9, 10)
         GROUP BY loginaccount) d
          ON a.firm_id = d.firm_id
        LEFT JOIN
        (SELECT
           fund_id              AS firm_id,
           sum(CASE WHEN type = 7
             THEN -amount
               ELSE amount END) AS hhtpl
         FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT
         WHERE type IN (3, 4, 5, 6, 7)
         GROUP BY fund_id) e
          ON a.firm_id = b.firm_id
      WHERE a.partner_id IN ('pmec', 'hht')
      GROUP BY  a.ia_id,
       a.ia_name,
       a.group_id,
        a.user_id,
        a.crm_user_id,a.real_name
     ) aa
  ) aaa
WHERE aaa.sumpl <= -10000 AND aaa.sumpl / aaa.maxasset <= -0.5


SELECT sum(net_assets)
FROM silver_njs.tb_silver_data_center@silver_std
WHERE hdate = 20170517 AND partner_id = 'pmec'