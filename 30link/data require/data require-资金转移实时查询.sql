SELECT
  a.group_type,
  a.sale_type,
  a.group_id,
  a.group_name,
  count(DISTINCT CASE WHEN a.pmec_firmid IS NOT NULL
    THEN a.pmec_firmid END) AS 广贵用户数,
  count(DISTINCT CASE WHEN a.hht_firmid IS NOT NULL
    THEN hht_firmid END)    AS 龙商开户数,
  sum(a.pmec_netvalue_21)   AS 广贵净资产总,
  sum(a.pmec_netvalue)      AS 广贵现资产,
  sum(a.hht_netvalue)       AS 龙商现资产
FROM info_silver.DM_CRM_TRANSFER_STAT a
WHERE sale_type NOT IN ('all')
GROUP BY a.sale_type, a.group_type,
  a.group_id, a.group_name

UNION ALL

SELECT
  '非投顾' AS group_type,
  '非投顾' AS sale_type,
  '非投顾' AS group_id,
  '非投顾' AS group_name,
  count(DISTINCT aaa.id1),
  count(DISTINCT CASE WHEN aaa.id2 IS NOT NULL
    THEN aaa.id2 END),
  sum(aaa.net_assets),
  sum(aaa.net_assets) + sum(aaa.pmec_net_in),
  sum(aaa.netinmoney)
FROM
  (
    SELECT DISTINCT
      a.firm_id AS id1,
      b.net_assets,
      c.pmec_net_in,
      d.netinmoney,
      e.firmid  AS id2
    FROM
      (SELECT
         a.firm_id,
         a.user_name
       FROM
         (SELECT *
          FROM info_silver.ods_history_user
          WHERE partner_id = 'pmec') a LEFT JOIN info_silver.tb_crm_user b ON a.crm_user_id = b.id
       WHERE a.crm_user_id IS NULL OR b.group_id IN (103, 108, 109, 104, 100, 107, 115)) a
      LEFT JOIN (SELECT *
                 FROM silver_njs.tb_silver_data_center@silver_std
                 WHERE hdate = 20170421) b
        ON a.firm_id = b.firmid
      LEFT JOIN (SELECT
                   firmid,
                   sum(CASE WHEN inorout = 'A'
                     THEN inoutmoney
                       ELSE -inoutmoney END) pmec_net_in
                 FROM silver_njs.history_transfer@silver_std
                 WHERE trunc(realdate) >= to_date('20170424', 'yyyymmdd')
                 GROUP BY firmid) c
        ON a.firm_id = c.firmid
      LEFT JOIN (SELECT *
                 FROM SILVER.TB_SILVER_ACCOUNT@SILVERONLINE_LINK
                 WHERE partner_id = 'hht') e
        ON a.user_name = e.username
      LEFT JOIN (SELECT
                   fund_id,
                   sum(charge_amount) AS netinmoney
                 FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT
                 WHERE trunc(CREATE_TIME) >= to_date('20170424', 'yyyymmdd')
                       AND ORDER_STATUS = 3 AND RECONC_STATUS = 2
                 GROUP BY fund_id) d
        ON e.firmid = d.fund_id
    WHERE b.net_assets > 0
  ) aaa;



SELECT sum(last_capital)
FROM NSIP_ACCOUNT.TB_NSIP_A_FUNDS_AFTER_SETTLE@LINK_NSIP_ACCOUNT
WHERE to_char(trade_date, 'yyyymmdd') = 20170421;

SELECT sum(charge_amount)
FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT
WHERE to_char(trade_date, 'yyyymmdd') > 20170421

SELECT sum()
FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT

select * from silver_consult.tb_crm_transfer_record@consul_std
 where to_char(submit_time,'yyyymmdd') = 20170424


select * from NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT where fund_id='163170424584314'