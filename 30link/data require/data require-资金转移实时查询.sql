SELECT
  a.group_type,
  a.sale_type,
  a.group_id,
  a.group_name,
  count(DISTINCT CASE WHEN a.pmec_firmid IS NOT NULL
    THEN a.pmec_firmid END) AS 广贵用户数,
  count(DISTINCT CASE WHEN a.hht_firmid IS NOT NULL
    THEN hht_firmid END)    AS 龙商开户数,
  sum(a.pmec_netvalue_21)   AS 广贵21日净资产,
  sum(a.pmec_netvalue)      AS 广贵现资产,
  sum(a.hht_netvalue)       AS 龙商现资产,
  sum(-b.netin) as 广贵本周出金量
FROM info_silver.DM_CRM_TRANSFER_STAT a
  left join
  (select firmid,sum(case when inorout='A' then inoutmoney when inorout='B' then inoutmoney*(-1) end) as netin
     from silver_njs.history_transfer@silver_std
     where fdate >=20170424 and partnerid='pmec'
  group by firmid) b
  on a.pmec_firmid = b.firmid
WHERE a.sale_type NOT IN ('all')
GROUP BY a.sale_type, a.group_type, a.group_id, a.group_name

UNION ALL

SELECT
  '非投顾' AS group_type,
  '非投顾' AS sale_type,
  '非投顾' AS group_id,
  '非投顾' AS group_name,
  count(DISTINCT aaa.id1) AS 广贵用户数,
  count(DISTINCT CASE WHEN aaa.id2 IS NOT NULL
    THEN aaa.id2 END) AS 龙商开户数,
  sum(aaa.asset21) AS 广贵21日净资产,
  sum(aaa.asset1) + sum(aaa.netin1) AS 广贵现资产,
  sum(aaa.netinmoney) AS 龙商现资产,
  -sum(aaa.netinall) as 广贵本周出金量
FROM
  (
    SELECT DISTINCT
      a.firm_id AS id1,
      b.net_assets as asset21,
      c.pmec_net_in as netin1,
      d.netinmoney,
      e.firmid  AS id2,
      g.pmec_net_in as netinall,
      f.net_assets as asset1
    FROM
      (SELECT
         a.firm_id,
         a.user_name
       FROM  /*非前后端用户列表*/
         (SELECT
            firm_id,
            user_name,
            crm_user_id
          FROM info_silver.ods_history_user
          WHERE partner_id = 'pmec') a LEFT JOIN info_silver.tb_crm_user b ON a.crm_user_id = b.id
       WHERE a.crm_user_id IS NULL OR b.group_id IN (103, 108, 109, 104, 100, 107, 115)) a
      LEFT JOIN   /*21日广贵净资产*/
      (SELECT
                   firmid,
                   net_assets
                 FROM silver_njs.tb_silver_data_center@silver_std
                 WHERE hdate = 20170421) b
        ON a.firm_id = b.firmid
      LEFT JOIN   /*今日广贵净入金*/
        (SELECT
                   firmid,
                   sum(CASE WHEN inorout = 'A'
                     THEN inoutmoney
                       ELSE -inoutmoney END) pmec_net_in
                 FROM silver_njs.history_transfer@silver_std
                 WHERE trunc(realdate) > to_date(to_char(sysdate-1,'yyyymmdd'), 'yyyymmdd')
                 GROUP BY firmid) c
        ON a.firm_id = c.firmid
      LEFT JOIN   /*查找龙商FIRM_ID*/
        (SELECT
                   firmid,
                   username
                 FROM SILVER.TB_SILVER_ACCOUNT@SILVERONLINE_LINK
                 WHERE partner_id = 'hht') e
        ON a.user_name = e.username
      LEFT JOIN     /*龙商本周净入金总*/
        (SELECT
                   fund_id,
                   sum(charge_amount) AS netinmoney
                 FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT
                 WHERE trunc(CREATE_TIME) > to_date(20170422, 'yyyymmdd')
                       AND ORDER_STATUS = 3 AND RECONC_STATUS = 2
                 GROUP BY fund_id) d
        ON e.firmid = d.fund_id
      left join     /*广贵本周净入金*/
        (SELECT
                   firmid,
                   sum(CASE WHEN inorout = 'A'
                     THEN inoutmoney
                       ELSE -inoutmoney END) pmec_net_in
                 FROM silver_njs.history_transfer@silver_std
                 WHERE trunc(realdate) > to_date(20170422, 'yyyymmdd')
                 GROUP BY firmid) g
        ON a.firm_id = g.firmid
       LEFT JOIN     /*广贵昨日净资产*/
        (SELECT
                   firmid,
                   net_assets
                 FROM silver_njs.tb_silver_data_center@silver_std
                 WHERE hdate = to_char(sysdate-1,'yyyymmdd')) f
        ON a.firm_id = f.firmid
    WHERE b.net_assets > 0
  ) aaa;

/* HHT昨日净资产 */
SELECT sum(last_capital)
FROM NSIP_ACCOUNT.TB_NSIP_A_FUNDS_AFTER_SETTLE@LINK_NSIP_ACCOUNT
WHERE to_char(trade_date, 'yyyymmdd') = to_char(sysdate-1,'yyyymmdd');

/* HHT今日总入金 */
SELECT sum(charge_amount)
FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT
WHERE to_char(trade_date, 'yyyymmdd') > to_char(sysdate-1,'yyyymmdd')；


SELECT *
FROM silver_consult.tb_crm_transfer_record@consul_std
WHERE to_char(submit_time, 'yyyymmdd') = 20170424

select * from info_silver.DM_CRM_TRANSFER_STAT


select * from NSIP_ACCOUNT.TB_NSIP_A_FUNDS_AFTER_SETTLE@LINK_NSIP_ACCOUNT
select * from NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT

/* 报表核对-CRM系统-资金转移-净入金 */
SELECT sum(a.charge_amount)
FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT a
  JOIN
  tb_silver_user_stat@silver_std c
    ON a.fund_id = c.firm_id
  JOIN
  tb_silver_user_stat@silver_std d
    ON c.user_id = d.user_id
       AND d.partner_id = 'pmec'
  JOIN
  (SELECT
     partner_id,
     net_assets,
     firmid
   FROM silver_njs.tb_silver_data_center@silver_std
   WHERE hdate = 20170421) b
    ON d.firm_id = b.firmid
  JOIN info_silver.dw_user_account e
    ON b.firmid = e.firm_id
       AND e.group_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 105, 106, 111, 112, 113, 114)
       AND e.group_id IS NOT NULL
WHERE a.order_status = 3 AND a.reconc_status = 2
      AND to_char(a.trade_date, 'yyyymmdd') > to_char(sysdate-3,'yyyymmdd')



/* 报表核对-CRM系统-资金转移-净资产 */
SELECT sum(a.net_zcmoney)
FROM info_silver.ods_order_zcmoney a
  JOIN
  tb_silver_user_stat@silver_std c
    ON a.firm_id = c.firm_id
  JOIN
  tb_silver_user_stat@silver_std d
    ON c.user_id = d.user_id
       AND d.partner_id = 'pmec'
  JOIN
  (SELECT
     partner_id,
     net_assets,
     firmid
   FROM silver_njs.tb_silver_data_center@silver_std
   WHERE hdate = 20170421) b
    ON d.firm_id = b.firmid
  JOIN info_silver.dw_user_account e
    ON b.firmid = e.firm_id
       AND e.group_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 105, 106, 111, 112, 113, 114)
       AND e.group_id IS NOT NULL
WHERE a.fdate = to_char(sysdate-1,'yyyymmdd')
and a.partner_id = 'hht'


SELECT sum(charge_amount)
FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT a
WHERE a.order_status = 3 AND a.reconc_status = 2
      AND to_char(a.trade_date, 'yyyymmdd') > 20170421

SELECT *
FROM NSIP_ACCOUNT.TB_NSIP_A_FUNDS_AFTER_SETTLE@LINK_NSIP_ACCOUNT

SELECT sum(net_zcmoney)
FROM info_silver.ods_order_zcmoney
WHERE partner_id = 'hht' AND fdate = 20170424





---各组净资产
SELECT sum(a.net_zcmoney)
FROM
  (
    SELECT
      partner_id,
      net_zcmoney,
      firm_id
    FROM info_silver.ods_order_zcmoney
    WHERE fdate = 20170421 AND partner_id = 'pmec') a
  JOIN info_silver.dw_user_account b
    ON a.firm_id = b.firm_id
--join info_silver.tb_crm_ia c  on b.ia_id = c.id
WHERE b.group_id IN (2, 3, 4, 5, 6, 9, 10, 11, 12, 105, 106, 112, 113, 114)
      AND b.ia_id IS NOT NULL


SELECT *
FROM info_silver.ods_order_zcmoney
