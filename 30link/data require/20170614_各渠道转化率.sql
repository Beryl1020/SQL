/*渠道数量*/
SELECT
  count(DISTINCT
        CASE WHEN a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
                  a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
          THEN a.sub_refer END) AS 信息流渠道,
  count(DISTINCT CASE WHEN a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR
                           a.sub_refer LIKE '%tgwbyb360%'
    THEN a.sub_refer END)       AS 应用市场,
  count(DISTINCT
        CASE WHEN a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
                  a.sub_refer LIKE '%tgwlpwpudao%'
          THEN a.sub_refer END) AS 直邮,
  count(DISTINCT CASE WHEN a.sub_refer LIKE '%tgwduiba%'
    THEN a.sub_refer END)       AS 其他
FROM info_silver.dw_user_account a
WHERE a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
      a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
      OR a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR a.sub_refer LIKE '%tgwbyb360%'
      OR a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
      a.sub_refer LIKE '%tgwlpwpudao%'
      OR a.sub_refer LIKE '%tgwduiba%'








/*用户及入金数量*/
SELECT
  count(DISTINCT
        CASE WHEN a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
                  a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
          THEN a.user_id END) AS 信息流渠道,
  count(DISTINCT CASE WHEN a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR
                           a.sub_refer LIKE '%tgwbyb360%'
    THEN a.user_id END)       AS 应用市场,
  count(DISTINCT
        CASE WHEN a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
                  a.sub_refer LIKE '%tgwlpwpudao%'
          THEN a.user_id END) AS 直邮,
  count(DISTINCT CASE WHEN a.sub_refer LIKE '%tgwduiba%'
    THEN a.user_id END)       AS 其他
FROM info_silver.dw_user_account a
  JOIN silver_njs.history_transfer@silver_std b
    ON a.firm_id = b.firmid
WHERE (a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
      a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
      OR a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR a.sub_refer LIKE '%tgwbyb360%'
      OR a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
      a.sub_refer LIKE '%tgwlpwpudao%'
      OR a.sub_refer LIKE '%tgwduiba%')
         AND a.partner_id = 'hht'





/*交易数量*/
SELECT
  count(DISTINCT
        CASE WHEN a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
                  a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
          THEN a.crm_user_id END) AS 信息流渠道,
  count(DISTINCT CASE WHEN a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR
                           a.sub_refer LIKE '%tgwbyb360%'
    THEN a.crm_user_id END)       AS 应用市场,
  count(DISTINCT
        CASE WHEN a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
                  a.sub_refer LIKE '%tgwlpwpudao%'
          THEN a.crm_user_id END) AS 直邮,
  count(DISTINCT CASE WHEN a.sub_refer LIKE '%tgwduiba%'
    THEN a.crm_user_id END)       AS 其他
FROM info_silver.dw_user_account a
  JOIN info_silver.ods_history_deal b
    ON a.firm_id = b.firmid
WHERE (a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
      a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
      OR a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR a.sub_refer LIKE '%tgwbyb360%'
      OR a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
      a.sub_refer LIKE '%tgwlpwpudao%'
      OR a.sub_refer LIKE '%tgwduiba%')
         AND a.partner_id = 'hht'






/*开启app用户数量*/
SELECT
  count(DISTINCT
        CASE WHEN a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
                  a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
          THEN a.user_id END) AS 信息流渠道,
  count(DISTINCT CASE WHEN a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR
                           a.sub_refer LIKE '%tgwbyb360%'
    THEN a.user_id END)       AS 应用市场,
  count(DISTINCT
        CASE WHEN a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
                  a.sub_refer LIKE '%tgwlpwpudao%'
          THEN a.user_id END) AS 直邮,
  count(DISTINCT CASE WHEN a.sub_refer LIKE '%tgwduiba%'
    THEN a.user_id END)       AS 其他
FROM info_silver.dw_user_account a
  JOIN info_silver.ods_history_deal b
    ON a.firm_id = b.firmid
WHERE a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
      a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
      OR a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR a.sub_refer LIKE '%tgwbyb360%'
      OR a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
      a.sub_refer LIKE '%tgwlpwpudao%'
      OR a.sub_refer LIKE '%tgwduiba%'
         AND a.partner_id = 'hht'
         AND a.last_login_time IS NOT NULL




/*平均交易量*/
SELECT
  avg(DISTINCT
        CASE WHEN a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
                  a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
          THEN a.contqty END) AS 信息流渠道,
  avg(DISTINCT CASE WHEN a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR
                           a.sub_refer LIKE '%tgwbyb360%'
    THEN a.contqty END)       AS 应用市场,
  avg(DISTINCT
        CASE WHEN a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
                  a.sub_refer LIKE '%tgwlpwpudao%'
          THEN a.contqty END) AS 直邮,
  avg(DISTINCT CASE WHEN a.sub_refer LIKE '%tgwduiba%'
    THEN a.contqty END)       AS 其他
from
  (
    SELECT
      a.sub_refer,
      a.user_id,
      sum(b.contqty) AS contqty
    FROM info_silver.dw_user_account a
      JOIN info_silver.ods_history_deal b
        ON a.firm_id = b.firmid
    WHERE (a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
          a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
          OR a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR a.sub_refer LIKE '%tgwbyb360%'
          OR a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
          a.sub_refer LIKE '%tgwlpwpudao%'
          OR a.sub_refer LIKE '%tgwduiba%'
             AND a.partner_id = 'hht')
    GROUP BY a.sub_refer, a.user_id
  ) a




/*平均净入金量*/
SELECT
  avg(DISTINCT
        CASE WHEN a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
                  a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
          THEN a.netin END) AS 信息流渠道,
  avg(DISTINCT CASE WHEN a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR
                           a.sub_refer LIKE '%tgwbyb360%'
    THEN a.netin END)       AS 应用市场,
  avg(DISTINCT
        CASE WHEN a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
                  a.sub_refer LIKE '%tgwlpwpudao%'
          THEN a.netin END) AS 直邮,
  avg(DISTINCT CASE WHEN a.sub_refer LIKE '%tgwduiba%'
    THEN a.netin END)       AS 其他
from
  (
    SELECT
      a.sub_refer,
      a.user_id,
      sum(case when b.inorout='A' then b.inoutmoney when b.inorout='B' then -b.inoutmoney end) AS netin
    FROM info_silver.dw_user_account a
      JOIN silver_njs.history_transfer@silver_std b
        ON a.firm_id = b.firmid
    WHERE (a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
          a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
          OR a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR a.sub_refer LIKE '%tgwbyb360%'
          OR a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
          a.sub_refer LIKE '%tgwlpwpudao%'
          OR a.sub_refer LIKE '%tgwduiba%')
             AND a.partner_id = 'hht'
    GROUP BY a.sub_refer, a.user_id
  ) a





/*拨打用户数*/
SELECT
  avg(DISTINCT
        CASE WHEN a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
                  a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
          THEN b.worksec END) AS 信息流渠道,
  avg(DISTINCT CASE WHEN a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR
                           a.sub_refer LIKE '%tgwbyb360%'
    THEN b.worksec END)       AS 应用市场,
  avg(DISTINCT
        CASE WHEN a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
                  a.sub_refer LIKE '%tgwlpwpudao%'
          THEN b.worksec END) AS 直邮,
  avg(DISTINCT CASE WHEN a.sub_refer LIKE '%tgwduiba%'
    THEN b.worksec END)       AS 其他
FROM info_silver.dw_user_account a
  JOIN info_silver.tb_crm_tel_record b
    ON a.crm_user_id= b.user_id
WHERE (a.sub_refer LIKE '%pzxxmxxl%' OR a.sub_refer LIKE '%pzxnearme%' OR a.sub_refer LIKE '%pzxvivo%' OR
      a.sub_refer LIKE '%pzxxl360%' OR a.sub_refer LIKE '%pzprucxxl%'
      OR a.sub_refer LIKE '%pz360szkp1%' OR a.sub_refer LIKE '%pz360swkp1%' OR a.sub_refer LIKE '%tgwbyb360%'
      OR a.sub_refer LIKE '%tgwlpw139%' OR a.sub_refer LIKE '%tgwlpwsd%' OR a.sub_refer LIKE '%tgwlpwyw%' OR
      a.sub_refer LIKE '%tgwlpwpudao%'
      OR a.sub_refer LIKE '%tgwduiba%')
         AND a.partner_id = 'hht'
      and b.worksec>0