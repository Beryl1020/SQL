
/* 平均累计净入金 */
SELECT
  user_id,
  crm_user_id,
  avg(ljrj)
FROM
  (
    SELECT
      a.user_id,
      a.crm_user_id,
      b.fdate,
      sum(b.ljrj) ljrj
    FROM
      (SELECT *
       FROM info_silver.ods_history_user
       WHERE partner_id IN ('pmec', 'hht')) a
      JOIN /* 到每天的累计净入金 */
      (
        SELECT
          a.fdate,
          firmid,
          sum(CASE WHEN inorout = 'A'
            THEN inoutmoney
              ELSE -inoutmoney END) ljrj
        FROM (
               SELECT DISTINCT fdate
               FROM silver_njs.history_transfer@silver_std
               WHERE fdate BETWEEN 20170401 AND 20170421
             ) a LEFT JOIN
          silver_njs.history_transfer@silver_std b
            ON b.fdate <= a.fdate
        GROUP BY a.fdate, firmid
      ) b
        ON a.firm_id = b.firmid
    WHERE a.user_id IN
          ('245819136','28216820','268618024','266186803','34362650','240701965','263879795','1000221133','1000077163','1000036739','1000116344','1000262759','1000181613','61298879','1000307933','1000116347','1000523754','1000420592')
    or a.crm_user_id in ('245819136','28216820','268618024','266186803','34362650','240701965','263879795','1000221133','1000077163','1000036739','1000116344','1000262759','1000181613','61298879','1000307933','1000116347','1000523754','1000420592'
)
    group by a.user_id,
      a.crm_user_id,
      b.fdate)
group by user_id,
  crm_user_id


/* 总累计净入金 */

select b.user_id, b.crm_user_id, sum(case when a.inorout='A' then a.inoutmoney when a.inorout='B' then -inoutmoney end)
from silver_njs.history_transfer@silver_std a
join info_silver.dw_user_account b
  on a.firmid=b.firm_id
where a.fdate <= 20170421
and  b.user_id IN
          ('245819136','28216820','268618024','266186803','34362650','240701965','263879795','1000221133','1000077163','1000036739','1000116344','1000262759','1000181613','61298879','1000307933','1000116347','1000523754','1000420592')
    or b.crm_user_id in ('245819136','28216820','268618024','266186803','34362650','240701965','263879795','1000221133','1000077163','1000036739','1000116344','1000262759','1000181613','61298879','1000307933','1000116347','1000523754','1000420592'
)
and a.partnerid in ('pmec','hht')
group by b.user_id, b.crm_user_id
