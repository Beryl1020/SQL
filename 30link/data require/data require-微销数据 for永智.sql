SELECT
  f.fa_id              主站id,
  f.crm_name           客户姓名,
  f.fia_id             投顾id,
  f.fname              投顾姓名,
  f.submit_time        流转时间,
  f.jhzj               激活资金,
  f.net_in             后端净入金,
  f.jye                后端交易额,
  f.sxf                后端手续费,
  f.sr + f.znj + f.sxf 后端收入,
  g.net_assets         净资产
FROM
  (
    SELECT
      e.*,
      nvl(sum(CASE WHEN changetype = '8'
        THEN -amount END), 0) znj,
      nvl(sum(CASE WHEN changetype IN (9, 10)
        THEN -amount END), 0) sr
    FROM
      (
        SELECT
          c.*,
          sum(d.contqty)           jye,
          sum(d.contqty) * 0.00065 sxf
        FROM
          (
            SELECT
              a.fa_id,
              a.firm_id,
              a.crm_name,
              a.fia_id,
              a.fname,
              a.submit_time,
              a.PMEC_NET_VALUE_SUB + a.PMEC_NET_IN_SUB jhzj,
              sum(CASE WHEN b.inorout = 'A'
                THEN b.inoutmoney
                  ELSE -b.inoutmoney END)              net_in
            FROM info_silver.ods_crm_transfer_record a
              LEFT JOIN silver_njs.history_transfer@silver_std b
                ON a.firm_id = b.firmid AND b.realdate > a.submit_time
            WHERE a.fgroup_id IN (112, 113, 114, 106) AND fia_id != 154 AND a.process IN (5, 6)
            GROUP BY a.fa_id, a.firm_id, a.crm_name, a.fia_id, a.fname, a.submit_time,
              a.PMEC_NET_VALUE_SUB + a.PMEC_NET_IN_SUB
          ) c
          LEFT JOIN info_silver.ods_history_deal d ON c.firm_id = d.firmid AND d.trade_time > c.submit_time
        GROUP BY c.fa_id, c.firm_id, c.crm_name, c.fia_id, c.fname, c.submit_time, c.jhzj, c.net_in
      ) e LEFT JOIN silver_njs.pmec_zj_flow@silver_std f ON e.firm_id = f.loginaccount AND f.createdate > e.submit_time
    GROUP BY e.fa_id, e.firm_id, e.crm_name, e.fia_id, e.fname, e.submit_time, e.jhzj, e.net_in, e.jye, e.sxf
  ) f LEFT JOIN silver_njs.tb_silver_data_center@silver_std g ON f.firm_id = g.firmid
WHERE trunc(f.submit_time) <= sysdate - 1 AND g.hdate = to_char(sysdate - 1, 'yyyymmdd')


