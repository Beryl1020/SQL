SELECT
  partner_id, sum(CONTQTY)                                                                  --各平台交易额
FROM ods_history_deal@silver_stat_urs_30_link
WHERE fdate BETWEEN 20170201 AND 20170228
GROUP BY partner_id


select
  sum (case when aa.fgroup_id in (2,3,4,5,6,9,10,11,12,105) then bb.contqty end),           --各销售方式交易额
  sum(case when aa.fgroup_id in (106,112,113,114) then bb.contqty end)
  from
(select firm_id,fia_id,fgroup_id from info_silver.ods_crm_transfer_record@silver_stat_urs_30_link
where to_char(submit_time,'yyyymmdd') <= 20170228) aa
    join ods_history_deal@silver_stat_urs_30_link bb
  on aa.firm_id=bb.firmid
where bb.fdate between 20170201 and 20170228

