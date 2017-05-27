SELECT *
FROM tb_silver_user_stat@silver_std                   --1
SELECT *
FROM info_silver.ods_history_user                     --2
SELECT *
FROM info_silver.ods_history_deal                     --3
SELECT *
FROM silver_njs.pmec_zj_flow@silver_std               --4
SELECT *
FROM silver_njs.tb_silver_data_center@silver_std     --资产表
SELECT *
FROM silver_njs.history_transfer@silver_std          --出入金表
SELECT *
FROM silver_consult.v_tb_crm_user@consul_std          --7
SELECT *
FROM info_silver.tb_crm_user                          --8
SELECT *
FROM silver_consult.tb_crm_transfer_record@consul_std --9
SELECT *
FROM info_silver.ods_crm_transfer_record              --流转单表
SELECT *
FROM silver_consult.tb_crm_tel_record@consul_std      --11
SELECT *
FROM info_silver.tb_crm_tel_record                    --12
SELECT *
FROM silver_consult.tb_crm_ia@consul_std              --13
SELECT *
FROM info_silver.tb_crm_ia                            --14
SELECT *
FROM silver_consult.tb_crm_dispatch_his@consul_std    --15
SELECT *
FROM info_silver.tb_crm_tag_user_rel                  --16
SELECT *
FROM silver_consult.tb_crm_tag@consul_std             --17
SELECT *
FROM silver_consult.tb_crm_tag_user_rel@consul_std    --18
SELECT *
FROM info_silver.pmec_zj_flow where to_char(fdate,'yyyymm') = '201703' and changetype in (3,4) and amount =0
SELECT *
FROM info_silver.edw_fund_fact_d

SELECT *
FROM info_silver.pmec_zj_flow
SELECT *
FROM info_silver.edw_user_fact_d
SELECT *
FROM info_silver.dw_user_account         --史上最全user表
SELECT *
FROM silver_njs.pmec_account_info@silver_std
SELECT *
FROM info_silver.ods_crm_user
SELECT *
FROM NSIP_ACCOUNT.TB_NSIP_A_FUNDS_AFTER_SETTLE@LINK_NSIP_ACCOUNT --HHT净资产
SELECT *
FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT --HHT净入金
SELECT *
FROM info_silver.tb_nsip_t_filled_order


SELECT *
FROM info_silver.tb_silver_account



SELECT *
FROM NSIP_TRADE.TB_NSIP_T_FILLED_ORDER@LINK_NSIP_TRADE  --实时交易表


SELECT *
FROM NSIP_TRADE.TB_NSIP_T_POSITION_DETAIL_H@LINK_NSIP_TRADE
SELECT *
FROM NSIP_ACCOUNT.tb_nsip_account_funds_bill@LINK_NSIP_ACCOUNT

SELECT *
FROM info_silver.ods_order_zcmoney where fdate = 20170526 and partner_id = 'hht'


SELECT *
FROM info_silver.edw_fund_fact_d

SELECT *
FROM info_silver.order_fund_silver

SELECT *
FROM info_silver.tb_nsip_a_funds_after_settle

SELECT *
FROM SILVER_NJS.HISTORY_DEAL@SILVERONLINE_LINK

SELECT *
FROM info_silver.rpt_crm_transfer_user_stat where to_char(stat_date,'yyyymmdd') = 20170526


select * from info_silver.ods_crm_tel_record







