SELECT *
FROM tb_silver_user_stat@silver_std                   --1
SELECT *
FROM info_silver.ods_history_user                     --2
SELECT *
FROM info_silver.ods_history_deal                     --3
SELECT *
FROM silver_njs.pmec_zj_flow@silver_std               --4
SELECT *
FROM silver_njs.tb_silver_data_center@silver_std      --5
SELECT *
FROM silver_njs.history_transfer@silver_std           --6
SELECT *
FROM silver_consult.v_tb_crm_user@consul_std          --7
SELECT *
FROM info_silver.tb_crm_user                          --8
SELECT *
FROM silver_consult.tb_crm_transfer_record@consul_std --9
SELECT *
FROM info_silver.ods_crm_transfer_record              --10
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
FROM info_silver.pmec_zj_flow
SELECT *
FROM info_silver.edw_fund_fact_d
WHERE partner_id = 'pmec'
SELECT *
FROM info_silver.pmec_zj_flow




