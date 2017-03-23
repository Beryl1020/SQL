select * from tb_account_detail@SILVER_STAT_URS_30_LINK where
--operation_amount>1000000
-- note='您从建设银行(企业)充值成功'
operation_type=6
--regexp_like(operation_id,'[0-9]+TX[0-9]+')


select * from tb_account_detail@SILVER_STAT_URS_30_LINK where
--operation_amount>1000000
-- note='您从建设银行(企业)充值成功'
regexp_like(operation_id,'[0-9]+TX[0-9]+')