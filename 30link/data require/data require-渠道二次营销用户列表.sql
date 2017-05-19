select a.user_name from info_silver.dw_user_account a
where a.open_account_time <= trunc(sysdate)
and a.partner_id = 'hht'
and a.firm_id not IN
    (select distinct fund_id from NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT where create_time<=trunc(sysdate))
and a.user_name not IN
    (select username from info_silver.tb_silver_user_activity where activity_enname = 'ljsvalid' and join_time <trunc(sysdate-7))
