SELECT
  refer_1_type,refer_2_type,to_char(open_account_time, 'yyyymmdd'),
  count(DISTINCT CASE WHEN partner_id = 'hht'
    THEN user_id END)
FROM info_silver.dw_user_account
WHERE to_char(open_account_time, 'yyyymm') = 201706
GROUP BY refer_1_type,sub_refer,refer_2_type,to_char(open_account_time, 'yyyymmdd')
