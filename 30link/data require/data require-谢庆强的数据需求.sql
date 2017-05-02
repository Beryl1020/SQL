



/* 广贵净出金 */
select b.user_id,e.real_name,a.firmid,c.firm_id,a.pmec_net_in,e.id,e.name,e.group_id from
  (SELECT
                   firmid,
                   sum(CASE WHEN inorout = 'A'
                     THEN inoutmoney
                       ELSE -inoutmoney END) pmec_net_in
                 FROM silver_njs.history_transfer@silver_std
                 WHERE trunc(realdate) > to_date(20170422, 'yyyymmdd')
                 GROUP BY firmid) a
  left join tb_silver_user_stat@silver_std b
  on a.firmid=b.firm_id
  left join tb_silver_user_stat@silver_std c
    on b.user_id = c.user_id
   and c.partner_id = 'hht'
  left join silver_consult.v_tb_crm_user@consul_std d
    on b.user_id = d.fa_id
  left join info_silver.tb_crm_ia e
    on d.ia_id = e.id
  left join   info_silver.dw_user_account  e
    on a.firmid = e.firm_id
where a.pmec_net_in <= -1000


SELECT
  fund_id,
  sum(charge_amount) AS netinmoney
FROM NSIP_ACCOUNT.TB_NSIP_ACCOUNT_CHARGE_ORDER@LINK_NSIP_ACCOUNT
WHERE trunc(CREATE_TIME) > to_date(20170422, 'yyyymmdd')
      AND ORDER_STATUS = 3 AND RECONC_STATUS = 2
GROUP BY fund_id




SELECT sum(used_margin)
FROM silver_njs.pmec_account_info@silver_std
where to_char(fdate,'yyyymmdd') = 20170501


select * from tb_silver_user_stat@silver_std where firm_id =163000000004214