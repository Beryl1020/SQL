select a.*, b.net_assets as "现资产", 总入金-net_assets as "累计亏损"
from
 (select a.crm_name,firm_id,cur_bname,cur_bgroup_id,当日亏损,submit_time,sum(amount) 总入金
  from

   (select a.crm_name,firm_id,cur_bname,cur_bgroup_id,submit_time,sum(-amount) 当日亏损
    from

     (select a.*,b.firmid
      from

       (select crm_name,firm_id,cur_bname,cur_bgroup_id,submit_time
        from
        info_silver.ods_crm_transfer_record@silver_stat_urs_30_link a
        where valid=1 and process in (5,6)) a
       left join
        (select   distinct firmid
         from ods_history_deal@silver_stat_urs_30_link
         where  partner_id='pmec' and to_char(sysdate-1,'yyyymmdd') =fdate and ordersty=151
        )b
       on a.firm_id=b.firmid
       where b.firmid is not null )a

     left join

     silver_njs.pmec_zj_flow b
     on a.firmid=b.loginaccount and b.changetype in (9,10) and fdate=trunc(sysdate-1)
     group by crm_name,firm_id,cur_bname,cur_bgroup_id,submit_time)a

   left join silver_njs.pmec_zj_flow b
   on a.firm_id=b.loginaccount  and b.changetype in (15,16)
   group by a.crm_name,firm_id,cur_bname,cur_bgroup_id,当日亏损,submit_time)a
  left join
  silver_njs.tb_silver_data_center b
  on a.firm_id=b.firmid and b.hdate=to_char(sysdate-1,'yyyymmdd')
