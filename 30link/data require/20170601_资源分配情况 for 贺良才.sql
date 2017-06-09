SELECT
  a.ia_id,
  b.name,
  case when b.group_id is not null then b.group_id else a.group_id end as group_id,
  count(DISTINCT a.user_id),
  to_char(a.create_time, 'yyyymmdd')
FROM silver_consult.tb_crm_dispatch_his@consul_std a
  LEFT JOIN info_silver.tb_crm_ia b
    ON a.ia_id = b.id
WHERE a.type = 11 AND to_char(a.create_time, 'yyyymmdd') = to_char(sysdate-1,'yyyymmdd')
GROUP BY to_char(a.create_time, 'yyyymmdd'), a.ia_id, b.name, a.group_id,b.group_id


SELECT *
FROM silver_consult.tb_crm_dispatch_his@consul_std a
WHERE a.type = 11 AND to_char(a.create_time, 'yyyymmdd') = 20170531 and ia_id is null


SELECT b.user_id ,b.crm_user_id,b.ia_id,b.group_id,b.ia_name,c.tag_id,d.create_time,a.connect_time
FROM
  (SELECT
     user_id,
     max(crm_user_id) AS crm_user_id,
     max(ia_id)       AS ia_id,
     max(group_id)    AS group_id,
     max(ia_name)     AS ia_name
   FROM info_silver.dw_user_account
   GROUP BY user_id) b
  LEFT JOIN (SELECT
               user_id,
               max(create_time) as connect_time
             FROM info_silver.ods_crm_tel_record
               where worksec>0
            group by user_id) a
  on a.user_id = b.crm_user_id
  LEFT JOIN
  (select useR_id, tag_id from silver_consult.tb_crm_tag_user_rel@consul_std where tag_id in (38,39,40)) c
    ON b.crm_user_id = c.user_id
  left join (select userid, max(create_time) as create_time from info_silver.tb_crm_memo group by userid) d
    on b.crm_user_id = d.userid
where b.crm_user_id is not NULL
and   (d.create_time is null or to_char(d.create_time,'yyyymmdd')<='20170505')
and    (a.connect_time is null or to_char(a.connect_time,'yyyymmdd')<='20170505')
and b.group_id in (2,3,4,5,6,9,10,11,12,105,106,117,116)




select count(distinct useR_id) from  info_silver.dw_user_account   where crm_user_id is not NULL


select * from  info_silver.ods_crm_tel_record where user_id ='1000629439'


select * from silver_consult.tb_crm_memo@consul_std

