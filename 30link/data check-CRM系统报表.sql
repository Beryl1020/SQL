
/* 有效通话次数 */
SELECT *
FROM info_silver.tb_crm_tel_record a
  JOIN
  (select * from
    (SELECT
       user_id,
       min(create_time) AS mindate
     FROM silver_consult.tb_crm_dispatch_his@consul_std
     WHERE ia_id IS NOT NULL
     GROUP BY user_id) b
      JOIN
    (SELECT user_id as id2, ia_id, create_time
     FROM silver_consult.tb_crm_dispatch_his@consul_std
    ) C
      ON b.user_id= C.id2 AND b.mindate = C.create_time
  ) dd
    ON a.user_id = dd.id2
WHERE a.ia_id = 540
      AND (a.worksec > 0 OR (a.worksec = 0 AND a.billsec >= 30))
      AND to_char(a.create_time, 'yyyymmdd') BETWEEN 20170410 AND 20170411
      AND to_char(dd.mindate, 'yyyymmdd') BETWEEN 20170410 AND 20170411;





select a.name,b.* from
(
select a.ia_id,
sum(case when b.worksec>0 then 1 when b.worksec=0 and b.billsec>=30 then 1 end) 有效拨打次数
from
(select user_id,ia_id,min(create_time) dispatch_time from silver_consult.tb_crm_dispatch_his@consult_std where ia_id is not null group by user_id,ia_id) a
join silver_consult.tb_crm_ia@consult_std c
on a.ia_id=c.id
left join
(select user_id,create_time,billsec,worksec from silver_consult.tb_crm_tel_record@consult_std where to_char(create_time,'yyyymmdd') between '20170410' and '20170411') b
on a.user_id=b.user_id
where c.group_id='6' and to_char(a.dispatch_time,'yyyymmdd') between '20170410' and '20170411'
group by a.ia_id
) b join silver_consult.tb_crm_ia@consult_std a on a.id=b.ia_id;