
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
WHERE a.ia_id = 540 and dd.ia_id=540
      AND (a.worksec > 0 OR (a.worksec = 0 AND a.billsec >= 30))
      AND to_char(a.create_time, 'yyyymmdd') BETWEEN 20170410 AND 20170411
      AND to_char(dd.mindate, 'yyyymmdd') BETWEEN 20170410 AND 20170411;




select * from silver_consult.tb_crm_dispatch_his@consul_std where user_id= 1000600465




select * from silver_consult.tb_crm_dispatch_his@consul_std where user_id=1000597745

select * from info_silver.tb_crm_tel_record
  where user_id = 1000598764
        ('1000600538','1000594501','1000600781','1000598063','1000595440','1000598944',
'1000596855','1000598243','1000597426','1000600918','1000598500','1000597896','1000600677',
'1000600255','1000594712','1000597088','1000595728','1000594565','1000594344','1000596957',
'1000597533','1000597745','1000601310','1000598341','1000600465','1000594240','1000601093',
'1000600905','1000595636','1000597593','1000600189','1000594060','1000594846','1000598606',
'1000597295','1000597783','1000597643','1000597711')







select distinct a.user_id
from
(
select * from
(
select user_id,create_time,ia_id,row_number() over(partition by user_id order by create_time asc) rn from silver_consult.tb_crm_dispatch_his@consul_std where ia_id is not null
and to_char(create_time,'yyyymmdd') between '20170410' and '20170411'
)
where rn=1)
a
join silver_consult.tb_crm_ia@consul_std c
on a.ia_id=c.id
left join
(select user_id,create_time,billsec,worksec from silver_consult.tb_crm_tel_record@consul_std where to_char(create_time,'yyyymmdd') between '20170410' and '20170411') b
on a.user_id=b.user_id
where  (b.worksec>0 or(b.worksec=0 and b.billsec>=30) and a.ia_id=540)

