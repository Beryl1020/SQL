select distinct b.group_id
from silver_consult.v_tb_crm_user@consult_std a
  left join
  silver_consult.tb_crm_ia@consult_std b
  on a.ia_id=b.id
where b.group_id is not null
order by b.group_id






select * from silver_consult.tb_crm_ia@consult_std