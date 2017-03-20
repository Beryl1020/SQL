select suba.id,suba.maxassets,suba.maxcontnum
  from

(select distinct data.firmid as id, max(data.net_assets) as maxassets,max(deal30.contnum) as maxcontnum
from silver_njs.tb_silver_data_center data
  left join ods_history_deal@silver_stat_urs_30_link deal30
  on deal30.firmid=data.firmid and deal30.partner_id=data.partner_id
where data.partner_id='pmec'
 and to_char(data.update_time,'yyyymmdd') between 20161002 and 20170101
 and deal30.fdate between 20161001 and 20161231
 and data.net_assets is not NULL
 and deal30.contnum is not NULL
group by data.firmid) suba

where suba.maxassets>=100000 and suba.maxcontnum>0


