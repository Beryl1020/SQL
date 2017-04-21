select * from info_silver.ods_order_silver
where firmid= 163000000139613

select a.firmid,a.buyorsal,a.wareid,a.contprice,a.contnum,to_char(trade_time,'yyyy-mm-dd hh:mi:ss')
  from info_silver.ods_order_silver a
where to_char(trade_time,'yyyy-mm-dd hh:mi:ss')>='2017-04-06 00:00:00'
and partner_id='pmec'

select * from info_silver.tb_room_strategy
where to_char(uptime,'yyyymmdd')=20170413

select * from silver_consult.tb_room_strategy@silver_std