

/*出入金记录*/
SELECT *
FROM silver_njs.history_transfer@silver_std
WHERE firmid = 163000000139613 AND fdate BETWEEN 20170410 AND 20170414;

/*持仓记录*/
SELECT *
FROM info_silver.pmec_hold_position_order
WHERE loginaccount = 163000000139613 AND to_char(fdate, 'yyyymmdd') BETWEEN 20170410 AND 20170414;

/*交易记录*/
select * from info_silver.ods_history_deal
where firmid = 163000000139613 AND fdate BETWEEN '20170410' AND '20170414';




/*出入金记录*/
SELECT *
FROM silver_njs.history_transfer@silver_std
WHERE firmid = 31836371 AND fdate BETWEEN 20170410 AND 20170414;

/*持仓记录*/
SELECT *
FROM silver_njs.cc@silveronline_link
WHERE firmid = 31836371 AND hdate BETWEEN 20170410 AND 20170414;


/*交易记录*/
select * from info_silver.ods_history_deal
where firmid = 31836371 AND fdate BETWEEN '20170410' AND '20170414';




