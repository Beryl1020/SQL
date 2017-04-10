SELECT *
FROM silver_consult.tb_crm_user_wechat_info@consul_std --where ia_alias='wyy4029';
SELECT *
FROM silver_consult.tb_crm_wechatinfo@consul_std;
SELECT *
FROM silver_consult.tb_crm_transfer_record@consul_std;
SELECT *
FROM silver_consult.tb_crm_ia@consul_std
WHERE id = 608;
SELECT *
FROM silver_consult.tb_crm_dispatch_his@consul_std;
SELECT *
FROM tb_silver_user_stat@silver_std;
SELECT *
FROM silver_consult.v_tb_crm_user@consul_std;



SELECT
     count(1) as 新增好友
   FROM silver_consult.tb_crm_user_wechat_info@consul_std
   WHERE (wechat_proto_comment LIKE 'A%' OR wechat_proto_comment LIKE 'a%') AND type = 1 AND   --新增资源数
         to_char(create_time,'yyyymmdd') between 20170318 and 20170324
         AND ia_alias IS NOT NULL


SELECT
     count(1) as 新增意向客户数
   FROM silver_consult.tb_crm_user_wechat_info@consul_std
   WHERE (wechat_proto_comment LIKE 'A%意向%' OR wechat_proto_comment LIKE 'a%意向%') AND type = 1 AND
         to_char(create_time,'yyyymmdd') between 20170318 and 20170324
         AND ia_alias IS NOT NULL








--每日没人 wk99518去掉
SELECT
  '内推'            flag,
  w.name          投顾,
  w.ia_alias,
  a.新增好友,
  b.总计好友,
  c.群个数,
  d.新增意向客户数,
  e.意向客户数,
  i.新增开户,
  h.新增出单,
  h.新增出单金额,
  j.累计出单,
  j.累计出单金额,
  j.累计单均净值,
  j.累计出单 / b.总计好友 资源转化率
FROM
  (SELECT DISTINCT
     a.iaid,
     a.weid ia_alias,
     b.name,
     b.group_id
   FROM silver_consult.tb_crm_wechatinfo@consult_std a JOIN silver_consult.tb_crm_ia@consul_std b ON a.iaid = b.id
   WHERE b.group_id IN (113, 114) OR a.weid = 'fc163168') w
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 新增好友
   FROM silver_consult.tb_crm_user_wechat_info@consul_std
   WHERE (wechat_proto_comment LIKE 'A%' OR wechat_proto_comment LIKE 'a%') AND type = 1 AND
         trunc(create_time) = trunc(sysdate) - 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) a
    ON w.ia_alias = a.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 总计好友
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'A%' OR wechat_proto_comment LIKE 'a%') AND type = 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) b
    ON w.ia_alias = b.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 群个数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE wechat_nick_name LIKE '%网易%交流群%' AND type = 3 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) c
    ON w.ia_alias = c.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 新增意向客户数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'A%意向%' OR wechat_proto_comment LIKE 'a%意向%') AND type = 1 AND
         trunc(create_time) = trunc(sysdate) - 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) d
    ON w.ia_alias = d.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 意向客户数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'A%意向%' OR wechat_proto_comment LIKE 'a%意向%') AND type = 1
   GROUP BY ia_alias) e
    ON w.ia_alias = e.ia_alias
  LEFT JOIN
  (SELECT
     fia_id,
     count(1)                                       新增出单,
     sum(PMEC_NET_VALUE_SUB) + sum(PMEC_NET_IN_SUB) 新增出单金额
   FROM silver_consult.tb_crm_transfer_record@consul_std
   WHERE trunc(submit_time) = trunc(sysdate) - 1 AND process IN (5, 6)
   GROUP BY fia_id) h
    ON w.iaid = h.fia_id
  LEFT JOIN
  (SELECT
     fia_id,
     count(1)                                       累计出单,
     sum(PMEC_NET_VALUE_SUB) + sum(PMEC_NET_IN_SUB) 累计出单金额,
     avg(PMEC_NET_VALUE_SUB) + avg(PMEC_NET_IN_SUB) 累计单均净值
   FROM silver_consult.tb_crm_transfer_record@consul_std
   WHERE trunc(submit_time) <= trunc(sysdate) - 1 AND process IN (5, 6)
   GROUP BY fia_id) j
    ON w.iaid = j.fia_id
  LEFT JOIN (SELECT
               a.ia_id,
               count(1) 新增开户
             FROM silver_consult.v_tb_crm_user@consul_std a JOIN tb_silver_user_stat@silver_std b ON a.fa_id = b.user_id
             WHERE trunc(b.open_account_time) = trunc(sysdate) - 1
             GROUP BY a.ia_id) i
    ON w.iaid = i.ia_id
UNION
SELECT
  '外推'            flag,
  w.name          投顾,
  w.ia_alias,
  a.新增好友,
  b.总计好友,
  c.群个数,
  d.新增意向客户数,
  e.意向客户数,
  i.新增开户,
  h.新增出单,
  h.新增出单金额,
  j.累计出单,
  j.累计出单金额,
  j.累计单均净值,
  j.累计出单 / b.总计好友 资源转化率
FROM
  (SELECT DISTINCT
     a.iaid,
     a.weid ia_alias,
     b.name,
     b.group_id
   FROM silver_consult.tb_crm_wechatinfo@consult_std a JOIN silver_consult.tb_crm_ia@consul_std b ON a.iaid = b.id
   WHERE b.group_id IN (112) AND a.weid != 'wk99518') w
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 新增好友
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'W%' OR wechat_proto_comment LIKE 'w%') AND type = 1 AND
         trunc(create_time) = trunc(sysdate) - 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) a
    ON w.ia_alias = a.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 总计好友
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'W%' OR wechat_proto_comment LIKE 'w%') AND type = 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) b
    ON w.ia_alias = b.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 群个数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE wechat_nick_name LIKE '%网易%交流群%' AND type = 3 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) c
    ON w.ia_alias = c.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 新增意向客户数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'W%意向%' OR wechat_proto_comment LIKE 'w%意向%') AND type = 1 AND
         trunc(create_time) = trunc(sysdate) - 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) d
    ON w.ia_alias = d.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 意向客户数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'W%意向%' OR wechat_proto_comment LIKE 'w%意向%') AND type = 1
   GROUP BY ia_alias) e
    ON w.ia_alias = e.ia_alias
  LEFT JOIN
  (SELECT
     fia_id,
     count(1)                                       新增出单,
     sum(PMEC_NET_VALUE_SUB) + sum(PMEC_NET_IN_SUB) 新增出单金额
   FROM silver_consult.tb_crm_transfer_record@consul_std
   WHERE trunc(submit_time) = trunc(sysdate) - 1 AND process IN (5, 6)
   GROUP BY fia_id) h
    ON w.iaid = h.fia_id
  LEFT JOIN
  (SELECT
     fia_id,
     count(1)                                       累计出单,
     sum(PMEC_NET_VALUE_SUB) + sum(PMEC_NET_IN_SUB) 累计出单金额,
     avg(PMEC_NET_VALUE_SUB) + avg(PMEC_NET_IN_SUB) 累计单均净值
   FROM silver_consult.tb_crm_transfer_record@consul_std
   WHERE trunc(submit_time) <= trunc(sysdate) - 1 AND process IN (5, 6)
   GROUP BY fia_id) j
    ON w.iaid = j.fia_id
  LEFT JOIN (SELECT
               a.ia_id,
               count(1) 新增开户
             FROM silver_consult.v_tb_crm_user@consul_std a JOIN tb_silver_user_stat@silver_std b ON a.fa_id = b.user_id
             WHERE trunc(b.open_account_time) = trunc(sysdate) - 1
             GROUP BY a.ia_id) i
    ON w.iaid = i.ia_id;

--每日每组
SELECT
  w.group_id,
  sum(a.新增好友)               新增好友,
  sum(b.总计好友)               总计好友,
  sum(c.群个数)                群个数,
  sum(d.新增意向客户数)            新增意向客户数,
  sum(e.意向客户数)              意向客户数,
  sum(i.新增开户)               新增开户,
  sum(h.新增出单)               新增出单,
  sum(h.新增出单金额)             新增出单金额,
  sum(j.累计出单)               累计出单,
  sum(j.累计出单金额)             累计出单金额,
  sum(j.累计单均净值)             累计单均净值,
  sum(j.累计出单) / sum(b.总计好友) 资源转化率
FROM
  (SELECT DISTINCT
     a.iaid,
     a.weid ia_alias,
     b.name,
     b.group_id
   FROM silver_consult.tb_crm_wechatinfo@consult_std a JOIN silver_consult.tb_crm_ia@consul_std b ON a.iaid = b.id
   WHERE b.group_id IN (113, 114)) w
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 新增好友
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'A%' OR wechat_proto_comment LIKE 'a%') AND type = 1 AND
         trunc(create_time) = trunc(sysdate) - 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) a
    ON w.ia_alias = a.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 总计好友
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'A%' OR wechat_proto_comment LIKE 'a%') AND type = 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) b
    ON w.ia_alias = b.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 群个数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE wechat_nick_name LIKE '%网易%交流群%' AND type = 3 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) c
    ON w.ia_alias = c.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 新增意向客户数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'A%意向%' OR wechat_proto_comment LIKE 'a%意向%') AND type = 1 AND
         trunc(create_time) = trunc(sysdate) - 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) d
    ON w.ia_alias = d.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 意向客户数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'A%意向%' OR wechat_proto_comment LIKE 'a%意向%') AND type = 1
   GROUP BY ia_alias) e
    ON w.ia_alias = e.ia_alias
  LEFT JOIN
  (SELECT
     fia_id,
     count(1)                                       新增出单,
     sum(PMEC_NET_VALUE_SUB) + sum(PMEC_NET_IN_SUB) 新增出单金额
   FROM silver_consult.tb_crm_transfer_record@consul_std
   WHERE trunc(submit_time) = trunc(sysdate) - 1 AND process IN (5, 6)
   GROUP BY fia_id) h
    ON w.iaid = h.fia_id
  LEFT JOIN
  (SELECT
     fia_id,
     count(1)                                       累计出单,
     sum(PMEC_NET_VALUE_SUB) + sum(PMEC_NET_IN_SUB) 累计出单金额,
     avg(PMEC_NET_VALUE_SUB) + avg(PMEC_NET_IN_SUB) 累计单均净值
   FROM silver_consult.tb_crm_transfer_record@consul_std
   WHERE trunc(submit_time) <= trunc(sysdate) - 1 AND process IN (5, 6)
   GROUP BY fia_id) j
    ON w.iaid = j.fia_id
  LEFT JOIN (SELECT
               a.ia_id,
               count(1) 新增开户
             FROM silver_consult.v_tb_crm_user@consul_std a JOIN tb_silver_user_stat@silver_std b ON a.fa_id = b.user_id
             WHERE trunc(b.open_account_time) = trunc(sysdate) - 1
             GROUP BY a.ia_id) i
    ON w.iaid = i.ia_id
GROUP BY w.group_id
UNION
SELECT
  w.group_id,
  sum(a.新增好友),
  sum(b.总计好友),
  sum(c.群个数),
  sum(d.新增意向客户数),
  sum(e.意向客户数),
  sum(i.新增开户),
  sum(h.新增出单),
  sum(h.新增出单金额),
  sum(j.累计出单),
  sum(j.累计出单金额),
  sum(j.累计单均净值),
  sum(j.累计出单) / sum(b.总计好友) 资源转化率
FROM
  (SELECT DISTINCT
     a.iaid,
     a.weid ia_alias,
     b.name,
     b.group_id
   FROM silver_consult.tb_crm_wechatinfo@consult_std a JOIN silver_consult.tb_crm_ia@consul_std b ON a.iaid = b.id
   WHERE b.group_id IN (112)) w
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 新增好友
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'W%' OR wechat_proto_comment LIKE 'w%') AND type = 1 AND
         trunc(create_time) = trunc(sysdate) - 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) a
    ON w.ia_alias = a.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 总计好友
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'W%' OR wechat_proto_comment LIKE 'w%') AND type = 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) b
    ON w.ia_alias = b.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 群个数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE wechat_nick_name LIKE '%网易%交流群%' AND type = 3 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) c
    ON w.ia_alias = c.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 新增意向客户数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'W%意向%' OR wechat_proto_comment LIKE 'w%意向%') AND type = 1 AND
         trunc(create_time) = trunc(sysdate) - 1 AND ia_alias IS NOT NULL
   GROUP BY ia_alias) d
    ON w.ia_alias = d.ia_alias
  LEFT JOIN
  (SELECT
     ia_alias,
     count(1) 意向客户数
   FROM silver_consult.tb_crm_user_wechat_info@consult_std
   WHERE (wechat_proto_comment LIKE 'W%意向%' OR wechat_proto_comment LIKE 'w%意向%') AND type = 1
   GROUP BY ia_alias) e
    ON w.ia_alias = e.ia_alias
  LEFT JOIN
  (SELECT
     fia_id,
     count(1)                                       新增出单,
     sum(PMEC_NET_VALUE_SUB) + sum(PMEC_NET_IN_SUB) 新增出单金额
   FROM silver_consult.tb_crm_transfer_record@consul_std
   WHERE trunc(submit_time) = trunc(sysdate) - 1 AND process IN (5, 6)
   GROUP BY fia_id) h
    ON w.iaid = h.fia_id
  LEFT JOIN
  (SELECT
     fia_id,
     count(1)                                       累计出单,
     sum(PMEC_NET_VALUE_SUB) + sum(PMEC_NET_IN_SUB) 累计出单金额,
     avg(PMEC_NET_VALUE_SUB) + avg(PMEC_NET_IN_SUB) 累计单均净值
   FROM silver_consult.tb_crm_transfer_record@consul_std
   WHERE trunc(submit_time) <= trunc(sysdate) - 1 AND process IN (5, 6)
   GROUP BY fia_id) j
    ON w.iaid = j.fia_id
  LEFT JOIN (SELECT
               a.ia_id,
               count(1) 新增开户
             FROM silver_consult.v_tb_crm_user@consul_std a JOIN tb_silver_user_stat@silver_std b ON a.fa_id = b.user_id
             WHERE trunc(b.open_account_time) = trunc(sysdate) - 1
             GROUP BY a.ia_id) i
    ON w.iaid = i.ia_id
GROUP BY w.group_id;


