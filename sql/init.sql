CREATE TABLE `db_snapshot_config`
(
    `inst_id` int unsigned NOT NULL AUTO_INCREMENT COMMENT '实例ID',
    `db_type` varchar(30)  NOT NULL DEFAULT '' COMMENT '实例类型：mysql/mongo/redis/pgsql/mssql/tidb/doris',
    `host`    varchar(120) NOT NULL DEFAULT '' COMMENT '实例IP',
    `port`    int          NOT NULL DEFAULT '0' COMMENT '实例端口',
    `db_name` varchar(120) NOT NULL DEFAULT '' COMMENT '数据库名',
    PRIMARY KEY (`inst_id`),
    UNIQUE KEY `uk_ip_port` (`host`,`port`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci  COMMENT='db快照配置';


CREATE TABLE `db_snapshot`
(
    `inst_id`           bigint   NOT NULL COMMENT '实例ID',
    `create_time`       datetime NOT NULL COMMENT '快照创建时间',
    `txn_count`         int DEFAULT NULL COMMENT '事务数',
    `act_sess_count`    int DEFAULT NULL COMMENT '活动会话数',
    `sess_count`        int DEFAULT NULL COMMENT '总会话数',
    `long_query_count`   int DEFAULT NULL COMMENT '长查询个数',
    `max_query_seconds` int DEFAULT NULL COMMENT '最长查询耗时(s)',
    `long_txn_count`    int DEFAULT NULL COMMENT '长事务个数',
    `max_txn_seconds`   int DEFAULT NULL COMMENT '最长事务耗时(s)',
    `blocked_sess_count`   int DEFAULT NULL COMMENT '被阻塞的会话数',
    `duration_seconds`  int DEFAULT NULL COMMENT '采集快照耗时(s)',
    `msg`               text COMMENT '报错信息',
    PRIMARY KEY (`inst_id`, `create_time`),
    KEY                 `create_time` (`create_time`),
    KEY                 `idx_msg` (`msg`(32))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='db快照汇总'
PARTITION BY RANGE  COLUMNS(create_time)
(PARTITION p202601 VALUES LESS THAN ('2026-02-01') ENGINE = InnoDB,
 PARTITION p202602 VALUES LESS THAN ('2026-03-01') ENGINE = InnoDB,
 PARTITION p202603 VALUES LESS THAN ('2026-04-01') ENGINE = InnoDB,
 PARTITION p202604 VALUES LESS THAN ('2026-05-01') ENGINE = InnoDB,
 PARTITION p202605 VALUES LESS THAN ('2026-06-01') ENGINE = InnoDB,
 PARTITION p202606 VALUES LESS THAN ('2026-07-01') ENGINE = InnoDB,
 PARTITION p202607 VALUES LESS THAN ('2026-08-01') ENGINE = InnoDB,
 PARTITION p202608 VALUES LESS THAN ('2026-09-01') ENGINE = InnoDB,
 PARTITION p202609 VALUES LESS THAN ('2026-10-01') ENGINE = InnoDB,
 PARTITION p202610 VALUES LESS THAN ('2026-11-01') ENGINE = InnoDB,
 PARTITION p202611 VALUES LESS THAN ('2026-12-01') ENGINE = InnoDB,
 PARTITION p202612 VALUES LESS THAN ('2027-01-01') ENGINE = InnoDB,
 PARTITION pmax VALUES LESS THAN (MAXVALUE) );

