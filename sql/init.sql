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
    `act_sess_count`    int DEFAULT NULL COMMENT '活动连接数',
    `sess_count`        int DEFAULT NULL COMMENT '连接数',
    `big_query_count`   int DEFAULT NULL COMMENT '大查询个数',
    `wait_sess_count`   int DEFAULT NULL COMMENT '等待连接数',
    `lock_count`        int DEFAULT NULL COMMENT '行锁数',
    `max_query_seconds` int DEFAULT NULL COMMENT '最长查询耗时(s)',
    `max_txn_seconds`   int DEFAULT NULL COMMENT '最长事务耗时(s)',
    `duration_seconds`  int DEFAULT NULL COMMENT '采集快照耗时(s)',
    `msg`               text COMMENT '报错信息',
    PRIMARY KEY (`inst_id`, `create_time`),
    KEY                 `create_time` (`create_time`),
    KEY                 `idx_msg` (`msg`(32))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='db快照汇总'
PARTITION BY RANGE  COLUMNS(create_time)
(PARTITION p202501 VALUES LESS THAN ('2025-02-01') ENGINE = InnoDB,
 PARTITION p202502 VALUES LESS THAN ('2025-03-01') ENGINE = InnoDB,
 PARTITION p202503 VALUES LESS THAN ('2025-04-01') ENGINE = InnoDB,
 PARTITION p202504 VALUES LESS THAN ('2025-05-01') ENGINE = InnoDB,
 PARTITION p202505 VALUES LESS THAN ('2025-06-01') ENGINE = InnoDB,
 PARTITION p202506 VALUES LESS THAN ('2025-07-01') ENGINE = InnoDB,
 PARTITION p202507 VALUES LESS THAN ('2025-08-01') ENGINE = InnoDB,
 PARTITION p202508 VALUES LESS THAN ('2025-09-01') ENGINE = InnoDB,
 PARTITION p202509 VALUES LESS THAN ('2025-10-01') ENGINE = InnoDB,
 PARTITION p202510 VALUES LESS THAN ('2025-11-01') ENGINE = InnoDB,
 PARTITION p202511 VALUES LESS THAN ('2025-12-01') ENGINE = InnoDB,
 PARTITION p202512 VALUES LESS THAN ('2026-01-01') ENGINE = InnoDB,
 PARTITION pmax VALUES LESS THAN (MAXVALUE) );

