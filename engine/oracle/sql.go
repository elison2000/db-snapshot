package oracle

const LongOpsSQL = `select 
       to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') "create_time",
       sid "sid",
       serial# "serial",
       username "username",
       sql_id "sql_id",
       time_remaining "time_remaining",
       elapsed_seconds "elapsed_seconds",
       round(sofar/totalwork*100) "completed_pct",
       opname "op_name",
       target "target",
       target_desc "target_desc",
       sofar "so_far",
       totalwork "total_work",
       units "units",
       to_char(start_time,'yyyy-mm-dd hh24:mi:ss') "start_time",
       to_char(last_update_time,'yyyy-mm-dd hh24:mi:ss') "last_update_time"
from v$session_longops 
where time_remaining > 0 
order by time_remaining desc`

const ActSessSQL = `SELECT /*+ OPT_PARAM('_optimizer_adaptive_plans','false') NO_MONITOR */
    to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') "create_time",
    s.sid "sid",
    s.serial# "serial",
    s.username "username",
    s.program "program",
    s.machine "machine",
    s.sql_id "sql_id",
    s.prev_sql_id "prev_sql_id",
    s.last_call_et "exec_sec",
    s.blocking_session "blocking_session",
    s.final_blocking_session "final_blocking_session",
    s.event "event",
    s.wait_class "wait_class",
    s.state "state",
    CASE 
        WHEN s.state = 'WAITING' THEN s.seconds_in_wait
        ELSE 0 
    END "wait_sec",
    s.p1 "p1",
    s.p2 "p2",
    s.p3 "p3"
FROM v$session s
WHERE s.status = 'ACTIVE'
    AND s.type = 'USER'
    AND s.username IS NOT NULL
    AND s.sql_id IS NOT NULL
    AND s.program not like '%(MS0%)'
    AND s.program not like '%(J0%)'
ORDER BY s.last_call_et DESC`

const TxnSQL = `select 
       to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') "create_time",
       s.sid "sid",
       s.username "username",
       s.machine "machine",
       s.program "program",
       s.status "status",
       decode(s.command, 3, 'select', 2, 'insert', 6, 'update', 7, 'delete', 'other') "command_type",
       s.sql_id "sql_id",
       s.prev_sql_id "prev_sql_id",
       s.wait_class "wait_class",
       s.event "event",
       s.blocking_session "blocking_session",
       s.last_call_et "exec_sec",
       xidusn || '.' || xidslot || '.' || xidsqn "xid",
       t.status "txn_status",
       to_char(t.start_date,'yyyy-mm-dd hh24:mi:ss') "txn_start_time",
       round((sysdate - t.start_date)*3600*24) "txn_exec_sec",
       t.cr_get "cr_get",
       t.phy_io "phy_io",
       t.used_ublk "used_blocks",
       t.used_urec "undo_rows"
from v$transaction t 
left join v$session s on s.taddr = t.addr
order by start_date`

const BlockerSQL = `with blocker as (
    select /*+ materialize */ distinct final_blocking_session as sid
    from v$session
)
SELECT /*+ LEADING(b s) USE_NL(s) NO_MERGE(b) */ 
    to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') "create_time",
    s.sid "sid",
    s.serial# "serial",
    s.username "username",
    s.machine "machine",
    s.program "program",
    decode(s.command, 3, 'select', 2, 'insert', 6, 'update', 7, 'delete', 'other') "command_type",
    s.sql_id "sql_id",
    s.prev_sql_id "prev_sql_id",
    s.status "status",
    s.state "state",
    s.wait_class "wait_class",
    s.event "event",
    to_char(s.logon_time, 'yyyy-mm-dd hh24:mi:ss') "logon_time",
    CASE
        WHEN s.state = 'WAITING' THEN s.seconds_in_wait
        WHEN s.state IN ('WAITED SHORT TIME', 'WAIT UNKNOW TIME') THEN NULL
        WHEN s.state = 'WAITING KNOWN TIME' THEN s.wait_time
        ELSE s.seconds_in_wait
    END "wait_sec",
    s.last_call_et "exec_sec",
    s.blocking_session "blocking_session",
    s.final_blocking_session "final_blocking_session",
    s.p1 "p1",
    s.p2 "p2",
    s.p3 "p3"
FROM blocker b, v$session s
WHERE s.sid = b.sid 
ORDER BY s.last_call_et desc`

const UserSessCountSQL = `select 
       to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') "create_time",
       username "username",
       count(*) "cnt"
from v$session 
WHERE TYPE<>'BACKGROUND' 
group by username 
order by 3 desc`

const ClientSessCountSQL = `select * from (
    select 
        to_char(sysdate,'yyyy-mm-dd hh24:mi:ss') "create_time",
        machine "client",
        count(*) "cnt"
    from v$session 
    WHERE TYPE<>'BACKGROUND' 
    group by machine 
    order by 3 desc
) where rownum<=100`

const SQLInfoSQL = `select /*+ LEADING(t) USE_NL(s) NO_MERGE(t) PUSH_PRED(s) */ 
       s.sql_id "sql_id",
       to_char(s.last_active_time,'yyyy-mm-dd hh24:mi:ss') "last_active_time",
       s.executions "executions",
       round(s.elapsed_time/1000000,2) "exec_sec",
       case when s.executions<>0 then round(s.elapsed_time/s.executions/1000000,2) end "avg_exec_sec",
       substr(s.sql_text,1,2000) "sql_text"
from (SELECT column_value as sql_id FROM TABLE(sys.odcivarchar2list(%s))) t
JOIN v$sqlstats s ON s.sql_id = t.sql_id`
