package pgsql

const ActSessSQL = `select
	to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,
	pid,
	coalesce(datname, '') as db,
	coalesce(usename, '') as user,
	coalesce(application_name, '') as application_name,
	coalesce(backend_type, '') as backend_type,
	coalesce(host(client_addr), '') as client,
	coalesce(state, '') as state,
	coalesce(wait_event_type, '') as wait_event_type,
	coalesce(wait_event, '') as wait_event,
	coalesce(round(extract(epoch FROM (now()-query_start))::numeric,1), 0) as duration_ses,
	coalesce(to_char(query_start,'yyyy-mm-dd hh24:mi:ss'), '') as query_start,
	coalesce(query, '') as sql_text
from pg_stat_activity
where state<>'idle'
order by duration_ses desc`

const TxnSQL = `select
	to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,
	pid,
	coalesce(datname, '') as db,
	coalesce(usename, '') as user,
	coalesce(application_name, '') as application_name,
	coalesce(backend_type, '') as backend_type,
	coalesce(host(client_addr), '') as client,
	coalesce(state, '') as state,
	coalesce(wait_event_type, '') as wait_event_type,
	coalesce(wait_event, '') as wait_event,
	coalesce(round(extract(epoch from (now()-xact_start))::numeric,1), 0) as txn_exec_time,
	coalesce(round(extract(epoch from (now()-query_start))::numeric,1), 0) as exec_time,
	coalesce(to_char(xact_start,'yyyy-mm-dd hh24:mi:ss'), '') as txn_start,
	coalesce(to_char(query_start,'yyyy-mm-dd hh24:mi:ss'), '') as query_start,
	coalesce(query, '') as sql_text
from pg_stat_activity 
where state in ('active', 'idle in transaction') and xact_start is not null
order by xact_start`

const LockSQL = `with lck as ( 
	SELECT pid,COUNT(*) AS lock_count,sum(CASE WHEN GRANTED = 'f' THEN 1 else 0 end) as wait_lock_count,ARRAY_AGG(DISTINCT locktype) AS lock_types 
	FROM pg_locks GROUP BY pid)
SELECT 
	to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,
	lck.pid,
	coalesce(pg_blocking_pids(lck.pid), '{}') as blocking_pid,
	coalesce(psa.datname, '') as db,
	coalesce(psa.application_name, '') as application_name,
	coalesce(to_char(LEAST (query_start, xact_start),'yyyy-mm-dd hh24:mi:ss'), '') as start_time,
	coalesce(psa.STATE, '') as state,
	coalesce(round(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - LEAST (query_start, xact_start)))::numeric, 2), 0) AS txn_runtime,
	COALESCE(lck.lock_count, 0) AS lock_count,
	COALESCE(lck.wait_lock_count,0) as wait_lock_count,
	COALESCE(lck.lock_types, '{}') AS lock_types,
	coalesce(psa.query, '') as sqltext
FROM pg_stat_activity psa
JOIN lck ON psa.pid = lck.pid
WHERE psa.state <> 'idle'
ORDER BY xact_start`

const UserSessCountSQL = `select to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,coalesce(datname, '') as db,coalesce(usename, '') as user,count(*) cnt from pg_stat_activity group by datname,usename order by cnt desc`

const AppSessCountSQL = `select to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,coalesce(datname, '') as db,coalesce(application_name, '') as application_name,count(*) cnt from pg_stat_activity group by datname,application_name order by cnt desc`

const ClientSessCountSQL = `select to_char(now(), 'yyyy-mm-dd hh24:mi:ss') create_time,coalesce(datname, '') as db,coalesce(host(client_addr), '') as client,count(*) cnt from pg_stat_activity group by datname,client_addr order by cnt desc`
