package mysql

const ActSessSQL = `select now() create_time, id session_id,user,ifnull(db,'') db,substring_index(host,':',1) client,time exec_sec,command,state,ifnull(info,'') sql_text
	 from information_schema.processlist
	where id <> connection_id() and user not in ('system user','event_scheduler','replicator','aurora')
	  and command not in ( 'sleep','Binlog Dump','Binlog Dump GTID') order by exec_sec desc`

const TxnSQL = `select
	now() create_time,
	trx_mysql_thread_id session_id,
	b.user,
	ifnull(b.db,'') db,
	substring_index(b.host,':',1) client,
	b.command command,
	b.state state,
	b.time exec_sec,
	trx_id txn_id,
	ifnull(trx_state,'') txn_state,
	ifnull(trx_operation_state,'') txn_operation_state,
	trx_started txn_start_time,
	timestampdiff(second,trx_started,now()) txn_exec_sec,
	ifnull(timestampdiff(second, trx_wait_started, now()), 0) txn_wait_sec,
	trx_tables_locked tables_locked,
	trx_rows_locked rows_locked,
	trx_rows_modified rows_modified,
	trx_isolation_level isolation_level,
	ifnull(trx_query,'') sql_text
from
	information_schema.innodb_trx a left join information_schema.processlist b on b.id = a.trx_mysql_thread_id
order by
	txn_exec_sec desc`

const UserSessCountSQL = `select now() create_time,ifnull(user,'') user,count(*) cnt from information_schema.processlist group by user order by count(*) desc limit 100`

const DBSessCountSQL = `select now() create_time,ifnull(db,'') db,count(*) cnt from information_schema.processlist group by db order by count(*) desc limit 100`

const ClientSessCountSQL = `select now() create_time,substring_index(host, ':', 1) client,count(*) cnt from information_schema.processlist group by client order by count(*) desc limit 100;`
