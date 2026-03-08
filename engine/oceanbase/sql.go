package oceanbase

const ActSessSQL = `select curtime() create_time, svr_ip, id session_id, user, db, user_client_ip client, tenant, round(time,3) exec_sec, command, state, trans_id txn_id, info sql_text FROM oceanbase.gv$ob_processlist where state<>'SLEEP' order by exec_sec desc`

const TxnSQL = `with b as (select trans_id,min(ctx_create_time) ctx_create_time from oceanbase.__all_virtual_trans_stat group by trans_id)
select now() create_time, svr_ip, id session_id, user, db, user_client_ip client, tenant, round(time,3) exec_sec, date_format(ctx_create_time,'%Y-%m-%d %H:%i:%s') txn_start_time, ifnull(timestampdiff(second,b.ctx_create_time,now()),0) txn_exec_sec,command, a.state, a.trans_id txn_id, info sql_text 
FROM oceanbase.gv$ob_processlist a join b on a.trans_id=b.trans_id order by txn_exec_sec desc`

const LockSQL = `with t as (
select a.id1 blocking_txn,a.trans_id blocked_txn,b.id1 from oceanbase.gv$ob_locks a join oceanbase.gv$ob_locks b on a.trans_id=b.trans_id and a.block=1 and a.type='TX' and b.block=1 and b.type='TR')
select bt.session_id blocking_sess_id,bt.tx_id blocking_txn_id,bt.ctx_create_time blocking_create_time,timestampdiff(second,bt.ctx_create_time,now()) blocking_exec_sec,bt.last_request_time blocking_last_req_at,wt.session_id blocked_sess_id,wt.tx_id blocked_txn_id,wt.ctx_create_time blocked_create_time,timestampdiff(second,wt.ctx_create_time,now()) blocked_exec_sec,wt.last_request_time blocked_last_req_at
from t left join oceanbase.gv$ob_transaction_participants bt on bt.tx_id=t.blocking_txn left join oceanbase.gv$ob_transaction_participants wt on wt.tx_id=t.blocked_txn`

const LockObjSQL = `select svr_ip,svr_port,tenant_id,trans_id txn_id,id1,id2,type,lmode lock_mode,block,round(ctime/1000000) lock_sec from oceanbase.gv$ob_locks where block=1 order by ctime desc`

const SessCountSQL = `select now() create_time,user,db,count(*) cnt from oceanbase.gv$ob_processlist group by user,db order by count(*) desc limit 100`
