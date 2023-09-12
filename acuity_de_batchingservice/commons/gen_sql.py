import acuity_de_batchingservice.commons.CONSTANTS as CONSTANTS

########################################
##              MONITOR               ##
########################################  

open_sql = f'''SELECT bt.batch_txn_sk, bcd.batch_config_sk, bt.batch_txn_sts_sk, bt.batch_txn_data_extract_dt, bt.batch_txn_wndw_bgn_dt, bt.batch_txn_wndw_end_dt,bt.batch_txn_expct_file_cnt, bt.batch_txn_recv_file_cnt
,bcd.batch_config_dtl_sk, bcd.batch_config_dtl_src_file_nm 
,bc.batch_config_nm, bc.batch_config_trgt_app, bc.batch_config_trgt_obj_nm, bc.batch_config_trgt_scrpt
,(coalesce(btd.batch_config_dtl_sk, {CONSTANTS.INVALID_VALUE}) = bcd.batch_config_dtl_sk) as file_rcvd
		from gia_metadata_cmn.batch_config_dtl bcd
		left join (select * from gia_metadata_cmn.batch_txn_dtl where batch_txn_sk in (select batch_txn_sk from gia_metadata_cmn.batch_txn where batch_txn_sts_sk = {CONSTANTS.BATCH_TXN_STS_OPEN}) )btd
		on bcd.batch_config_sk = btd.batch_config_sk
        and bcd.batch_config_dtl_sk = btd.batch_config_dtl_sk
		left join gia_metadata_cmn.batch_txn bt
 		on btd.batch_txn_sk = bt.batch_txn_sk
		left join gia_metadata_cmn.batch_config bc
		on bcd.batch_config_sk = bc.batch_config_sk
where bcd.batch_config_sk in (select batch_config_sk from gia_metadata_cmn.batch_txn where batch_txn_sts_sk = {CONSTANTS.BATCH_TXN_STS_OPEN})'''

no_file_sql = f'''select bc.batch_config_sk, batch_config_nm, batch_config_trgt_app, batch_config_trgt_obj_nm, batch_config_trgt_scrpt, batch_config_alert_email_list, batch_txn_data_extract_dt, sla from 
	(
	SELECT bt.batch_config_sk, batch_txn_data_extract_dt, batch_txn_wndw_bgn_dt, batch_txn_wndw_end_dt, batch_txn_wndw_end_dt + interval '1 hour' * batch_txn_max_wait_hours as sla, batch_txn_max_wait_hours, 
	RANK() OVER ( PARTITION BY bt.batch_config_sk ORDER BY batch_txn_sk desc) myr
		FROM gia_metadata_cmn.batch_txn bt	
	) btt	
	inner join gia_metadata_cmn.batch_config bc
	on btt.batch_config_sk = bc.batch_config_sk
	and bc.batch_config_sts_sk = {CONSTANTS.CONFIG_STS_ACTIVE}
	and batch_config_excp_chk_ind = 'Y'
	where btt.myr = 1
	and sla < current_timestamp - interval '65 day'; '''

serv_sql = 'select inet_server_addr();'
