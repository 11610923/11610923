import acuity_de_batchingservice.commons.CONSTANTS as CONSTANTS
import acuity_de_batchingservice.commons.connect_pg as pg
from datetime import datetime

def check_exception(meta_rec: dict, v_json_msg: dict, max_rec: dict):
    ret_val=[CONSTANTS.BATCH_TXN_EXCP_RSN_NE, '']
    new_wndw_bgn_dt = max_rec[0]['new_wndw_bgn_dt']
    new_wndw_end_dt = max_rec[0]['new_wndw_end_dt']
    v_batch_txn_data_extract_dt = v_json_msg['extracttimestamp']
    v_tenant_nm=v_json_msg['tenantNm']
    v_table_nm=v_json_msg['tableName']
    v_meta_schema = CONSTANTS.GIA_METADATA_SCHEMA_PREFIX + v_tenant_nm 
    v_btd =  v_meta_schema + '.' + CONSTANTS.BATCH_TXN_DTL_TABLE
    
    if meta_rec['batch_config_excp_chk_ind'] == 'N':
        return ret_val

    # Compare and Validate the Extract Timestamp from the Source JSON message
    # Valid Window
    if new_wndw_bgn_dt <= datetime.strptime(v_batch_txn_data_extract_dt, CONSTANTS.EXTRACT_DTTM_FORMAT) <= new_wndw_end_dt:

        # Duplicate file check for the current valid open window
        btd_query = '''select btd.batch_config_dtl_sk from {} btd where btd.batch_txn_sk = {}
                            '''.format(v_btd, max_rec[0]['batch_txn_sk'])
        btd_dict = pg.connect_pg.commit_pg_txn(btd_query)
        for btd_rec in btd_dict:
            if meta_rec['batch_config_dtl_sk'] == btd_rec['batch_config_dtl_sk'] and max_rec[0]['batch_txn_sts_sk'] == CONSTANTS.BATCH_TXN_STS_OPEN:
                ret_val=[CONSTANTS.BATCH_TXN_EXCP_RSN_DUP, '''This is a duplicate file/JSON as per the current batch of batch_txn_sk = {}, hence rejecting the file'''.format(max_rec[0]['batch_txn_sk'])]
        return ret_val
    # Previous Window
    elif new_wndw_bgn_dt > datetime.strptime(v_batch_txn_data_extract_dt, CONSTANTS.EXTRACT_DTTM_FORMAT):
        ret_val=[CONSTANTS.BATCH_TXN_EXCP_RSN_PREV_WIN, '''File extract date {} is older than the expected window <{} & {}>, as per the batch batch_txn_sk = {}, which means the file belongs to previous Batch, hence rejecting the message for source Table: {}'''.format(v_batch_txn_data_extract_dt, new_wndw_bgn_dt, new_wndw_end_dt, max_rec[0]['batch_txn_sk'], v_table_nm)]
        return ret_val
    # Future Window
    elif new_wndw_end_dt < datetime.strptime(v_batch_txn_data_extract_dt, CONSTANTS.EXTRACT_DTTM_FORMAT):
        ret_val=[CONSTANTS.BATCH_TXN_EXCP_RSN_FUT_WIN, '''File extract date {} is greater than the expected window <{} & {}>, as per the batch batch_txn_sk = {}, which means it belongs to a future Batch, so pushing back to the queue, to be processed in the correct order'''.format(v_batch_txn_data_extract_dt,new_wndw_bgn_dt, new_wndw_end_dt, max_rec[0]['batch_txn_sk'])]
        return ret_val
    return ret_val