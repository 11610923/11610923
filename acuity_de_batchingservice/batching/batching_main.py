from datetime import datetime
import json
import sys
import time

sys.path.insert(0, r'/batching_code')
import acuity_de_batchingservice.commons.log4j_logger as log4j_logger
#import acuity_de_batchingservice.commons.json_validator as validate_json
import acuity_de_batchingservice.commons.consume_batch as cb
#import acuity_de_batchingservice.commons.gen_dml_strings as gen_dml
import acuity_de_batchingservice.commons.connect_pg as pg
import acuity_de_batchingservice.commons.CONSTANTS as CONSTANTS
import acuity_de_batchingservice.commons.trigger_workflow as trigger_wf
import acuity_de_batchingservice.commons.chk_excp as chk_excp
from acuity_de_batchingservice.commons.publish_sqs import publish_sqs as publish_sqs 

my_debug = False


def batching_main():
    log = log4j_logger.Log4j
    i=1
    while True: 
        try:
            time.sleep(CONSTANTS.CONSUME_SLEEPTIME)
            json_msg_array = cb.consume_batch.consume_sqs(CONSTANTS.BATCHING_QUEUE_NAME)

            if json_msg_array:
                v_json_msg = json.loads(json_msg_array[0]['Body'])
                print(v_json_msg)
                
                v_table_nm=v_json_msg['tableName']
                v_batch_txn_data_extract_dt=v_json_msg['extracttimestamp']
                v_tenant_nm=v_json_msg['tenantNm']
                v_meta_schema = CONSTANTS.GIA_METADATA_SCHEMA_PREFIX + v_tenant_nm 
                v_bc =  v_meta_schema + '.' + CONSTANTS.BATCH_CONFIG_TABLE
                v_bcd =  v_meta_schema + '.' + CONSTANTS.BATCH_CONFIG_DTL_TABLE
                v_bt =  v_meta_schema + '.' + CONSTANTS.BATCH_TXN_TABLE
                v_btd =  v_meta_schema + '.' + CONSTANTS.BATCH_TXN_DTL_TABLE

                meta_query = '''select bcd.batch_config_dtl_sk, bcd.batch_config_dtl_src_file_nm, bc.batch_config_sk, bc.tenant_cd, bc.batch_config_nm, bc.batch_config_desc,
                        bc.batch_config_cnt, bc.batch_config_sngl_file_ind, bc.batch_config_freq_hour, bc.batch_config_sla_hour, bc.lob_sk, bc.batch_config_sts_sk,
                        bc.batch_config_load_prcss_nm_sk, bc.batch_config_trgt_app, bc.batch_config_trgt_obj_nm, bc.batch_config_trgt_scrpt, bc.batch_config_alert_email_list, bc.batch_config_excp_chk_ind
                FROM {} bcd
                join {} bc
                on bcd.batch_config_sk = bc.batch_config_sk
                where bcd.batch_config_dtl_src_file_nm = '{}'
                and bc.batch_config_sts_sk = {} '''.format(v_bcd, v_bc, v_table_nm,CONSTANTS.CONFIG_STS_ACTIVE)

                # Metadata lookup using the fetch_metadata function
                meta_dict = pg.connect_pg.commit_pg_txn(meta_query)
            

                if len(meta_dict) >0:
                    for meta_rec in meta_dict:
                        '''
                        meta_query to verify if an open batch transaction exists in batch_txn_dtl or batch_txn tables
                        for the given window
                        '''
                        if my_debug: print('------------Round - {}--------'.format(meta_dict.index(meta_rec)))

                        # Fetching max Batch transaction records for each config from metadata for the received source table 
                        max_batch_rec_query = '''select bt.batch_txn_sk, bt.batch_config_sk, bt.batch_txn_sts_sk, bt.batch_txn_wndw_bgn_dt, bt.batch_txn_wndw_end_dt, 
                                                bt.batch_txn_expct_file_cnt, bt.batch_txn_recv_file_cnt, bt.batch_txn_excp_rsn_sk, bc.batch_config_freq_hour,
                                                case when bt.batch_txn_sts_sk = {txn_sts_comp} then bt.batch_txn_wndw_end_dt + interval '1 sec' 
                                                    else bt.batch_txn_wndw_bgn_dt end as new_wndw_bgn_dt
                                                , case when bt.batch_txn_sts_sk = {txn_sts_comp} then bt.batch_txn_wndw_end_dt + interval '1 sec' * (bc.batch_config_freq_hour * 3600 )  
                                                else bt.batch_txn_wndw_end_dt end as new_wndw_end_dt
                                                from {} bt inner join {} bc on bt.batch_config_sk = bc.batch_config_sk where batch_txn_sk = 
                        (
                            select max(batch_txn_sk)
                            from {}
                            where batch_config_sk = {}
                            and batch_txn_sts_sk != {}
                        )'''.format(v_bt, v_bc, v_bt, meta_rec["batch_config_sk"], 
                                    CONSTANTS.BATCH_TXN_STS_FAIL, txn_sts_comp=CONSTANTS.BATCH_TXN_STS_COMPLETED)
                        max_batch_rec_list = pg.connect_pg.commit_pg_txn(max_batch_rec_query)
                        
                        if len(max_batch_rec_list) > 0:
                            if max_batch_rec_list[0]['batch_txn_sts_sk'] == CONSTANTS.BATCH_TXN_STS_COMPLETED:
                                # Checking the exception scenarios for this message
                                v_batch_txn_excp_list = chk_excp.check_exception(meta_rec, v_json_msg, max_batch_rec_list)
                                if len(v_batch_txn_excp_list) > 0:
                                    v_batch_txn_excp_rsn_sk = v_batch_txn_excp_list[0]
                                    v_batch_txn_excp_rsn_msg = v_batch_txn_excp_list[1]
                                else:
                                    err_msg = 'Error while checking the Exception process'
                                    print(err_msg)
                                    ### TODO Call Control Service
                                    ctrl_msg = publish_sqs.gen_msg(CONSTANTS.CONTROL, meta_rec, v_json_msg, CONSTANTS.TECH_ERROR, err_msg)
                                    ctrl_resp = publish_sqs.pub_sqs(CONSTANTS.CONTROL_QUEUE_NAME, ctrl_msg)
                                    if ctrl_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                                        raise Exception('Error while sending message to Control Service queue:{}'.format(CONSTANTS.CONTROL_QUEUE_NAME))
                                if v_batch_txn_excp_rsn_sk in [CONSTANTS.BATCH_TXN_EXCP_RSN_PREV_WIN, CONSTANTS.BATCH_TXN_EXCP_RSN_DUP]:
                                    print(v_batch_txn_excp_rsn_msg)
                                    ins_batch_txn='''insert into {} 
                                                    ( batch_config_sk, batch_txn_sts_sk, batch_txn_data_extract_dt, batch_txn_wndw_bgn_dt, batch_txn_wndw_end_dt, 
                                                    batch_txn_expct_file_cnt, batch_txn_recv_file_cnt, batch_txn_excp_rsn_sk, batch_txn_trgr_sts_cd_sk, batch_txn_max_wait_hours)
                                                    select {}, {txn_sts}, '{}',  '{}', '{}',{}, 
                                                    1, {}, {}, {} 
                                                    '''.format(v_bt, meta_rec['batch_config_sk'], v_batch_txn_data_extract_dt, max_batch_rec_list[0]['new_wndw_bgn_dt'], 
                                                    max_batch_rec_list[0]['new_wndw_end_dt'], meta_rec['batch_config_cnt'], v_batch_txn_excp_rsn_sk, CONSTANTS.BATCH_TXN_TRGR_STS_FAIL,
                                                    meta_rec['batch_config_sla_hour'], txn_sts=CONSTANTS.BATCH_TXN_STS_FAIL)
                                    ins_batch_txn_dtl='''INSERT INTO {} ( batch_config_dtl_sk, batch_txn_sk, batch_config_sk, batch_txn_dtl_sts_sk, batch_txn_json_dtl, batch_txn_dtl_excp_rsn_sk)
                                                        select {}, max(batch_txn_sk), {config_sk}, {txn_sts_fail}, cast('{}' as json ), {} from {} where batch_config_sk = {config_sk} and 
                                                        batch_txn_sts_sk = {txn_sts_fail}'''.format(v_btd, meta_rec['batch_config_dtl_sk'], json.dumps(v_json_msg), v_batch_txn_excp_rsn_sk, 
                                                                                            v_bt, txn_sts_fail = CONSTANTS.BATCH_TXN_STS_FAIL , config_sk=meta_rec['batch_config_sk'] )
                                    pg.connect_pg.commit_pg_txn(ins_batch_txn)
                                    pg.connect_pg.commit_pg_txn(ins_batch_txn_dtl)
                                    ### TODO Call Control Service
                                    ctrl_msg = publish_sqs.gen_msg(CONSTANTS.CONTROL, meta_rec, v_json_msg, CONSTANTS.TECH_ERROR, v_batch_txn_excp_rsn_msg)
                                    ctrl_resp = publish_sqs.pub_sqs(CONSTANTS.CONTROL_QUEUE_NAME, ctrl_msg)
                                    if ctrl_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                                        raise Exception('Error while sending message to Control Service queue:{}'.format(CONSTANTS.CONTROL_QUEUE_NAME))
                                    continue
                                elif v_batch_txn_excp_rsn_sk == CONSTANTS.BATCH_TXN_EXCP_RSN_FUT_WIN:
                                    print(v_batch_txn_excp_rsn_msg)
                                    pub_batch_resp = publish_sqs.pub_sqs(CONSTANTS.BATCHING_QUEUE_NAME, v_json_msg)
                                    if pub_batch_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                                        pub_batch_resp_msg = 'Failed to send message back to queue to process in correct order, error_code:'.format(pub_batch_resp['ResponseMetadata']['HTTPStatusCode'])
                                        print(pub_batch_resp_msg)
                                        # TODO Call Control Service
                                        ctrl_msg = publish_sqs.gen_msg(CONSTANTS.CONTROL, meta_rec, v_json_msg, CONSTANTS.QUEUE_ERROR, pub_batch_resp_msg)
                                        ctrl_resp = publish_sqs.pub_sqs(CONSTANTS.CONTROL_QUEUE_NAME, ctrl_msg)
                                        if ctrl_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                                            raise Exception('Error while sending message to Control Service queue:{}'.format(CONSTANTS.CONTROL_QUEUE_NAME))
                                    continue
                                elif v_batch_txn_excp_rsn_sk == CONSTANTS.BATCH_TXN_EXCP_RSN_NE:
                                    # The max record for this config in the batch transaction table is in completion status and no exceptions
                                    # hence inserting new batch and batch transaction entries
                                    ins_batch_txn='''insert into {} 
                                                    ( batch_config_sk, batch_txn_sts_sk, batch_txn_data_extract_dt, batch_txn_wndw_bgn_dt, batch_txn_wndw_end_dt, 
                                                    batch_txn_expct_file_cnt, batch_txn_recv_file_cnt, batch_txn_excp_rsn_sk, batch_txn_trgr_sts_cd_sk, batch_txn_max_wait_hours)
                                                    select {}, {txn_sts_open}, '{}',  '{}', '{}',{}, 
                                                    1, {}, null, {} 
                                                    '''.format(v_bt, meta_rec['batch_config_sk'], v_batch_txn_data_extract_dt, 
                                                               max_batch_rec_list[0]['new_wndw_bgn_dt'], max_batch_rec_list[0]['new_wndw_end_dt'], 
                                                               meta_rec['batch_config_cnt'], v_batch_txn_excp_rsn_sk, meta_rec['batch_config_sla_hour']
                                                               ,txn_sts_open = CONSTANTS.BATCH_TXN_STS_OPEN)
                                    ins_batch_txn_dtl='''INSERT INTO {} (
                                                        batch_config_dtl_sk, batch_txn_sk, batch_config_sk, batch_txn_dtl_sts_sk, batch_txn_json_dtl, batch_txn_dtl_excp_rsn_sk)
                                                        select {},(select max(batch_txn_sk) from {} where batch_config_sk = {} and batch_txn_sts_sk = {txn_sts_open} ),
                                                        {}, {}, cast('{}' as json ), {} 
                                                        '''.format(v_btd, meta_rec['batch_config_dtl_sk'], v_bt, 
                                                                   meta_rec['batch_config_sk'], meta_rec['batch_config_sk'], CONSTANTS.BATCH_TXN_STS_COMPLETED
                                                                   , json.dumps(v_json_msg), v_batch_txn_excp_rsn_sk,txn_sts_open = CONSTANTS.BATCH_TXN_STS_OPEN )
                                if my_debug: print('____________________________________________________________________________________')
                                if my_debug: print('batch_txn insert query', ins_batch_txn)
                                if my_debug: print('____________________________________________________________________________________')
                                if my_debug: print('batch_txn_dtl insert query', ins_batch_txn_dtl)
                                if my_debug: print('____________________________________________________________________________________')
                                pg.connect_pg.commit_pg_txn(ins_batch_txn)
                                pg.connect_pg.commit_pg_txn(ins_batch_txn_dtl)
                                #print(ins_batch_txn_rec)
                                ver_batch_txn_sts_query = '''select * from {} where batch_txn_sk = (select max(batch_txn_sk) from {} where batch_config_sk = {} 
                                and batch_txn_sts_sk = {txn_sts_open} )
                                '''.format(v_bt, v_bt, meta_rec['batch_config_sk'],txn_sts_open = CONSTANTS.BATCH_TXN_STS_OPEN)
                                if my_debug: print('____________________________________________________________________________________')
                                if my_debug: print('batch verification query: ', ver_batch_txn_sts_query)
                                if my_debug: print('____________________________________________________________________________________')
                                ver_batch_txn_sts_rec_dict = pg.connect_pg.commit_pg_txn(ver_batch_txn_sts_query)
                                if my_debug: print('____________________________________________________________________________________')
                                if my_debug: print('Batch verification output dictionary', ver_batch_txn_sts_rec_dict)
                                if my_debug: print('____________________________________________________________________________________')

                                if ver_batch_txn_sts_rec_dict[0]['batch_txn_expct_file_cnt'] == ver_batch_txn_sts_rec_dict[0]['batch_txn_recv_file_cnt']:
                                    print('Expected number of files for this batch config is received. Trigger the target loads :', meta_rec['batch_config_trgt_scrpt'])
                                    # TODO Add step to verify the status of Target DBX workflow
                                    trigger_res = trigger_wf.trigger_workflow.triggerJob(CONSTANTS.DATABRICKS_URL, CONSTANTS.DATABRICKS_AT, meta_rec['batch_config_trgt_scrpt'])
                                    print("Databricks workflow trigger response :", trigger_res)
                                    if CONSTANTS.DATABRICKS_SUCC in str(trigger_res):
                                        print('Sucessfully triggerred the workflow:', meta_rec['batch_config_trgt_scrpt'])
                                        mark_batch_txn_completed_query='''update {} 
                                                        set batch_txn_sts_sk = {txn_sts_comp},
                                                        batch_txn_trgr_sts_cd_sk = {trgr_sts_succ},
                                                        batch_txn_excp_rsn_sk = {excp_rsn_ne},
                                                        upd_by_user_id = user,
                                                        upd_dttm = current_timestamp
                                                        where batch_txn_sk = {}
                                                        '''.format(v_bt, ver_batch_txn_sts_rec_dict[0]['batch_txn_sk'], 
                                                                txn_sts_comp=CONSTANTS.BATCH_TXN_STS_COMPLETED, 
                                                                trgr_sts_succ=CONSTANTS.BATCH_TXN_TRGR_STS_SUCC, excp_rsn_ne=CONSTANTS.BATCH_TXN_EXCP_RSN_NE)
                                        pg.connect_pg.commit_pg_txn(mark_batch_txn_completed_query)
                                    else:
                                        print('Failed to trigger the workflow:', meta_rec['batch_config_trgt_scrpt'], 'marking the batch as failed')
                                        mark_batch_txn_failed_query='''update {} 
                                                        set batch_txn_sts_sk = {txn_sts_fail},
                                                        batch_txn_trgr_sts_cd_sk = {trgr_sts_fail},
                                                        batch_txn_excp_rsn_sk = {excp_rsn_ne},
                                                        upd_by_user_id = user,
                                                        upd_dttm = current_timestamp
                                                        where batch_txn_sk = {}
                                                        '''.format(v_bt, ver_batch_txn_sts_rec_dict[0]['batch_txn_sk'], 
                                                                txn_sts_fail=CONSTANTS.BATCH_TXN_STS_FAIL, 
                                                                trgr_sts_fail=CONSTANTS.BATCH_TXN_TRGR_STS_FAIL, excp_rsn_ne=CONSTANTS.BATCH_TXN_EXCP_RSN_NE)
                                        pg.connect_pg.commit_pg_txn(mark_batch_txn_failed_query)

                                else:
                                    print('Expected number of files are not received yet, so this batch transaction will stay open')
                                
                            elif max_batch_rec_list[0]['batch_txn_sts_sk'] == CONSTANTS.BATCH_TXN_STS_OPEN:
                                #The lastest batch is still open so we need to insert only batch transaction details table with this new file 
                                #Update the existing batch transaction file count
                                            
                                # Checking the exception scenarios for this message
                                v_batch_txn_excp_list = chk_excp.check_exception(meta_rec, v_json_msg, max_batch_rec_list)
                                if len(v_batch_txn_excp_list) > 0:
                                    v_batch_txn_excp_rsn_sk = v_batch_txn_excp_list[0]
                                    v_batch_txn_excp_rsn_msg = v_batch_txn_excp_list[1]
                                else:
                                    err_msg = 'Error while checking the Exception process'
                                    print(err_msg)
                                    ### TODO Call Control Service
                                    ctrl_msg = publish_sqs.gen_msg(CONSTANTS.CONTROL, meta_rec, v_json_msg, CONSTANTS.QUEUE_ERROR, err_msg)
                                    ctrl_resp = publish_sqs.pub_sqs(CONSTANTS.CONTROL_QUEUE_NAME, ctrl_msg)
                                    if ctrl_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                                        raise Exception('Error while sending message to Control Service queue:{}'.format(CONSTANTS.CONTROL_QUEUE_NAME))
                                if v_batch_txn_excp_rsn_sk in [CONSTANTS.BATCH_TXN_EXCP_RSN_PREV_WIN, CONSTANTS.BATCH_TXN_EXCP_RSN_DUP]:
                                    print(v_batch_txn_excp_rsn_msg)
                                    ins_batch_txn='''insert into {} 
                                                    ( batch_config_sk, batch_txn_sts_sk, batch_txn_data_extract_dt, batch_txn_wndw_bgn_dt, batch_txn_wndw_end_dt, 
                                                    batch_txn_expct_file_cnt, batch_txn_recv_file_cnt, batch_txn_excp_rsn_sk, batch_txn_trgr_sts_cd_sk, batch_txn_max_wait_hours)
                                                    select {}, {txn_sts}, '{}',  '{}', '{}',{}, 
                                                    1, {}, {}, {} 
                                                    '''.format(v_bt, meta_rec['batch_config_sk'], v_batch_txn_data_extract_dt, max_batch_rec_list[0]['new_wndw_bgn_dt'], 
                                                    max_batch_rec_list[0]['new_wndw_end_dt'], meta_rec['batch_config_cnt'], v_batch_txn_excp_rsn_sk, CONSTANTS.BATCH_TXN_TRGR_STS_FAIL,
                                                    meta_rec['batch_config_sla_hour'], txn_sts=CONSTANTS.BATCH_TXN_STS_FAIL)
                                    ins_batch_txn_dtl='''INSERT INTO {} ( batch_config_dtl_sk, batch_txn_sk, batch_config_sk, batch_txn_dtl_sts_sk, batch_txn_json_dtl, batch_txn_dtl_excp_rsn_sk)
                                                        select {}, max(batch_txn_sk), {config_sk}, {txn_sts_fail}, cast('{}' as json ), {} from {} where batch_config_sk = {config_sk} and 
                                                        batch_txn_sts_sk = {txn_sts_fail}'''.format(v_btd, meta_rec['batch_config_dtl_sk'], json.dumps(v_json_msg), v_batch_txn_excp_rsn_sk, 
                                                                                            v_bt, txn_sts_fail = CONSTANTS.BATCH_TXN_STS_FAIL , config_sk=meta_rec['batch_config_sk'] )
                                    pg.connect_pg.commit_pg_txn(ins_batch_txn)
                                    pg.connect_pg.commit_pg_txn(ins_batch_txn_dtl)
                                    continue
                                elif v_batch_txn_excp_rsn_sk == CONSTANTS.BATCH_TXN_EXCP_RSN_FUT_WIN:
                                    print(v_batch_txn_excp_rsn_msg)
                                    pub_batch_resp = publish_sqs.pub_sqs(CONSTANTS.BATCHING_QUEUE_NAME, v_json_msg)                                   
                                    if pub_batch_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                                        pub_batch_resp_msg = 'Failed to send message back to queue to process in correct order, error_code:'.format(pub_batch_resp['ResponseMetadata']['HTTPStatusCode'])
                                        print(pub_batch_resp_msg)
                                        # TODO Call Control Service
                                        ctrl_msg = publish_sqs.gen_msg(CONSTANTS.CONTROL, meta_rec, v_json_msg, CONSTANTS.QUEUE_ERROR, pub_batch_resp_msg)
                                        ctrl_resp = publish_sqs.pub_sqs(CONSTANTS.CONTROL_QUEUE_NAME, ctrl_msg)
                                        if ctrl_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                                            raise Exception('Error while sending message to Control Service queue:{}'.format(CONSTANTS.CONTROL_QUEUE_NAME))
                                    continue
                                elif v_batch_txn_excp_rsn_sk == CONSTANTS.BATCH_TXN_EXCP_RSN_NE:
                                    # Add code to verify if this table entry is already exists in the this open batch
                                    # If yes it should raise exception as duplicate message
                                    ins_batch_txn_dtl='''INSERT INTO {} (
                                                    batch_config_dtl_sk, batch_txn_sk, batch_config_sk, batch_txn_dtl_sts_sk, batch_txn_json_dtl, batch_txn_dtl_excp_rsn_sk)
                                                    select {},{},{}, {}, cast('{}' as json ), {excp_rsn_ne} 
                                                    '''.format(v_btd, meta_rec['batch_config_dtl_sk'],
                                                    max_batch_rec_list[0]['batch_txn_sk'], meta_rec['batch_config_sk'], CONSTANTS.BATCH_TXN_STS_COMPLETED, 
                                                    json.dumps(v_json_msg), excp_rsn_ne=CONSTANTS.BATCH_TXN_EXCP_RSN_NE )
                                    upd_batch_txn = '''update {} 
                                                set batch_txn_recv_file_cnt = batch_txn_recv_file_cnt+1,
                                                upd_by_user_id = user,
                                                upd_dttm = current_timestamp
                                                where batch_txn_sk = {}
                                                '''.format(v_bt, max_batch_rec_list[0]['batch_txn_sk'])
                                if my_debug: print('____________________________________________________________________________________')
                                if my_debug: print('batch_txn update query', upd_batch_txn)
                                if my_debug: print('____________________________________________________________________________________')
                                if my_debug: print('batch_txn_dtl insert query', ins_batch_txn_dtl)
                                if my_debug: print('____________________________________________________________________________________')
                                
                                pg.connect_pg.commit_pg_txn(ins_batch_txn_dtl)
                                pg.connect_pg.commit_pg_txn(upd_batch_txn)

                                ver_batch_txn_sts_query = '''select * from {} where batch_txn_sk = {}'''.format(v_bt, max_batch_rec_list[0]['batch_txn_sk'])
                                if my_debug: print('____________________________________________________________________________________')
                                if my_debug: print('batch verification query: ', ver_batch_txn_sts_query)
                                if my_debug: print('____________________________________________________________________________________')
                                ver_batch_txn_sts_rec_dict = pg.connect_pg.commit_pg_txn(ver_batch_txn_sts_query)
                                if my_debug: print('____________________________________________________________________________________')
                                if my_debug: print('Batch verification output dictionary', ver_batch_txn_sts_rec_dict)
                                if my_debug: print('____________________________________________________________________________________')
                                if ver_batch_txn_sts_rec_dict[0]['batch_txn_expct_file_cnt'] == ver_batch_txn_sts_rec_dict[0]['batch_txn_recv_file_cnt']:
                                    print('Expected number of files for this batch config is received. Trigger the target loads :', meta_rec['batch_config_trgt_scrpt'])
                                    # TODO Add step to verify the status of Target DBX workflow
                                    trigger_res = trigger_wf.trigger_workflow.triggerJob(CONSTANTS.DATABRICKS_URL, CONSTANTS.DATABRICKS_AT, meta_rec['batch_config_trgt_scrpt'])
                                    print("Databricks workflow trigger response :", trigger_res)
                                    if CONSTANTS.DATABRICKS_SUCC in str(trigger_res):
                                        print('Sucessfully triggerred the workflow:', meta_rec['batch_config_trgt_scrpt'])
                                        mark_batch_txn_completed_query='''update {} 
                                                        set batch_txn_sts_sk = {txn_sts_comp},
                                                        batch_txn_trgr_sts_cd_sk = {trgr_sts_succ},
                                                        batch_txn_excp_rsn_sk = {excp_rsn_ne},
                                                        upd_by_user_id = user,
                                                        upd_dttm = current_timestamp
                                                        where batch_txn_sk = {}
                                                        '''.format(v_bt, ver_batch_txn_sts_rec_dict[0]['batch_txn_sk'], 
                                                                txn_sts_comp=CONSTANTS.BATCH_TXN_STS_COMPLETED, 
                                                                trgr_sts_succ=CONSTANTS.BATCH_TXN_TRGR_STS_SUCC, excp_rsn_ne=CONSTANTS.BATCH_TXN_EXCP_RSN_NE)
                                        pg.connect_pg.commit_pg_txn(mark_batch_txn_completed_query)
                                    else:
                                        print('Failed to trigger the workflow:', meta_rec['batch_config_trgt_scrpt'], 'marking the batch as failed')
                                        mark_batch_txn_failed_query='''update {} 
                                                        set batch_txn_sts_sk = {txn_sts_fail},
                                                        batch_txn_trgr_sts_cd_sk = {trgr_sts_fail},
                                                        batch_txn_excp_rsn_sk = {excp_rsn_ne},
                                                        upd_by_user_id = user,
                                                        upd_dttm = current_timestamp
                                                        where batch_txn_sk = {}
                                                        '''.format(v_bt, ver_batch_txn_sts_rec_dict[0]['batch_txn_sk'], 
                                                                txn_sts_fail=CONSTANTS.BATCH_TXN_STS_FAIL, 
                                                                trgr_sts_fail=CONSTANTS.BATCH_TXN_TRGR_STS_FAIL, excp_rsn_ne=CONSTANTS.BATCH_TXN_EXCP_RSN_NE)
                                        pg.connect_pg.commit_pg_txn(mark_batch_txn_failed_query)

                                else:
                                    print('Expected number of files are not received yet, so this batch transaction will stay open')

                            elif max_batch_rec_list[0]['batch_txn_sts_sk'] == CONSTANTS.BATCH_TXN_STS_FAIL:
                                print('Its a failed Batch Transaction: raise exception')
                            else:
                                print('Invalid Batch status')
                            

                        else:
                            print('''There are no existing batch transaction records for this Batchconfig for the source file : {}
                                    Proceeding with first entry to both batch_txn and batch_txn_dtl'''.format(v_table_nm))
                            
                            ins_batch_txn='''insert into {} 
                                                ( batch_config_sk, batch_txn_sts_sk, batch_txn_data_extract_dt, batch_txn_wndw_bgn_dt, batch_txn_wndw_end_dt, 
                                                batch_txn_expct_file_cnt, batch_txn_recv_file_cnt, batch_txn_excp_rsn_sk, batch_txn_trgr_sts_cd_sk, batch_txn_max_wait_hours)
                                                select {}, {txn_sts_open}, '{ext_dt}',  to_date('{ext_dt}', '{pg_tm_fmt}') , 
                                                to_date('{ext_dt}', '{pg_tm_fmt}') + interval '1 sec' * ({} * 3600 -1),{}, 1, null, null, {} 
                                                '''.format(v_bt, meta_rec['batch_config_sk'], meta_rec['batch_config_freq_hour'],
                                                           meta_rec['batch_config_cnt'], meta_rec['batch_config_sla_hour'],txn_sts_open = CONSTANTS.BATCH_TXN_STS_OPEN, 
                                                           ext_dt=v_batch_txn_data_extract_dt, pg_tm_fmt=CONSTANTS.EXTRACT_PG_DTTM_FORMAT)

                            ins_batch_txn_dtl='''INSERT INTO {} (
                                                    batch_config_dtl_sk, batch_txn_sk, batch_config_sk, batch_txn_dtl_sts_sk, batch_txn_json_dtl, batch_txn_dtl_excp_rsn_sk)
                                                    select {},(select max(batch_txn_sk) from {} where batch_config_sk = {} and batch_txn_sts_sk = {txn_sts_open} ),
                                                    {}, {}, cast('{}' as json ), {excp_rsn_ne} 
                                                    '''.format(v_btd, meta_rec['batch_config_dtl_sk'], v_bt, 
                                                               meta_rec['batch_config_sk'], meta_rec['batch_config_sk'], CONSTANTS.BATCH_TXN_STS_COMPLETED, 
                                                               json.dumps(v_json_msg), excp_rsn_ne=CONSTANTS.BATCH_TXN_EXCP_RSN_NE,txn_sts_open = CONSTANTS.BATCH_TXN_STS_OPEN)
                            if my_debug: print('____________________________________________________________________________________')
                            if my_debug: print('batch_txn insert query', ins_batch_txn)
                            if my_debug: print('____________________________________________________________________________________')
                            if my_debug: print('batch_txn_dtl insert query', ins_batch_txn_dtl)
                            if my_debug: print('____________________________________________________________________________________')
                            pg.connect_pg.commit_pg_txn(ins_batch_txn)
                            pg.connect_pg.commit_pg_txn(ins_batch_txn_dtl)
                            ver_batch_txn_sts_query = '''select * from {} where batch_txn_sk = (select max(batch_txn_sk) from {} where batch_config_sk = {} 
                            and batch_txn_sts_sk = {txn_sts_open} )
                                                    '''.format(v_bt, v_bt, meta_rec['batch_config_sk']
                                                               ,txn_sts_open = CONSTANTS.BATCH_TXN_STS_OPEN)
                            if my_debug: print('____________________________________________________________________________________')
                            if my_debug: print('batch verification query: ', ver_batch_txn_sts_query)
                            if my_debug: print('____________________________________________________________________________________')
                            ver_batch_txn_sts_rec_dict = pg.connect_pg.commit_pg_txn(ver_batch_txn_sts_query)
                            if my_debug: print('____________________________________________________________________________________')
                            if my_debug: print('Batch verification output dictionary', ver_batch_txn_sts_rec_dict)
                            if my_debug: print('____________________________________________________________________________________')

                            if ver_batch_txn_sts_rec_dict[0]['batch_txn_expct_file_cnt'] == ver_batch_txn_sts_rec_dict[0]['batch_txn_recv_file_cnt']:
                                print('Expected number of files for this batch config is received. Trigger the target loads :', meta_rec['batch_config_trgt_scrpt'])
                                # TODO Add step to verify the status of Target DBX workflow
                                trigger_res = trigger_wf.trigger_workflow.triggerJob(CONSTANTS.DATABRICKS_URL, CONSTANTS.DATABRICKS_AT, meta_rec['batch_config_trgt_scrpt'])
                                print("Databricks workflow trigger response :", trigger_res)
                                if CONSTANTS.DATABRICKS_SUCC in str(trigger_res):
                                    print('Sucessfully triggerred the workflow:', meta_rec['batch_config_trgt_scrpt'])
                                    mark_batch_txn_completed_query='''update {} 
                                                    set batch_txn_sts_sk = {txn_sts_comp},
                                                    batch_txn_trgr_sts_cd_sk = {trgr_sts_succ},
                                                    batch_txn_excp_rsn_sk = {excp_rsn_ne},
                                                    upd_by_user_id = user,
                                                    upd_dttm = current_timestamp
                                                    where batch_txn_sk = {}
                                                    '''.format(v_bt, ver_batch_txn_sts_rec_dict[0]['batch_txn_sk'], 
                                                            txn_sts_comp=CONSTANTS.BATCH_TXN_STS_COMPLETED, 
                                                            trgr_sts_succ=CONSTANTS.BATCH_TXN_TRGR_STS_SUCC, excp_rsn_ne=CONSTANTS.BATCH_TXN_EXCP_RSN_NE)
                                    pg.connect_pg.commit_pg_txn(mark_batch_txn_completed_query)
                                else:
                                    print('Failed to trigger the workflow:', meta_rec['batch_config_trgt_scrpt'], 'marking the batch as failed')
                                    mark_batch_txn_failed_query='''update {} 
                                                    set batch_txn_sts_sk = {txn_sts_fail},
                                                    batch_txn_trgr_sts_cd_sk = {trgr_sts_fail},
                                                    batch_txn_excp_rsn_sk = {excp_rsn_ne},
                                                    upd_by_user_id = user,
                                                    upd_dttm = current_timestamp
                                                    where batch_txn_sk = {}
                                                    '''.format(v_bt, ver_batch_txn_sts_rec_dict[0]['batch_txn_sk'], 
                                                            txn_sts_fail=CONSTANTS.BATCH_TXN_STS_FAIL, 
                                                            trgr_sts_fail=CONSTANTS.BATCH_TXN_TRGR_STS_FAIL, excp_rsn_ne=CONSTANTS.BATCH_TXN_EXCP_RSN_NE)
                                    pg.connect_pg.commit_pg_txn(mark_batch_txn_failed_query)
                                    
                            else:
                                print('Expected number of files are not received yet, so this batch transaction will stay open')
                
                else:
                    err_msg = 'No active metadata for this source table: {}'.format(v_table_nm)
                    print(err_msg)
                    ins_batch_txn='''insert into {} (
                                    batch_config_sk, batch_txn_sts_sk, batch_txn_data_extract_dt, batch_txn_wndw_bgn_dt, batch_txn_wndw_end_dt, 
                                    batch_txn_expct_file_cnt, batch_txn_recv_file_cnt, batch_txn_excp_rsn_sk, batch_txn_trgr_sts_cd_sk, batch_txn_max_wait_hours)
                                    select {inv_val}, {inv_val}, '{}',  current_date , current_date,{inv_val}, {inv_val}, {inv_val}, {inv_val}, {inv_val} 
                                    '''.format(v_bt, v_batch_txn_data_extract_dt, inv_val=CONSTANTS.INVALID_VALUE)
                    ins_batch_txn_dtl='''INSERT INTO {} (
                                        batch_config_dtl_sk, batch_txn_sk, batch_config_sk, batch_txn_dtl_sts_sk, batch_txn_json_dtl, batch_txn_dtl_excp_rsn_sk)
                                        select {inv_val},(select max(batch_txn_sk) from {} where batch_config_sk = {inv_val} ),{inv_val}, {inv_val}, cast('{}' as json ), {inv_val} 
                                        '''.format(v_btd, v_bt, json.dumps(v_json_msg), inv_val=CONSTANTS.INVALID_VALUE )
                    pg.connect_pg.commit_pg_txn(ins_batch_txn)
                    pg.connect_pg.commit_pg_txn(ins_batch_txn_dtl)
                    # TODO Call control Service
                    print('Entered information into Transaction tables, ending the process')
                    ctrl_msg = publish_sqs.gen_msg(CONSTANTS.CONTROL, {'batch_config_nm': 'No active Batch', 'batch_config_trgt_obj_nm': 'None'}, v_json_msg, CONSTANTS.QUEUE_ERROR, err_msg)
                    ctrl_resp = publish_sqs.pub_sqs(CONSTANTS.CONTROL_QUEUE_NAME, ctrl_msg)
                    if ctrl_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                        raise Exception('Error while sending message to Control Service queue:{}'.format(CONSTANTS.CONTROL_QUEUE_NAME))
            
                print('Process Completed for ', v_table_nm)
            else:
                print('No Message in this run:', datetime.now())
        except(Exception) as error:
            print("Failed Processing the message {} \n {}".format(v_json_msg, error)) # TODO: add exception handling
        finally:
            print('Completed round {}'.format(i))
            i+=1
            print('Sleeping for : {} seconds, for next message from {}'.format(CONSTANTS.CONSUME_SLEEPTIME, CONSTANTS.BATCHING_QUEUE_NAME))


if __name__ == "__main__":

#   batching_main()
    print("Print to test batching ECS")


