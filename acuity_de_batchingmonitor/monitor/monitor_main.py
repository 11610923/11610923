import sys
from itertools import groupby
from datetime import datetime
import uuid

sys.path.insert(0, r'/monitor_code')
import acuity_de_batchingmonitor.commons.CONSTANTS as CONSTANTS
import acuity_de_batchingmonitor.commons.connect_pg as pg
import acuity_de_batchingmonitor.commons.gen_sql as gen_sql
from acuity_de_batchingmonitor.commons.publish_sqs import publish_sqs as ps


def b_mon():

    """
        This method will be used to create public directory.

        :param str ctrl_pub_msg: Event Message.
        :param str config_nm: Configuration Name.
        :param str extract_dt: Extraxt Date.
        :param str trgt_obj_nm: Target Object Name.

        :returns: The JSON Response.
        
        :rtype: dict
    """

    try:
        # Fetching all Open Batches from Metadata Tables
        open_dict = pg.connect_pg.commit_pg_txn(gen_sql.open_sql)
        
        open_dict_by_config = {}
        for key, group in groupby(open_dict, lambda x: x['batch_config_sk']):
            values_list = [{k:v for k, v in d.items() if k != 'batch_config_sk'} for d in group]
            open_dict_by_config[key] = values_list
        
        for config_sk, config_value_list in open_dict_by_config.items():
            src_tab_list=[src['batch_config_dtl_src_file_nm'] for src in config_value_list if not src['file_rcvd'] ]
            ctrl_open_msg = f'''There is an open batch for the batch config {config_sk}, waiting for {len(src_tab_list)} table(s)/file(s) : {src_tab_list} with the extract date: "{config_value_list[0]['batch_txn_data_extract_dt']}" from source. Hence the target workflow "{config_value_list[0]['batch_config_trgt_scrpt']}" on "{config_value_list[0]['batch_config_trgt_app']}" is not triggered. Please investigate and take appropriate action.'''
            open_pub_dict = create_pub_dict(ctrl_open_msg, config_value_list[0]['batch_config_nm'], str(config_value_list[0]['batch_txn_data_extract_dt']), config_value_list[0]['batch_config_trgt_app'])
            open_pub_resp = ps.pub_sqs(CONSTANTS.CONTROL_QUEUE_NAME, open_pub_dict)
            if open_pub_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception('Error while sending message about Open Batches, from Batching Service Monitor to Control Service queue:{}'.format(CONSTANTS.CONTROL_QUEUE_NAME))
            
        # Fetching all Delayed Batches from Metadata Tables
        no_file_dict_list = pg.connect_pg.commit_pg_txn(gen_sql.no_file_sql)
        
        for no_file_dict in no_file_dict_list:
            ctrl_no_file_msg = f'''For the Batch Config {no_file_dict['batch_config_sk']}, the last load was with extract Timestamp "{no_file_dict['batch_txn_data_extract_dt']}". The next set of files were expected by "{no_file_dict['sla']}", none of the files are received till today, hence the workflow {no_file_dict['batch_config_trgt_scrpt']} on {no_file_dict['batch_config_trgt_app']} is not triggered. Please investigate and take the appropriate action.'''
            no_file_pub_dict = create_pub_dict(ctrl_no_file_msg, no_file_dict['batch_config_nm'], str(no_file_dict['batch_txn_data_extract_dt']), no_file_dict['batch_config_trgt_app'])
            no_file_pub_resp = ps.pub_sqs(CONSTANTS.CONTROL_QUEUE_NAME, no_file_pub_dict)
            if no_file_pub_resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception('Error while sending message about delayed bacth start, from Batching Service Monitor to Control Service queue:{}'.format(CONSTANTS.CONTROL_QUEUE_NAME))
            
    except(Exception) as error:
        print('Error:- ', error)

    finally:
        print(f'Batching Service Monitoring process completed at:', datetime.now().strftime('%Y-%m-%d %H:%M:%S') )

def create_pub_dict(ctrl_pub_msg: str, config_nm: str, extract_dt: str, trgt_obj_nm: str):
  
    """
        This method will be used to create public directory.

        :param str ctrl_pub_msg: Event Message.
        :param str config_nm: Configuration Name.
        :param str extract_dt: Extraxt Date.
        :param str trgt_obj_nm: Target Object Name.

        :returns: The JSON Response.

        :rtype: dict
    """
        
    pub_dict = CONSTANTS.CONTROL_JSON_DICT
    v_uuid = str(uuid.uuid4())
    v_current_dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    pub_dict['uuid'] = v_uuid
    pub_dict['eventMinor'] = config_nm
    pub_dict['exceptionType'] = CONSTANTS.BUSS_EXCEPTION
    pub_dict['exceptionName'] = ctrl_pub_msg
    pub_dict['eventMessage'] = ctrl_pub_msg
    pub_dict['extractStartTimeStamp'] = extract_dt
    pub_dict['eventTimeStamp'] = v_current_dtm
    pub_dict['entity'] = trgt_obj_nm

    return pub_dict


if __name__ == "__main__":
        b_mon()