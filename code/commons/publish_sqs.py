import boto3 as boto3
import uuid
from datetime import datetime
import json

import acuity_de_batchingmonitor.commons.CONSTANTS as CONSTANTS

class publish_sqs:
    
    def pub_sqs(queue_name: str, pub_msg: dict):
        # Create SQS client
        
        """
            This method will Create SQS client

            :param str queue_name: The Queue Name.
            
            :param dict pub_msg: The Public Message.

            :returns: The response message.

            :rtype: json
        """
        
        #session = boto3.Session()
        sqs = boto3.client('sqs')
        
        # Get URL for SQS queue
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        
        if queue_name == CONSTANTS.BATCHING_QUEUE_NAME:
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(pub_msg),
                MessageGroupId=str(uuid.uuid4()),
                MessageDeduplicationId=str(uuid.uuid4())
            )
        else:
            response = sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(pub_msg)
            )

        return response
    
    def gen_msg(target: str, meta_dict: dict, msg_dict: dict, exceptionType: str, evtMsg: str):
    
        """
            This method will Create SQS client

            :param str queue_name: The Queue Name.
            
            :param dict pub_msg: The Public Message.

            :returns: The response message.

            :rtype: json
        """
    
        pub_dict = CONSTANTS.CONTROL_JSON_DICT
        if target == 'CTRL':
            print(target)
            v_uuid = str(uuid.uuid4())
            v_current_dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            pub_dict['uuid'] = v_uuid
            pub_dict['eventMinor'] = meta_dict['batch_config_nm']
            pub_dict['exceptionType'] = exceptionType
            pub_dict['exceptionName'] = evtMsg
            pub_dict['eventMessage'] = evtMsg
            pub_dict['extractStartTimeStamp'] = msg_dict['extracttimestamp']
            pub_dict['eventTimeStamp'] = v_current_dtm
            pub_dict['entity'] = meta_dict['batch_config_trgt_obj_nm']
        
        return pub_dict
