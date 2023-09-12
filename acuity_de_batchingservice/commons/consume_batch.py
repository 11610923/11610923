import boto3 as boto3

#def __init__(self, queue_name):
#    self.queue_name = queue_name

class consume_batch:

    session = boto3.Session()
    def consume_sqs(queue_name):

        # Create SQS client
        sqs = boto3.client('sqs', region_name='us-east-1')
        
        # Get URL for SQS queue
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        
        response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=100,
        WaitTimeSeconds=10
        )

        #print(response)  

        
        if 'Messages' in response:

            messages = response['Messages']
            for message in messages:
                receipt_handle = message['ReceiptHandle']
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
            
            return messages
            
        else:
            return ()

    #return response['Messages']

#consume_sqs('audit_queue')


    
