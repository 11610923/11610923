from unittest import main
import psycopg2
from psycopg2 import Error
import boto3
import json
from botocore.config import Config
import sys

import acuity_de_batchingmonitor.commons.CONSTANTS as CONSTANTS

class connect_pg:

    def get_conn_params():

        my_config = Config(region_name = 'us-east-1',retries = {'max_attempts': 10, 'mode': 'standard'})

        secrets_client = boto3.client('secretsmanager', config=my_config)

        secret_arn = 'arn:aws:secretsmanager:us-east-1:918623739618:secret:rds/postgres/gwt-acuity-dev-cmn-abc20220824162033286600000001-Sg5Bln'

        auth_token: dict = secrets_client.get_secret_value(SecretId=secret_arn).get('SecretString')

        return auth_token


    def commit_pg_txn(query: str):

        record = None
        connection = None
        rec_dict_list = []

        try:
            
            my_config = Config(region_name = 'us-east-1',retries = {'max_attempts': 10, 'mode': 'standard'})

            secrets_client = boto3.client('secretsmanager', config=my_config)

            secret_arn = 'arn:aws:secretsmanager:us-east-1:918623739618:secret:rds/postgres/gwt-acuity-dev-cmn-abc20220824162033286600000001-Sg5Bln'

            auth_token: str = secrets_client.get_secret_value(SecretId=secret_arn).get('SecretString')
            auth_token_dict: dict = json.loads(auth_token)

            username = auth_token_dict.get('username')
            password = auth_token_dict.get('password')
            host = auth_token_dict.get('host').split(':')[0]
            port = auth_token_dict.get('port')
            dbname = CONSTANTS.PG_GIA_DB
        except (Exception, Error) as error:
            raise Exception("Error while fetching details from Secrects Manager", error)
        else:
            try:             
                connection = psycopg2.connect(user=username,
                                            password=password,
                                            host=host,
                                            port=port,
                                            dbname=dbname)
                                            
                # Create a cursor to perform database operations
                cursor = connection.cursor()
                connection.autocommit = True
            except (Exception, Error) as error:
                raise Exception("Error while connecting to PostgreSQL", error)
            else:
                try:
                    # Executing a SQL query
                    cursor.execute(query)
                    # Fetch result
                except (Exception, Error) as error:
                    raise Exception("Error while Executing the query in PostgreSQL", error)
                else:
                    if query.upper().startswith('SELECT'):
                        try:
                            record = cursor.fetchall()
                        except (Exception, Error) as error:
                            raise Exception("Error while Fetching records from PostgreSQL for the query", error)
                        else:
                            if len(record) > 0:
                                rec_dict_list=[dict(zip([column[0] for column in cursor.description], row)) for row in record]
        finally:
            if (connection):
                cursor.close()
                connection.close()
                
        return rec_dict_list


    #if __name__ == "__main__":
    #    print(commit_pg_txn("select max(batch_txn_sk) from batch_srvc.batch_txn "))
            
