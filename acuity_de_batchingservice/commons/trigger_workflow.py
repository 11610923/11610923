
import requests
import argparse
import json
import os
import sys

sys.path.insert(0, r'/batching_code')
import acuity_de_batchingservice.commons.trigger_workflow as trig_wf

class NoJobIdFound(Exception):
    "Raised when there is no Databricks workflow avaiable and no JobId to return"
    pass

class FetchJobIdError(Exception):
    "Raised when process failed while fetching the Job Id for the given Databricks workflow"
    pass

class trigger_workflow:

   def getJobId(databricks_url: str, access_token: str, databricks_wf: str):

      url_list = "https://"+databricks_url+"/api/2.1/jobs/list"
      headers = '{"Authorization": "Bearer '+ access_token +'"}'

      headers = json.loads(headers)

      try:
         res = requests.get(url_list, headers=headers)
         request = res.json()
         
         job = request['jobs']
         
         jobId_list = []

         for i in job:
            if databricks_wf in i['settings']['name']:
               jobId_list.append(i['job_id'])
        
         if len(jobId_list) == 0:
            raise NoJobIdFound
         return jobId_list

      except NoJobIdFound:
         raise NoJobIdFound("No Databricks workflow with name: {}".format(databricks_wf))
      except (Exception) as error:
         raise FetchJobIdError("Error while fetching the Job ID for the Databricks Workflow: {}, With the Error: {}".format(databricks_wf, error))
      




   def triggerJob(databricks_url: str, access_token:str, databricks_wf:str):

      headers = '{"Authorization": "Bearer '+ access_token +'"}'
      headers = json.loads(headers)
      res_succ_list =[]
      res_fail_list =['999']


      try:
         jobId_list = trig_wf.trigger_workflow.getJobId(databricks_url, access_token, databricks_wf)
         
         for id in jobId_list:
            url_trigger = "https://"+databricks_url+"/api/2.1/jobs/run-now"
            params ='{"job_id": "'+str(id)+'"}'
            params = json.loads(params)

            res = requests.post(url=url_trigger, headers=headers, json=params)
            #print(res.content)
            res_succ_list.append(res)
         return res_succ_list
      except (NoJobIdFound, FetchJobIdError) as error:
         res_fail_list.append(error)
         return res_fail_list
      except (Exception) as error:
         res_fail_list.append('Failed while triggerring the Databricks Workflow: {}, having job ID: {}, with error: {}'.format(databricks_wf, jobId_list, error))
         return res_fail_list

      


   #if __name__=="__main__":
      
      #trig_resp = triggerJob('acuity-ozd.cloud.databricks.com', 'dapi8a4a8c7125901c986f40ca350e39b301', 'gia_silver_prov_fndtn_batching')

      
      #print(trig_resp)
      #print(type(trig_resp))
