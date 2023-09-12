AUDIT_QUEUE_NAME = 'gwt-acuity-dev-cmn-abc-audit-service.fifo'
BATCHING_QUEUE_NAME = 'gwt-acuity-dev-cmn-batching-service.fifo'
########################################
            ## COMMON ##
########################################  

CREATE_USER = 'ACUITY'
UPDATE_USER = 'ACUITY'
CONSUME_SLEEPTIME=30

########################################
##  BATCHING SERVICE METADATA TABLES  ##
########################################   
# BC 	--> BATCH_CONFIG
# BCT 	--> BATCH_CONFIG_DTL
# BT 	--> BATCH_TXN
# BTD 	--> BATCH_TXN_DTL
# LTC	--> LOOK_UP_TYP_CD
# LTV	--> LOOK_UP_TYP_VAL
PG_GIA_DB = 'gia-db'
GIA_METADATA_SCHEMA_PREFIX='gia_metadata_'
BATCH_CONFIG_TABLE='BATCH_CONFIG'
BATCH_CONFIG_DTL_TABLE='BATCH_CONFIG_DTL'
BATCH_TXN_TABLE='BATCH_TXN'
BATCH_TXN_DTL_TABLE='BATCH_TXN_DTL'

BC_COLUMNS = 'BATCH_CONFIG_SK, TENANT_CD, BATCH_CONFIG_NM, BATCH_CONFIG_DESC, BATCH_CONFIG_CNT, BATCH_CONFIG_SNGL_FILE_IND, BATCH_CONFIG_FREQ_HOUR, BATCH_CONFIG_SLA_HOUR, LOB_SK, BATCH_CONFIG_STS_SK, BATCH_CONFIG_LOAD_PRCSS_NM_SK, BATCH_CONFIG_TRGT_APP, BATCH_CONFIG_TRGT_OBJ_NM, BATCH_CONFIG_TRGT_SCRPT, BATCH_CONFIG_ALERT_EMAIL_LIST, CRT_BY_USER_ID, UPD_BY_USER_ID, CRT_DTTM, UPD_DTTM'

BCD_COLUMNS = 'BATCH_CONFIG_DTL_SK, BATCH_CONFIG_SK, BATCH_CONFIG_DTL_SRC_FILE_NM, BATCH_CONFIG_DTL_SRC_FILE_TYP_SK, CRT_BY_USER_ID, UPD_BY_USER_ID, CRT_DTTM, UPD_DTTM'

BT_COLUMNS = 'BATCH_TXN_SK, BATCH_CONFIG_SK, BATCH_TXN_STS_SK, BATCH_TXN_DATA_EXTRACT_DT, BATCH_TXN_WNDW_BGN_DT, BATCH_TXN_WNDW_END_DT, BATCH_TXN_EXPCT_FILE_CNT, BATCH_TXN_RECV_FILE_CNT, BATCH_TXN_EXCP_RSN_SK, BATCH_TXN_TRGR_STS_CD_SK, BATCH_TXN_MAX_WAIT_HOURS, CRT_BY_USER_ID, UPD_BY_USER_ID, CRT_DTTM, UPD_DTTM'

BTD_COLUMNS = 'BATCH_TXN_DTL_SK, BATCH_CONFIG_DTL_SK, BATCH_TXN_SK, BATCH_CONFIG_SK, BATCH_TXN_DTL_STS_SK, BATCH_TXN_JSON_DTL, BATCH_TXN_DTL_EXCP_RSN_SK, CRT_BY_USER_ID, UPD_BY_USER_ID, CRT_DTTM, UPD_DTTM'

LTC_COLUMNS = 'LOOK_UP_TYP_CD, LOOK_UP_TYP_CD_DESC, CRT_BY_USER_ID, UPD_BY_USER_ID, CRT_DTTM, UPD_DTTM'

LTV_COLUMNS = 'LOOK_UP_TYP_VAL_SK, LOOK_UP_TYP_CD, LOOK_UP_TYP_VAL, LOOK_UP_TYP_VAL_DESC, CRT_BY_USER_ID, UPD_BY_USER_ID, CRT_DTTM, UPD_DTTM'

CONFIG_STS_ACTIVE=100003
CONFIG_STS_INACTIVE=100004
BATCH_TXN_STS_OPEN=100011
BATCH_TXN_STS_COMPLETED=100012
BATCH_TXN_STS_FAIL=100013
BATCH_TXN_TRGR_STS_SUCC=100014
BATCH_TXN_TRGR_STS_FAIL=100015
BATCH_TXN_EXCP_RSN_INV_WIN=100016
BATCH_TXN_EXCP_RSN_DUP=100017
BATCH_TXN_EXCP_RSN_PREV_WIN=100018
BATCH_TXN_EXCP_RSN_FUT_WIN=100019
BATCH_TXN_EXCP_RSN_NE=100099
INVALID_VALUE=999999

EXTRACT_DTTM_FORMAT='%Y-%m-%dT%H:%M:%S.%fZ'
EXTRACT_PG_DTTM_FORMAT='YYYY-MM-DDTHH24:MI:SS.USZ'

########################################
            ## Databricks ENV ##
########################################  

DATABRICKS_URL = 'acuity-ozd.cloud.databricks.com'
DATABRICKS_AT = 'dapi8a4a8c7125901c986f40ca350e39b301'
DATABRICKS_WF = 'gia_silver_prov_fndtn_batching'
DATABRICKS_SUCC = '200'

########################################
            ## Control Service ##
########################################  

CONTROL_QUEUE_NAME = 'gwt-acuity-dev-cmn-abc-control-service'

CONTROL_JSON_DICT = {
	'uuid': "None",
	'eventMessage': "None",
	'eventMajor': "BATCHING",
	'eventMinor': "None",
	'exceptionFlag': "Y",
	'exceptionType': "None",
	'exceptionName': "None",
	'exceptionArgument': "Target Trigger",
	'entity': "None",
	'version': "None",
	'subEntity': {}, 
	'retryCount': "None",
	'extractStartTimeStamp': "None",
	'eventTimeStamp': "None",
	'extractEndTimeStamp': "None",
	'btch_dtl': "None",
}

########################################
            ## Exception Types ##
########################################

TECH_ERROR = 'Technical Error'
BUSS_EXCEPTION = 'Business Exception'
SYS_ERROR = 'System Error'
QUEUE_ERROR = 'Queue Error'
TGT_ERROR = 'Target Error'

########################################
            ## SYSTEMS ##
########################################

CONTROL = 'CTRL'
SILVER = 'SLVR'
GOLD = 'GOLD'
BRONZE = 'BRNZ'
RAW = 'RAW'