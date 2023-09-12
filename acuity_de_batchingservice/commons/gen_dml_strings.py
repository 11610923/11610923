from operator import contains
import sys
from datetime import datetime
import json

#import mapping_json_to_tbl_slvr_gld as mapping

sys.path.insert(0, r'C:\\Users\\vgundla\Documents\\Repo\\Gainwell\\acuity-de-auditservice')

#sys.path.insert(0, r'/audit_code')

import acuity_de_auditservice.commons.CONSTANTS as CONSTANTS
import acuity_de_auditservice.commons.connect_pg as pg


class gen_dml_stmt:

    pg_schema :str = CONSTANTS.SCHEMA

    file_id :str = CONSTANTS.FC_FILE_ID
    msg_id :str =  CONSTANTS.MI_MSG_ID
    
    parent_file_nm :str = CONSTANTS.FC_PARENT_FILE_NAME
    file_nm :str = CONSTANTS.FC_FILE_NAME

    fc_col :str = CONSTANTS.FC_COLUMNS
    ign_upd_col :str = CONSTANTS.FC_IGN_UPD_COL
    fcs_col :str = CONSTANTS.FCS_COL
    fcs_ref_col :str = CONSTANTS.FCS_REF_COL
    msg_inv_col: str = CONSTANTS.MSG_INV_COL
    excp_col:str = CONSTANTS.EXCP_COL
    excp_cfg_col:str = CONSTANTS.EXCP_CFG_COL

    btch_ctrl_tbl: str = CONSTANTS.BATCH_CTRL_TBL
    btch_ctrl_dtl_tbl: str = CONSTANTS.BATCH_CTRL_DTL_TBL
    btch_ctrl_dtl_src_tbl: str = CONSTANTS.BATCH_CTRL_DTL_SRC_TBL

    btch_ctrl_ins_col: str = CONSTANTS.BATCH_CTRL_INS_COL
    btch_ctrl_upd_col: str = CONSTANTS.BATCH_CTRL_UPD_COL
    btch_ctrl_dtl_col: str = CONSTANTS.BATCH_CTRL_DTL_COL
    btch_ctrl_dtl_src_col: str = CONSTANTS.BATCH_CTRL_DTL_SRC_COL

    batch_id: str = CONSTANTS.BATCH_ID
    #uuid_col: str = CONSTANTS.UUID
    batch_dtl_sts_nm: str = CONSTANTS.BATCH_DTL_STS_NM
    #btch_ctrl_dtl_src_col: str = CONSTANTS.BATCH_DTL_SRC_COL
    batch_dtl_id: str = CONSTANTS.BATCH_DTL_ID

    slvr_btch_strt_sts: str = CONSTANTS.SLVR_BTCH_STRT_STS
    slvr_btch_fail_sts: str = CONSTANTS.SLVR_BTCH_FAIL_STS
    slvr_btch_succ_sts: str = CONSTANTS.SLVR_BTCH_SUCC_STS

    slvr_load_strt_sts: str = CONSTANTS.SLVR_LOAD_STRT_STS
    slvr_load_succ_sts: str = CONSTANTS.SLVR_LOAD_SUCC_STS

    excp_actv_ind: str = CONSTANTS.EXCP_ACTV_IND

    crt_usr :str = CONSTANTS.CREATE_USER
    upd_usr :str = CONSTANTS.UPDATE_USER

    ## TODO: Should replace with the actual objects
    # .format(columns = 'columns', schema = pg_schema, table_name = 'table_name')
    select_string = 'SELECT {columns} FROM %(schema)s.{table_name}' % {
        'schema': pg_schema}
    # .format(schema = pg_schema, table_name = 'table_name')
    update_string = 'UPDATE %(schema)s.{table_name}' % {'schema': pg_schema}
    where_string = ' WHERE {conditions}'
    insert_string = 'INSERT INTO %(schema)s.{table_name} ({columns}) VALUES ( {values} )' % {
        'schema': pg_schema}
    upsert_string = ' ON CONFLICT ({col_name}) DO UPDATE SET '

    def upsrt_fc_tbl(self, mapping_dict: dict):

        #print(mapping_dict)
        parent_file_name = mapping_dict.get('PARENT_FILE_NM')
        sel_query_parent_id = self.select_string.format(columns = ''.join( ('max(' , self.file_id, ')') ), table_name = 'FILE_CNTL' ) + \
                              self.where_string.format(conditions =''.join( (self.parent_file_nm, '=\'', parent_file_name, '\'')) )

        print(sel_query_parent_id)
        #sel_query_parent_id = "select max(file_id) from public.file_cntl where file_nm='TEST_FILE_3'"
        parent_id = pg.connect_pg.commit_pg_txn(sel_query_parent_id)[0][0]                          
     
        mapping_dict['PARENT_FILE_ID'] = parent_id
        mapping_dict['CRT_USER_NM'] = self.crt_usr
        mapping_dict['CRT_DTM'] = datetime.now()
        mapping_dict['UPD_USER_NM'] = self.upd_usr
        mapping_dict['UPD_DTM'] = datetime.now()
        

        columns = self.fc_col
        values = '\''
        update_values = ''
        ign_upd_col_list :list = [ ign_col.strip() for ign_col in (self.ign_upd_col).split(',') ]
        #['INGSTN_CONFIG_ID', 'PARENT_FILE_ID', 'FILE_NM', 'SCHMA_VRSN_ID', 'FEED_TYPE', 'EXTRCT_DTM', 'FILE_PRCSS_BEG_DTM', 'CRT_USER_NM', 'CRT_DTM', 'FILE_PRCSS_END_DTM']

        #print((ign_upd_col_list))

        print("mapping_dict ==> {}".format(mapping_dict))
        excp_flag: str = mapping_dict.get('EXCEPTION_FLAG')

# TODO: Add final status to the below condition like excp_flag.upper() == 'Y' or status = 'bronze completed'
        if excp_flag.upper() == 'Y':
            #print(ign_upd_col_list)
            ign_upd_col_list.remove('FILE_PRCSS_END_DTM')

        for col in columns.split(','):

            col_strip = col.strip()
            col_value: str = str(mapping_dict.get(col_strip))
            #print(col_value)
            values = values + str(col_value) + "', '"

# TODO: add actual value when to update the file process end dtm , that is the last status for the file or check the exception flag
            #if col_value != None and 'FAILURE' or 'NOT_FOUND' in col_value.upper():

            if col_value and col_strip not in ign_upd_col_list:
                update_values = update_values + col_strip + " = '" + str(col_value) + "', "

        values = values.replace("'None'", "null")[0:-3]
        upsert_string = self.upsert_string + update_values[0:-2]
        #print(values)

        upsrt_fc_query = self.insert_string.format(
            table_name='FILE_CNTL', columns=columns, values=values) + upsert_string.format(col_name='FILE_NM')

        values = ''
        print(upsrt_fc_query)    

        return upsrt_fc_query

    def ins_fcs_tbl(self, mapping_dict: dict, fcs_ref_dict: dict, excp_cfg_dict: dict):

        file_name = mapping_dict.get('FILE_NM')
        parent_file_name = mapping_dict.get('PARENT_FILE_NM')

        sel_query_file_id = self.select_string.format(columns = ''.join( ('max(' , self.file_id, ')') ), table_name = 'FILE_CNTL' ) + \
                              self.where_string.format(conditions = ''.join( (self.file_nm, '=\'', file_name, '\' and ', self.parent_file_nm, '=\'', parent_file_name, '\'')) )

        print(sel_query_file_id)

        file_id = pg.connect_pg.commit_pg_txn(sel_query_file_id)[0][0]   

        file_sts_desc = mapping_dict.get('FILE_STS_DESC')

        excp_flag: str = mapping_dict.get('EXCEPTION_FLAG')

        file_sts_cd = ''
        
        if excp_flag.upper() == 'N':
            file_sts_cd = fcs_ref_dict.get(file_sts_desc)
        elif excp_flag.upper() == 'Y':
            file_sts_cd = excp_cfg_dict.get(file_sts_desc)

        columns = self.fcs_col
        values = '\''

        mapping_dict['FILE_CNTRL_ID'] = file_id
        mapping_dict['FILE_STS_CD'] = file_sts_cd
        mapping_dict['FILE_STS_DESC'] = file_sts_desc
        mapping_dict['CRT_USER_NM'] = self.crt_usr
        mapping_dict['CRT_DTM'] = datetime.now()

        for col in columns.split(','):

            col_strip = col.strip()
            col_value = mapping_dict.get(col_strip)
            #print(col_value)
            values = values + str(col_value) + "', '"

        #values = values.replace("'None'", "null")[0:-3]
        # TODO: uncomment the above after testing and comment the below
        values = values[0:-3]
        # prepare columns and values
        insrt_fcs_query = self.insert_string.format(table_name = 'file_cntl_sts', columns = columns, values = values)
        values = ''

        print(insrt_fcs_query)


        # crt insrt query to insert record into file ctl sts tbl
        # crt insrt query to messages tbl

        #print(sel_query_file_id)
        #sel_query_file_id = "select max(file_id) from public.file_cntl where file_nm='TEST_FILE_3'"
        #record_list : list = pg.Connect_PG.commit_pg_txn(sel_query_file_id)                              
        #file_id = record_list[0][0]


        return insrt_fcs_query

    def sel_fcs_ref_tbl(self):

        sel_query_fcs_ref = self.select_string.format(columns = self.fcs_ref_col, table_name = 'FILE_STS_REF')

        return sel_query_fcs_ref

    def ins_msg_tbl(self, msg_body, valid_json = True):

        #SVC_NAME, PAYLOAD, CRT_USER_NM, CRT_DTM
        msg_inv_dict: dict = {}
        columns = self.msg_inv_col
        values = '\''

        #col_key_list =  []

        if valid_json:
            msg_inv_dict['SVC_NAME'] =  msg_body.get('eventMajor')
        else:
            msg_inv_dict['SVC_NAME'] =  'INVALID_JSON'
        
        msg_inv_dict['PAYLOAD'] = json.dumps(msg_body)
        msg_inv_dict['CRT_USER_NM'] = self.crt_usr
        msg_inv_dict['CRT_DTM'] = datetime.now()    


        for col in msg_inv_dict.keys():

            col_strip = col.strip()
            col_value = msg_inv_dict.get(col_strip)
            values = values + str(col_value) + "', '"

        values = values[0:-3]
        ins_msg_tbl = self.insert_string.format(table_name = 'msg_inventory', columns = columns, values = values)
        values = ''
        print(ins_msg_tbl)
        return ins_msg_tbl

    def sel_excp_cfg_tbl(self):

        #print(self.select_string.format(columns = self.excp_cfg_col, table_name = 'EXCP_CONFIG') + self.where_string.format(conditions = ''.join( (self.excp_actv_ind.upper(), '=\'Y\'') )))
        where_cond = self.where_string.format(conditions = ''.join( (self.excp_actv_ind.upper(), '=\'Y\'') ))

        sel_query_excp_cfg = self.select_string.format(columns = self.excp_cfg_col, table_name = 'EXCP_CONFIG')  + where_cond
    
        #print('sel_query_excp_cfg ==> {}'.format(sel_query_excp_cfg))
        return sel_query_excp_cfg

    def ins_excp_tbl(self, mapping_dict: dict, excp_cfg_dict: dict):

        file_name = mapping_dict.get('FILE_NM')
        parent_file_name = mapping_dict.get('PARENT_FILE_NM')

        sel_query_file_id = self.select_string.format(columns = ''.join( ('max(' , self.file_id, ')') ), table_name = 'FILE_CNTL' ) + \
                              self.where_string.format(conditions = ''.join( (self.file_nm, '=\'', file_name, '\' and ', self.parent_file_nm, '=\'', parent_file_name, '\'')) )

        print(sel_query_file_id)

        file_id = pg.connect_pg.commit_pg_txn(sel_query_file_id)[0][0] 

        excp_config_desc = mapping_dict.get('EXCP_TYP_DESC')
        excp_typ_cd =  excp_cfg_dict.get(excp_config_desc) 

        # EXCP_CONFIG_DESC, EXCP_TYP_CD


        sel_query_msg_id = self.select_string.format(columns = ''.join( ('max(' , self.msg_id, ')') ), table_name = 'MSG_INVENTORY' ) + \
                              self.where_string.format(conditions = ''.join( ('payload ->> \'fileName\' =\'', file_name, '\' and ', 'payload ->> \'eventMinor\'=\'', excp_config_desc, '\'')) )

        msg_id = pg.connect_pg.commit_pg_txn(sel_query_msg_id)[0][0] 

        columns = self.excp_col
        # MSG_ID, FILE_CNTRL_ID, EXCP_TYP_CD, SVC_NOW_INCDT_ID, CRT_USER_NM, CRT_DTM
        values = '\''

        mapping_dict['MSG_ID'] = msg_id
        mapping_dict['FILE_CNTRL_ID'] = file_id
        mapping_dict['EXCP_TYP_CD'] = excp_typ_cd
        mapping_dict['SVC_NOW_INCDT_ID'] = ''
        mapping_dict['CRT_USER_NM'] = self.crt_usr
        mapping_dict['CRT_DTM'] = datetime.now()

        for col in columns.split(','):

            col_strip = col.strip()
            col_value = mapping_dict.get(col_strip)
            #print(col_value)
            values = values + str(col_value) + "', '"

        values = values.replace("'None'", "null")[0:-3]
        # prepare columns and values
        insrt_excp_query = self.insert_string.format(table_name = 'file_excp', columns =columns, values = values)
        values = ''

        print(insrt_excp_query)

        return insrt_excp_query

    def ups_btch_ctrl_tbl(self, mapping_dict: dict):

        #silver_json = open("C:\\Users\\vgundla\\Documents\\Repo\\Gainwell\\acuity-de-auditservice\\acuity_de_auditservice\\dependencies\\silver.json")
        #json_dict: dict = json.load(silver_json)

        batch_tbl_dict: dict = {}
        values = '\''
        update_values = ''
        #mapping_dict = mapping.mapping_json_to_tbl_slvr_gld()

        ins_col = self.btch_ctrl_ins_col
        upd_col = self.btch_ctrl_upd_col
        #print(columns)
        #col_list :list = [ col.strip() for col in (columns).split(',') ]
        #col_list.remove('' ,)
        #print(col_list)

        batch_id = str(mapping_dict.get('BATCH_ID')).upper()

        batch_tbl_dict['CRT_USER_NM'] = self.crt_usr
        batch_tbl_dict['CRT_DTM'] = datetime.now()
        # batch_tbl_dict['UPD_USER_NM'] = self.upd_usr
        # batch_tbl_dict['UPD_DTM'] = datetime.now()
        batch_tbl_dict['BATCH_ID'] = batch_id

        if 'BATCH_BGN_DTM' in mapping_dict:
            batch_tbl_dict['BATCH_STS_NM'] = self.slvr_btch_strt_sts
            batch_tbl_dict['BATCH_BGN_DTM'] = mapping_dict['BATCH_BGN_DTM']
            batch_tbl_dict['UPD_DTM'] = datetime.now()
            batch_tbl_dict['UPD_USER_NM'] = self.upd_usr

            #upsert_string = 'DO NOTHING';
        # elif 'FAILED' in str(mapping_dict.get('BATCH_DTL_STS_NM')).upper():
        #     mapping_dict['BATCH_STS_NM'] = self.slvr_btch_fail_sts


        elif 'BATCH_END_DTM' in mapping_dict:

            batch_tbl_dict['UPD_USER_NM'] = self.upd_usr
            batch_tbl_dict['UPD_DTM'] = datetime.now() 
            batch_tbl_dict['BATCH_END_DTM'] = mapping_dict['BATCH_END_DTM']   

            #this start time not used to avoid None conflict inserting BGN Batch
            batch_tbl_dict['BATCH_BGN_DTM'] = datetime.now()       

            sel_batch_id_qry = self.select_string.format(columns = self.batch_id, table_name = self.btch_ctrl_tbl) + self.where_string.format(conditions = ''.join( ('UPPER(', self.batch_id, ') = \'', batch_id, '\'') ))

            batch_id_val = pg.connect_pg.commit_pg_txn(sel_batch_id_qry)[0][0]

            ftch_sts_qry = self.select_string.format(columns = 'COUNT(*)', table_name = self.btch_ctrl_dtl_tbl) + self.where_string.format(conditions = ''.join( ( self.batch_id, ' = \'', str(batch_id_val) ,'\' AND UPPER(', self.batch_dtl_sts_nm, ') LIKE \'%FAIL%\'' ) ))

            fail_count = pg.connect_pg.commit_pg_txn(ftch_sts_qry)[0][0]

            if fail_count > 0 or 'FAILED' in str(mapping_dict.get('BATCH_DTL_STS_NM')).upper():
                batch_tbl_dict['BATCH_STS_NM'] = self.slvr_btch_fail_sts
            else:
                batch_tbl_dict['BATCH_STS_NM'] = self.slvr_btch_succ_sts

# BATCH_CTRL_COL = 'UUID, BATCH_STS_NM, BATCH_BGN_DTM, BATCH_END_DTM, CRT_USER_NM, CRT_DTM, UPD_USER_NM, UPD_DTM'
        print('batch_tbl_dict ==> {}'.format(batch_tbl_dict.keys()))
        # for col in col_list:
        #     batch_tbl_dict[col] = mapping_dict[col]
        print('batch_tbl_dict ==> {}'.format(batch_tbl_dict))
        #upsrt_batch_query = self.insert_string.format(table_name=self.btch_ctrl_tbl, columns=columns, values=values) + upsert_string.format(col_name='FILE_NM')

        for  col in ins_col.split(','):

            col_strip = col.strip()
            col_value = batch_tbl_dict.get(col_strip)
            values = values + str(col_value) + "', '"

        values = values[0:-3]

        #ins_col = ', '.join( (batch_tbl_dict.keys()) )

        for col in upd_col.split(','): 
            print('col ==> {}'.format(col))
            col_strip = col.strip()
            col_value = batch_tbl_dict.get(col_strip)
            update_values = update_values + col_strip + " = '" + str(col_value) + "', "

        update_values = update_values.replace("'None'", "null")
        upsert_string = self.upsert_string + update_values[0:-2]

        upsrt_batch_query = self.insert_string.format(table_name=self.btch_ctrl_tbl, columns=ins_col, values=values) + upsert_string.format(col_name='BATCH_ID')
            #'SELECT * FROM {1}.{2} WHERE UPPER({3}) LIKE {4}'.format(self.pg_schema, self.btch_ctrl_dtl_tbl, 'BATCH_DTL_STS_NM', uuid) 
        print(upsrt_batch_query) 

        return upsrt_batch_query


    def ins_btch_ctrl_dtl_tbl(self, mapping_dict: dict):

        #mapping_dict = mapping.mapping_json_to_tbl_slvr_gld()

        #  'BATCH_ID, REG_NM, DB_NM, TBL_NM, BATCH_DTL_STS_NM, CRT_USER_NM, CRT_DTM'
        print(list(mapping_dict.keys()))

        del_key = {'DB_NM.TBL_NM', 'TBL_N_VERSION'}
        mapping_key_iter = [e for e in list(mapping_dict.keys()) if e not in del_key]
        print(mapping_key_iter)

        batch_tbl_dtl_dict: dict = {}
        values = '\''
        columns = self.btch_ctrl_dtl_col

        for key in mapping_key_iter:
            batch_tbl_dtl_dict[key] = mapping_dict[key]

        db_tbl: str = mapping_dict['DB_NM.TBL_NM']    

        batch_tbl_dtl_dict['DB_NM'] = db_tbl.split('.')[0]
        batch_tbl_dtl_dict['TBL_NM'] = db_tbl.split('.')[1]
        batch_tbl_dtl_dict['CRT_USER_NM'] = self.crt_usr
        batch_tbl_dtl_dict['CRT_DTM'] = datetime.now()
        batch_tbl_dtl_dict['BATCH_DTL_STS_NM'] = mapping_dict['BATCH_DTL_STS_NM']


        for col in columns.split(','):
            col_strip = col.strip()
            col_value = batch_tbl_dtl_dict.get(col_strip)
            values = values + str(col_value) + "', '"

        values = values[0:-3]

        
        ins_btch_ctrl_dtl_query = self.insert_string.format(table_name=self.btch_ctrl_dtl_tbl, columns=columns, values=values)

        values = ''

        print(ins_btch_ctrl_dtl_query)

        


        



        return ins_btch_ctrl_dtl_query

    def ins_btch_ctrl_dtl_src_tbl(self, mapping_dict: dict):

        #mapping_dict = mapping.mapping_json_to_tbl_slvr_gld()

        #  'BATCH_DTL_ID, DB_NM, TBL_NM, ENTITY_VRSN, CRT_USER_NM, CRT_DTM'
        print(list(mapping_dict.keys()))

        del_key = {'DB_NM.TBL_NM', 'TBL_N_VERSION'}
        mapping_key_iter = [e for e in list(mapping_dict.keys()) if e not in del_key]
        print(mapping_key_iter)

        batch_tbl_dtl_src_dict: dict = {}
        ins_btch_ctrl_dtl_src_query_list = list()
        values = '\''
        columns = self.btch_ctrl_dtl_src_col
        batch_id = mapping_dict[self.batch_id]

        
        db_tbl: str = mapping_dict['DB_NM.TBL_NM']    

        db_nm = db_tbl.split('.')[0]
        tbl_nm = db_tbl.split('.')[1]
        load_sts = 'GIA_LOAD_SLVR_COMPLETED'
        sel_query_batch_dtl_id = self.select_string.format(columns = self.batch_dtl_id, table_name = self.btch_ctrl_dtl_tbl) + \
                              self.where_string.format(conditions = ''.join( (self.batch_id, ' = \'',  batch_id, '\' AND DB_NM = \'', db_nm, '\' AND TBL_NM = \'', tbl_nm, '\' AND BATCH_DTL_STS_NM = \'', load_sts, '\'' ) ))
        print(sel_query_batch_dtl_id)    

        batch_dtl_id = pg.connect_pg.commit_pg_txn(sel_query_batch_dtl_id)[0][0]       
        print(batch_dtl_id)                   


        # for key in mapping_key_iter:
        #     batch_tbl_dtl_src_dict[key] = mapping_dict[key]

        batch_tbl_dtl_src_dict['BATCH_DTL_ID'] = batch_dtl_id
        batch_tbl_dtl_src_dict['CRT_USER_NM'] = self.crt_usr
        batch_tbl_dtl_src_dict['CRT_DTM'] = datetime.now()
        print("printing the mapping_dict")
        print(mapping_dict)
        db_tbl: dict = mapping_dict['TBL_N_VERSION']  

        print(db_tbl)  

        for key in db_tbl.keys():
            batch_tbl_dtl_src_dict['DB_NM'] = key.split('.')[0]
            batch_tbl_dtl_src_dict['TBL_NM'] = key.split('.')[1]
            batch_tbl_dtl_src_dict['ENTITY_VRSN'] = db_tbl.get(key)

            for col in columns.split(','):
                col_strip = col.strip()
                col_value = batch_tbl_dtl_src_dict.get(col_strip)
                values = values + str(col_value) + "', '"

            values = values[0:-3]

            ins_btch_ctrl_dtl_src_query = self.insert_string.format(table_name=self.btch_ctrl_dtl_src_tbl, columns=columns, values=values)
            values = '\''
            ins_btch_ctrl_dtl_src_query_list.append(ins_btch_ctrl_dtl_src_query)

        print(ins_btch_ctrl_dtl_src_query_list)
        values = ''

        return ins_btch_ctrl_dtl_src_query_list





        # batch_tbl_dtl_src_dict['DB_NM'] = db_tbl.split('.')[0]
        # batch_tbl_dtl_src_dict['TBL_NM'] = db_tbl.split('.')[1]
        # batch_tbl_dtl_src_dict['CRT_USER_NM'] = self.crt_usr
        # batch_tbl_dtl_src_dict['CRT_DTM'] = datetime.now()


        # for col in columns.split(','):
        #     col_strip = col.strip()
        #     col_value = batch_tbl_dtl_src_dict.get(col_strip)
        #     values = values + str(col_value) + "', '"

        # values = values[0:-3]

        
        # ins_btch_ctrl_dtl_query = self.insert_string.format(table_name=self.btch_ctrl_dtl_tbl, columns=columns, values=values)

        # print(ins_btch_ctrl_dtl_query)

        


        


        
        #return ins_btch_ctrl_dtl_query

#tmp = gen_dml_stmt()

#msg_body = '{ "configID": "2", "txnID": "", "parentFileName": "GWT_TEST_FILE_2.TXT.tar.gz", "fileName": "GWT_TEST_FILE_2.TXT_2", "eventMessage": "This is a test messsage", "eventMajor": "RAW", "eventMinor": "AC_RAW_REFRESH_LOAD_FAILURE", "exceptionFlag": "Y", "fileHashIdentifier": "asdqwr567456713363vgfdhge456", "Recordcount": "100", "version": "v1", "feedType": "Incremental", "incrementalFromTimestamp": "2022-06-26 00:00:00", "incrementalToTimestamp": "2022-07-25 00:00:00", "extractTimestamp": "2022-07-26 00:00:00", "retryCount": "0", "insertUser": "Acuity", "updateUser": "Acuity", "eventTimeStamp": "2022-07-26 01:24:30" }'

#mapping_dict = {'INGSTN_CONFIG_ID': '2', 'TXN_ID': '', 'PARENT_FILE_NM': 'GWT_TEST_FILE_2.TXT.tar.gz', 'FILE_NM': 'GWT_TEST_FILE_2.TXT_2', 'FILE_STS_DESC': 'AC_RAW_REFRESH_LOAD_FAILURE', 'EXCP_TYP_DESC': 'AC_RAW_REFRESH_LOAD_FAILURE', 'EXCEPTION_FLAG': 'Y', 'FILE_HASH_ID': 'asdqwr567456713363vgfdhge456', 'SRC_ROW_CNT': '100', 'SCHMA_VRSN_ID': 'v1', 'FEED_TYPE': 'Incremental', 'INCRMTL_LOAD_BGN_DTM': '2022-06-26 00:00:00', 'INCRMTL_LOAD_END_DTM': '2022-07-25 00:00:00', 'EXTRCT_DTM': '2022-07-26 00:00:00', 'RETRY_CNT': '0', 'FILE_PRCSS_BEG_DTM': '2022-07-26 01:24:30', 'FILE_PRCSS_END_DTM': '2022-07-26 01:24:30'}

# excp_cfg_dict: dict = {}
# tmp.ins_excp_tbl(mapping_dict, excp_cfg_dict)
#mapping_dict = mapping.mapping_json_to_tbl_slvr_gld()
#tmp.ins_btch_ctrl_dtl_src_tbl(mapping_dict)
#print(tmp.sel_excp_cfg_tbl())

# mapping_dict = {'INGSTN_CONFIG_ID': '1', 'TXN_ID': '123456789', 'PARENT_FILE_NM': 'HMS_DUMMY_FILE.TXT.tar.gz', 'FILE_NM': 'HMS_DUMMY_FILE.TXT', 'FILE_STS_DESC': 'AC_RAW_REFRESH_LOAD_START', 'EXCP_TYP_DESC': 'AC_RAW_REFRESH_LOAD_START', 'EXCEPTION_FLAG': 'Y/N', 'FILE_HASH_ID': 'asdqwr567456713363vgfdhge', 'SRC_ROW_CNT': '100', 'SCHMA_VRSN_ID': 'v1', 'FEED_TYPE': 'Incremental', 'INCRMTL_LOAD_BGN_DTM': '2022-06-26 00:00:00', 'INCRMTL_LOAD_END_DTM': '2022-07-25 00:00:00', 'EXTRCT_DTM': '2022-07-26 00:00:00', 'RETRY_CNT': '0', 'FILE_PRCSS_BEG_DTM': '2022-07-26 01:24:30', 'FILE_PRCSS_END_DTM': '2022-07-26 01:24:30'}

# print(tmp.upsrt_fc_tbl(mapping_dict=mapping_dict))
# #print(tmp.sel_fcs_ref_tbl())
# #fcs_ref_dict = dict(pg.connect_pg.commit_pg_txn(tmp.sel_fcs_ref_tbl()))
#test = dict(pg.connect_pg.commit_pg_txn(tmp.sel_fcs_ref_tbl()))
#tmp_qry = "select max(file_id) from public.file_cntl where file_nm='HMS_TEST_FILE_2.TXT.tar.gz'"
#test = pg.connect_pg.commit_pg_txn(tmp_qry)[0]
#print('test ==> {}'.format(test))
# batch_id_val = 1
# ftch_sts_qry = tmp.select_string.format(columns = 'COUNT(*)', table_name = tmp.btch_ctrl_dtl_tbl) + tmp.where_string.format(conditions = ''.join( ( tmp.batch_id, ' = \'', str(batch_id_val) ,'\' AND UPPER(', tmp.batch_dtl_sts_nm, ') LIKE \'%FAIL%\'' ) ))

#print(ftch_sts_qry)

# #print(tmp.insrt_fcs_tbl(mapping_dict, test ))
# msg_body = { "configID" : "1",  "fileName" : "HMS_DUMMY_FILE.TXT", "eventMessage":"This is a test messsage", "eventMajor":"BRNZ", "eventMinor" : "start/success/failure", "exceptionType" : "technical/server", "exceptionFlag" : "Y/N", "fileHashIdentifier" : "asdqwr567456713363vgfdhge", "Recordcount" : 
# "100", "version" : "v1", "feedType":"Incremental", "incrementalFromTimestamp":"2022-06-26 00:00:00", "incrementalToTimestamp":"2022-07-25 00:00:00", "extractTimestamp" : "2022-07-26 00:00:00", "retryCount" : "0", "insertUser" : "Acuity", "updateUser" : "Acuity", "eventTimeStamp":"2022-07-26 01:24:30" }

# #print(tmp.ins_msg_tbl(msg_body))
