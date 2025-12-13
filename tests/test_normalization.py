# -- Import hack ----------------------------------------------------------
import os, sys
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

from datetime import datetime
from pprint import pprint
from zeep import helpers as zeepHelper
import pandas as pd
import sqlite3
import orjson

# local imports
from praxedo_ws.soap_client import PraxedoSoapClient
from praxedo_ws.ws_utility import *

# Praxedo Qual credential
QUAL_CREDENTIAL = PraxedoSoapClient.WsCredential(usr='qua.webservice',
                                                psw='#Qua.webservice-1/*')

WSDL_BIZ_EVT_URL         = "https://eu6.praxedo.com/eTech/services/cxf/v6.1/BusinessEventManager?wsdl"
WSDL_BIZ_EVT_ATTACH_URL  = 'https://eu6.praxedo.com/eTech/services/cxf/v6/BusinessEventAttachmentManager?wsdl'

if __name__ == "__main__":
    
    print('program start')
    
    # creating a new Praxedo web service client
    praxWsClient = PraxedoSoapClient(WSDL_BIZ_EVT_URL,
                                     WSDL_BIZ_EVT_ATTACH_URL,
                                     QUAL_CREDENTIAL)
    
    # opening a connection
    praxWsClient.open_connection()
    
    # requesting a business event
    #get_evt_results = praxWsClient.get_bizEvt(['81215384','81215383','81215382'],PraxedoSoapClient.SRCH_BIZEVT_POPUL_OPT_SET.EXTENDED)
    
    srch_start = datetime.strptime('14/10/25 00:00','%d/%m/%y %H:%M')
    srch_stop = datetime.strptime('14/10/25 23:59','%d/%m/%y %H:%M')
    COMPLETION_DATE = PraxedoSoapClient.DATE_CONSTRAINT.COMPLETION
    CREATION_DATE = PraxedoSoapClient.DATE_CONSTRAINT.CREATION
    LAST_MODIF_DATE = PraxedoSoapClient.DATE_CONSTRAINT.LASTMODIFI
    EXTENDED_RESULTS = PraxedoSoapClient.SRCH_BIZEVT_POPUL_OPT_SET.EXTENDED
    BASIC_RESULTS = PraxedoSoapClient.SRCH_BIZEVT_POPUL_OPT_SET.BASIC


    get_evt_result = praxWsClient.get_bizEvt(['81240673'])
    print('get result:')
    pprint(get_evt_result.entities[0].status)


    srch_evt_results = praxWsClient.search_bizEvts(srch_start, srch_stop,LAST_MODIF_DATE,BASIC_RESULTS) # type: ignore
    
    print(f'srch_evt_results : len = {len(srch_evt_results.entities)}')
    # srch_evt_results.entities[0].completionData.fields = None
    
    for idx, biz_evt in enumerate(srch_evt_results.entities) :
        #if biz_evt.status == 'CANCELED':
        pprint(f'idx:{idx} id: {biz_evt.id} status:{biz_evt.status}')

    #pprint(srch_evt_results.entities[0])

    
    # Ws utility trials
   #  build_core_model_from_ws_result(srch_evt_results)

    # pprint(srch_evt_results.entities)

    # tbl_name = 'response_table'
    
    # writing the result to a csv file
    # json_df.to_csv(f'{tbl_name}.csv')
    
    # writing the result to a text file
    # with open(f'{tbl_name}.txt', "w", encoding="utf-8") as file:
    #    file.write(json_df.to_string())
    # print(df.to_string())
    
    # writing the dataframe to a sqlite table
    #with sqlite3.connect(f'{tbl_name}.sqlite3') as conn:
    #    json_df.to_sql(tbl_name,conn,if_exists='replace',index=False) # type: ignore
    
    
    print('program end')