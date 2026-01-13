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
from praxedo_ws.soap import PraxedoSoapClient
from praxedo_ws.utility import *

# Praxedo Qual credential
QUAL_AUTH = PraxedoSoapClient.UserCredential(usr='qua.webservice',
                                           psw='#Qua.webservice-1/*')

PROD_AUTH = PraxedoSoapClient.UserCredential(usr='WSDEM',
                                           psw='WsdemWsdem2358')

if __name__ == "__main__":
    
    print('program start')
    
    # creating a new Praxedo web service client
    praxWsClient = PraxedoSoapClient()
    
    # opening a connection
    praxWsClient.connect(QUAL_AUTH)
    
    # requesting a business event
    #get_evt_results = praxWsClient.get_bizEvt(['83007173'],PraxedoSoapClient.SRCH_BIZEVT_POPUL_OPT_SET.EXTENDED)
    
    #pprint(get_evt_results.entities[0].completionData)
    
    #pyobj_fields = zeepHelper.serialize_object(get_evt_results.entities[0].completionData.fields)
    
    #with open(f'fields.json', "w", encoding="utf-8") as file:
    #    file.write(orjson.dumps(pyobj_fields).decode('utf-8'))
    
    #week52_2024    = ws_utility.get_week_sequence(52,2024)
    last_2025_week  = ws_utility.get_week_days_sequence(52,2025)
    
    print(f'last 2025 week : {last_2025_week}')
    
    
    exit()
    
    srch_start = datetime.strptime('10/07/25 9:30','%d/%m/%y %H:%M')
    srch_stop = datetime.strptime('10/07/25 9:40','%d/%m/%y %H:%M')
    COMPLETION_DATE = PraxedoSoapClient.DATE_CONSTRAINT.COMPLETION
    CREATION_DATE = PraxedoSoapClient.DATE_CONSTRAINT.CREATION
    EXTENDED_RESULTS = PraxedoSoapClient.SRCH_WO_RESULT_OPTION.EXTENDED
    srch_evt_results = praxWsClient.search_work_orders(srch_start, srch_stop,CREATION_DATE,EXTENDED_RESULTS) # type: ignore
    
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