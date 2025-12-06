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

# Praxedo Qual credential
QUAL_PRAX_AUTH = PraxedoSoapClient.WsCredential(usr='qua.webservice',
                                                psw='#Qua.webservice-1/*')

PRAX_BIZ_EVT_WSDL_URL         = "https://eu6.praxedo.com/eTech/services/cxf/v6.1/BusinessEventManager?wsdl"
PRAX_BIZ_EVT_ATTACH_WSDL_URL  = 'https://eu6.praxedo.com/eTech/services/cxf/v6/BusinessEventAttachmentManager?wsdl'

if __name__ == "__main__":
    
    print('program start')
    
    # creating a new Praxedo web service client
    praxWsClient = PraxedoSoapClient(PRAX_BIZ_EVT_WSDL_URL,PRAX_BIZ_EVT_ATTACH_WSDL_URL,QUAL_PRAX_AUTH)
    
    # opening a connection
    praxWsClient.open_connection()
    
    # requesting a business event
    #get_evt_results = praxWsClient.get_bizEvt(['81215384','81215383','81215382'],PraxedoSoapClient.SRCH_BIZEVT_POPUL_OPT_SET.EXTENDED)
    
    srch_start = datetime.strptime('06/11/25','%d/%m/%y')
    srch_stop = datetime.strptime('07/11/25','%d/%m/%y')
    DATE_CONSTRAINT = PraxedoSoapClient.DATE_CONSTRAINT
    SRCH_POPU_OPT = PraxedoSoapClient.SRCH_BIZEVT_POPUL_OPT_SET
    srch_evt_results = praxWsClient.search_bizEvts(srch_start, srch_stop,DATE_CONSTRAINT.COMPLETION,SRCH_POPU_OPT.EXTENDED) # type: ignore
    
    print(f'srch_evt_results : len = {len(srch_evt_results.entities)}')
    
    pprint(srch_evt_results.entities)
    
    
    # pprint(srch_evt_results.entities)
    
    print(f'number of biz events in response : {len(srch_evt_results.entities)}')
    
    # serializing into standard python structure...
    pyObj_result = zeepHelper.serialize_object(srch_evt_results.entities)
    
    # converting into json
    #json_result = str(orjson.dumps(pyObj_result),'utf-8')
    
    #pprint(json_result)
    
    # normalizing with pandas...
    print('normalizing with pandas')
    df = pd.json_normalize(pyObj_result,max_level=2) # type: ignore
    #print(df.to_string())
    
    # serialize all df column values into json
    json_df = df.map(lambda value : orjson.dumps(value, default= lambda val : 'None').decode('utf-8').strip('"'))
    
    
    tbl_name = 'response_table'
    
    # writing the result to a csv file
    # json_df.to_csv(f'{tbl_name}.csv')
    
    # writing the result to a text file
    with open(f'{tbl_name}.txt', "w", encoding="utf-8") as file:
        file.write(json_df.to_string())
    # print(df.to_string())
    
    # writing the dataframe to a sqlite table
    with sqlite3.connect(f'{tbl_name}.sqlite3') as conn:
        json_df.to_sql(tbl_name,conn,if_exists='replace',index=False) # type: ignore
    
    
    print('program end')