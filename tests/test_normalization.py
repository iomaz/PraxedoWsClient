# -- Import hack ----------------------------------------------------------
import os, sys
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

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
    result = praxWsClient.get_bizEvt(['81215384','81215383','81215382'])
    
    pprint(result.entities)
    
    print(f'number of biz events: {len(result.entities)}')
    
    # serializing into standard python structure...
    pyObj_result = zeepHelper.serialize_object(result.entities)
    
    # converting into json
    #json_result = str(orjson.dumps(pyObj_result),'utf-8')
    
    #pprint(json_result)
    
    # normalizing with pandas...
    print('normalizing with pandas')
    df = pd.json_normalize(pyObj_result,max_level=2) # type: ignore
    #print(df.to_string())
    
    # serialize all df column into json
    json_df = df.map(lambda value : str(orjson.dumps(value, default= lambda x : 'None'),'utf-8'))
    
    
    tbl_name = 'response_table'
    
    # writing the result to a csv file
    json_df.to_csv(f'{tbl_name}.csv')
    
    # writing the result to a text file
    with open(f'{tbl_name}.txt', "w", encoding="utf-8") as file:
        file.write(json_df.to_string())
    # print(df.to_string())
    
    # writing the dataframe to a sqlite table
    #with sqlite3.connect(f'{tbl_name}.sqlite3') as conn:
        #conn.execute(f'DROP TABLE IF EXISTS {tbl_name}')
        #conn.commit()
        # dtype_dict = {col: 'TEXT' for col in df.columns}
        #df.to_sql(tbl_name,conn,if_exists='replace',index=False) # type: ignore
    
    
    print('program end')