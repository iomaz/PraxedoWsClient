# -- Import hack ----------------------------------------------------------
from pprint import pprint
import os, sys
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

from zeep import helpers as zeepHelper
import pandas as pd

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
    result = praxWsClient.get_bizEvt(['81215384'])
    
    #pprint(result)
    
    print(f'number of biz events: {len(result.entities)}')
    
    # serializing into standard python structure...
    std_result = zeepHelper.serialize_object(result.entities)
    
    # normalizing with pandas...
    print('normalizing with pandas')
    df = pd.json_normalize(std_result[0])
    
    print(df)
    
    
    praxWsClient.close_connection()
    
    print('program end')