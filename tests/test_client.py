# -- Import hack ----------------------------------------------------------
import os, sys
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

from enum import Enum, unique
from pprint import pprint

# local imports
from praxedo_ws.soap_client.soap_client import PraxedoSoapClient



PRAX_BIZ_EVT_WSDL_URL         = "https://eu6.praxedo.com/eTech/services/cxf/v6.1/BusinessEventManager?wsdl"
PRAX_BIZ_EVT_ATTACH_WSDL_URL  = 'https://eu6.praxedo.com/eTech/services/cxf/v6/BusinessEventAttachmentManager?wsdl'


# Praxedo Qual credential
QUAL_PRAX_AUTH = PraxedoSoapClient.WsCredential(usr='qua.webservice',
                                                psw='#Qua.webservice-1/*')

PROD_PRAX_AUTH = PraxedoSoapClient.WsCredential(usr='WSDEM',
                                                psw='WsdemWsdem2358')

PROD_PRAX_AUTH = PraxedoSoapClient.WsCredential(usr='WSDEM2',
                                                psw='WsdemWsdem2358')


if __name__ == "__main__":
    
    print('program start')
    
    # creating a new Praxedo web service client
    praxWsClient = PraxedoSoapClient(PRAX_BIZ_EVT_WSDL_URL,PRAX_BIZ_EVT_ATTACH_WSDL_URL,QUAL_PRAX_AUTH)
    
    # opening a connection
    praxWsClient.open_connection()
    
    # requesting a business event
    result = praxWsClient.get_bizEvt(['81215384'])
    
    # printing the result
    pprint(result)
    
    # closing the connection
    praxWsClient.close_connection()

    print('program end')

