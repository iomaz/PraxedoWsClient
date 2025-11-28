
from enum import Enum, unique
from pprint import pprint


# local imports
from src.praxedo_ws.praxedo_ws_client import *

@unique
class EVT_STATUS(Enum):
    NEW             = 0
    QUALIFIED       = 1
    PRE_SCHEDULED   = 2
    SCHEDULED       = 3
    IN_PROGRESS     = 4
    COMPLETED       = 5
    VALIDATED       = 6
    CANCELLED       = 7


PRAX_BIZ_EVT_WSDL_URL         = "https://eu6.praxedo.com/eTech/services/cxf/v6.1/BusinessEventManager?wsdl"
PRAX_BIZ_EVT_ATTACH_WSDL_URL  = 'https://eu6.praxedo.com/eTech/services/cxf/v6/BusinessEventAttachmentManager?wsdl'


# Praxedo Qual credential
QUAL_PRAX_AUTH = PraxedoWSClient.WsCredential(usr='qua.webservice',
                                              psw='#Qua.webservice-1/*')

QUAL_WS_USER = {
                    'usr':'qua.webservice',
                    'psw':'#Qua.webservice-1/*'
                }

PROD_WS_USER = {
                    'usr':'WSDEM',
                    'psw':'WsdemWsdem2358'
                }

PROD_WS_USER2 = {
                    'usr':'WSDEM2',
                    'psw':'WsdemWsdem2358'
                }


if __name__ == "__main__":
    
    print('program start')
    
    # creating a new Praxedo web service client
    praxWsClient = PraxedoWSClient(PRAX_BIZ_EVT_WSDL_URL,PRAX_BIZ_EVT_ATTACH_WSDL_URL,QUAL_PRAX_AUTH)
    
    # opening a connection
    praxWsClient.open_connection()
    
    # requesting a business event
    result = praxWsClient.get_bizEvt(['81215384'])
    
    # printing the result
    pprint(result)
    
    # closing the connection
    praxWsClient.close_connection()

    print('program end')

