# -- Import hack ----------------------------------------------------------
import os, sys
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

from enum import Enum
from pprint import pprint

# local imports
from praxedo_ws.soap import PraxedoSoapClient


# Praxedo Qual credential
QUAL_USER1 = PraxedoSoapClient.UserCredential(usr='qua.webservice',
                                                psw='#Qua.webservice-1/*')


QUAL_USER2 = PraxedoSoapClient.UserCredential(usr='qua.webservice2',
                                           psw='#Qua.webservice-2/*')


PROD_USER = PraxedoSoapClient.UserCredential(usr='WSDEM',
                                                psw='WsdemWsdem2358')

PROD_USER2 = PraxedoSoapClient.UserCredential(usr='WSDEM2',
                                                psw='WsdemWsdem2358')


if __name__ == "__main__":
    print('program begins...')

    # creating a new Praxedo web service client
    praxWsClient = PraxedoSoapClient()
    
    # opening a connection
    praxWsClient.open_session(QUAL_USER1, QUAL_USER2)


    result = praxWsClient.create_work_order('ZP27')

    print('result:')
    print(result)

    praxWsClient.close_session()

    print('program ends...')