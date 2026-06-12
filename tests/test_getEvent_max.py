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
QUAL_USER = PraxedoSoapClient.UserCredential(usr='qua.webservice',
                                                psw='#Qua.webservice-1/*')

PROD_USER = PraxedoSoapClient.UserCredential(usr='WSDEM',
                                                psw='WsdemWsdem2358')

PROD_USER2 = PraxedoSoapClient.UserCredential(usr='WSDEM2',
                                                psw='WsdemWsdem2358')


if __name__ == "__main__":
    print('program begins...')

    # creating a new Praxedo web service client
    praxWsClient = PraxedoSoapClient()
    
    # opening a connection
    praxWsClient.open_session(PROD_USER)

    # generate a list of n workorder numbers starting
    range_start = 81040000
    range_size  = 50

    int_range = range(range_start, range_start + range_size)
    wo_list = [str(nbr) for nbr in int_range]

    print(wo_list)

    result = praxWsClient.get_work_orders(wo_list)

    print(f'request result size : {len(result)}')


    print('program ends...')