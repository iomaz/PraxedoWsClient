# -- Import hack ----------------------------------------------------------
from datetime import datetime
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
    
    print('program start')
    
    # creating a new Praxedo web service client
    praxWsClient = PraxedoSoapClient()
    
    # opening a connection
    praxWsClient.open_session(QUAL_USER)
    
    # requesting a bsiness event
    #result = praxWsClient.get_bizEvt(['81215384'])
    srch_from = datetime.strptime('01/09/26 12:00','%d/%m/%y %H:%M')
    srch_to   = datetime.strptime('06/09/26 14:00','%d/%m/%y %H:%M')
    
    COMPLETION_DATE = PraxedoSoapClient.DATE_CONSTRAINT.COMPLETION
    EXTENDED_RESULT = PraxedoSoapClient.SRCH_WO_RESULT_OPTION.EXTENDED

    for countIdx in range(0,100) :
        curr_rate = praxWsClient.count_hour_call() 
        print(f'current rate = {curr_rate}')
        result = praxWsClient.search_work_orders(COMPLETION_DATE,srch_from, srch_to,EXTENDED_RESULT)  # type: ignore

    # printing the result
    pprint(f'total wo nbr = {len(result)}')

    #pprint('result:')
    #pprint(result)
    
    # closing the connection
    praxWsClient.close_session()

    print('program end')

