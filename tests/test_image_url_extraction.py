# -- Import hack ----------------------------------------------------------
import os, sys
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

##  https://jsonpath.com/ ##

from datetime import datetime
from pprint import pprint
from zeep import helpers as zeepHelper
import pandas as pd


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
    praxWsClient.open_session(PROD_AUTH)
    
    # requesting a business event
    get_wo_results = praxWsClient.get_work_orders(['81313635'],PraxedoSoapClient.GET_WO_RESULT_OPTION.EXTENDED)
    
    normalize_ws_response(get_wo_results)
    
    
    print('program end')