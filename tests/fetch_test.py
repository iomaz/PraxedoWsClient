import os, sys
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

# local imports
from praxedo_ws.soap import PraxedoSoapClient
from praxedo_ws.utility import *


PROD_USER = PraxedoSoapClient.WsCredential(usr='WSDEM',
                                            psw='WsdemWsdem2358')

praxedoWS = PraxedoSoapClient()
praxedoWS.connect(PROD_USER)


def fetch_full_week_work_orders(arg_week_nbr:int, arg_year:int):
    
    # getting all search periods
    week_days_period = get_week_days(arg_week_nbr, arg_year)

    EXTENDED_RESULT = PraxedoSoapClient.SRCH_WO_RESULT_OPTION.EXTENDED
    WO_COMPLETED    = PraxedoSoapClient.DATE_CONSTRAINT.COMPLETION
    # searching and fetching each day and get the 
    for day_start, day_stop in week_days_period:
        
        wo_entities = praxedoWS.search_work_orders(WO_COMPLETED,day_start, day_stop,EXTENDED_RESULT) # type: ignore


