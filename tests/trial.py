# -- Import hack ----------------------------------------------------------
import os, sys
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

from pprint import pprint

# local imports
from praxedo_ws.soap_client import PraxedoSoapClient

if __name__ == "__main__":
    print('program start')
    
    print(PraxedoSoapClient.GET_BIZEVT_RESULT_CODE.SUCCESS.name)
    
    print('program end')