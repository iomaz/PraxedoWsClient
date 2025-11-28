
from enum import Enum, unique


WS_BIZ_EVT_WSDL_URL         = "https://eu6.praxedo.com/eTech/services/cxf/v6.1/BusinessEventManager?wsdl"
WS_BIZ_EVT_ATTACH_WSDL_URL  = 'https://eu6.praxedo.com/eTech/services/cxf/v6/BusinessEventAttachmentManager?wsdl'

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