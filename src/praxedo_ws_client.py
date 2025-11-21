import time
import requests
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport
from datetime import datetime
from enum import Enum, unique
import warnings
from concurrent.futures import ThreadPoolExecutor


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



class PraxedoWS:
    
    WS_BIZ_EVT_WSDL_URL         = "https://eu6.praxedo.com/eTech/services/cxf/v6.1/BusinessEventManager?wsdl"
    WS_BIZ_EVT_ATTACH_WSDL_URL  = 'https://eu6.praxedo.com/eTech/services/cxf/v6/BusinessEventAttachmentManager?wsdl'
    
    zeep_ws_session : Client # zeep client
    zeep_ws_session2 : Client
    
    zeepAttachServices : Client
    
    WS_SEARCH_MAX_RESULTS_PER_PAGE = 50  #This is the maximum allowed by Praxedo

    class DATE_CONSTRAINT(Enum):
        CREATION        =   'creationDate'
        COMMUNICA       =   'communicationDate'
        APPOINT         =   'appointmentDate'
        SCHEDULING      =   'schedulingDate'
        LOADING         =   'pdaLoadingDate'
        UNLOADING       =   'pdaUnloadingDate'
        STARTING        =   'startDate'
        COMPLETION      =   'completionDate'
        VALIDATION      =   'validationDate'
        LASTMODIFI      =   'lastModificationDate'

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
    

    #organisation ID
    ORG_ID = {
                1000:   'MNE',
                1008:   'MCE',
                1009:   'MBR',
                1010:   'MSU'
            }

    
    
    session = Session()
    session2 = Session()
    session.verify = False
    session2.verify = False
    
    
    searchAbort = False
    
    @classmethod
    def connectToEndPoint(cls,aEndPointCodeStr):
        """ Connect to the the service endpoint using the Zeep lib
        Args:
            aEndPointStr (_type_): _description_
        """
        print(f'connectToEndPoint() : EndPoint = {aEndPointCodeStr}')
        
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            
            if aEndPointCodeStr == 'QUAL':
                #GUI.printOutputTrace('connectToEndPoint()... Qual env. selected')
                cls.session.auth = HTTPBasicAuth(QUAL_WS_USER["usr"], QUAL_WS_USER["psw"])  # Qual
                
            if aEndPointCodeStr == 'PROD':
                #GUI.printOutputTrace('connectToEndPoint()... Prod env. selected')
                cls.session.auth  = HTTPBasicAuth(PROD_WS_USER["usr"], PROD_WS_USER["psw"]) # Prod user 1
                cls.session2.auth = HTTPBasicAuth(PROD_WS_USER2["usr"], PROD_WS_USER2["psw"]) # Prod user 2
                cls.zeep_ws_session2  = Client(wsdl=cls.WS_BIZ_EVT_WSDL_URL, transport=Transport(session=cls.session2))

            cls.zeep_ws_session   = Client(wsdl=cls.WS_BIZ_EVT_WSDL_URL, transport=Transport(session=cls.session))
            cls.zeepAttachServices = Client(wsdl=cls.WS_BIZ_EVT_ATTACH_WSDL_URL, transport=Transport(session=cls.session))
    
    
    @classmethod
    def ws_list_attachments(cls,arg_evt_ext_id):
        # print('ws_list_attachments()...')
    
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            list_attach_result = cls.zeepAttachServices.service.listAttachments(arg_evt_ext_id)
        
        return list_attach_result.entities
        
    
    @classmethod
    def ws_getAttachmentContent(cls,arg_evt_attach_id):
        #print('ws_getAttachmentContent()...')
        
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            get_attach_content_result= cls.zeepAttachServices.service.getAttachmentContent(arg_evt_attach_id)
    
    
        return get_attach_content_result.content
    
        #print(singleBisEvtAttach)
        
        #with open('./'+ aFileName, 'wb') as file:
        #   file.write(singleBisEvtAttach.content)
    
    
    WS_GETEVENT_RESULT_CODE =   {
                                0: 'SUCCESS',
                                1: 'INTERNAL ERROR',
                                50: 'UNCOMPLETE REQUEST',
                                51 :'TOO MANY EVENT REQUESTED',
                                157:'UNKNOWN CONTRACT'
                            }
    
    
    """
        **** 21x possible options : see : https://support.praxedo.com/hc/fr/articles/115004095289-Gestion-des-interventions#getevents
        businessEvent.populate.coreData        
        businessEvent.populate.coreData.referentialData
        businessEvent.populate.coreData.serviceOrderPosition
        businessEvent.populate.qualificationData
        businessEvent.populate.qualificationData.reopening
        businessEvent.populate.qualificationData.extendedExpectedItems
        businessEvent.populate.schedulingData
        businessEvent.populate.schedulingData.appointmentDate
        businessEvent.populate.schedulingData.populateInQualifiedStatus
        businessEvent.populate.completionData.fields
        businessEvent.populate.completionData.items
        businessEvent.populate.completionData.extendedItems
        businessEvent.populate.completionData.items.customAttributes
        businessEvent.populate.completionData.lifeCycleDate
        businessEvent.populate.completionData.excludeBinaryData
        businessEvent.populate.annotations
        businessEvent.populate.contractData
        businessEvent.populate.annotations.withReportFields
        businessEvent.populate.annotations.withCode
        extendedLastModificationDate
        businessEvent.feature.status.cancelled 
        """
    
    
    class WS_GET_EVTS_POPUL_OPT_SET(Enum):

        class OPTIONS(Enum):
            prefix = 'businessEvent.populate.'
            COREDATA_1     = f'{prefix}coreData'
            QUALIDATA_1    = f'{prefix}qualificationData'
            SCHEDUDATA_1   = f'{prefix}schedulingData'
            COMPLDATA_1    = f'{prefix}completionData.lifeCycleDate'
            COMPLDATA_2    = f'{prefix}completionData.fields'
            COMPLDATA_3    = f'{prefix}completionData.excludeBinaryData' 
            

        BASIC   = [ {'key': OPTIONS.COREDATA_1.value},
                    {'key': OPTIONS.QUALIDATA_1.value},
                    {'key': OPTIONS.SCHEDUDATA_1.value},
                    {'key': OPTIONS.COMPLDATA_1.value}
                 ]
        
        EXTENDED = BASIC.copy() + [
                    {'key': OPTIONS.COMPLDATA_2.value},
                    {'key': OPTIONS.COMPLDATA_3.value}
                                  ]
    
    
    
    @classmethod
    def ws_get_evt(cls,arg_evt_id_list, arg_populate_opt = WS_GET_EVTS_POPUL_OPT_SET.BASIC):

        evt_id_list_arg = arg_evt_id_list # [f"{arg_evt_id_str}"]
        
        populate_opt_arg =  arg_populate_opt.value
        
        print('Calling the getEvents service ...')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            get_evt_result = cls.zeep_ws_session.service.getEvents(evt_id_list_arg,populate_opt_arg)

        return get_evt_result
    
    
    class WS_SRCH_EVTS_POPUL_OPT_SET(Enum):

        """
        **** 20x possible options : see : https://support.praxedo.com/hc/fr/articles/115004095289-Gestion-des-interventions#searchevents
        businessEvent.populate.coreData        
        businessEvent.populate.coreData.referentialData
        businessEvent.populate.coreData.serviceOrderPosition
        businessEvent.populate.qualificationData
        businessEvent.populate.qualificationData.reopening
        businessEvent.populate.qualificationData.extendedExpectedItems
        businessEvent.populate.schedulingData
        businessEvent.populate.schedulingData.appointmentDate
        businessEvent.populate.schedulingData.populateInQualifiedStatus
        businessEvent.populate.completionData.cancellation
        businessEvent.populate.completionData.fields
        businessEvent.populate.completionData.items
        businessEvent.populate.completionData.extendedItems
        businessEvent.populate.completionData.items.customAttributes
        businessEvent.populate.completionData.lifeCycleDate
        businessEvent.populate.completionData.excludeBinaryData
        businessEvent.populate.annotations
        businessEvent.populate.annotations.withReportFields
        businessEvent.populate.annotations.withCode
        extendedLastModificationDate 
        """
        class OPTIONS(Enum):
            prefix = 'businessEvent.populate.'
            COREDATA_1     = f'{prefix}coreData'
            QUALIDATA_1    = f'{prefix}qualificationData'
            SCHEDUDATA_1   = f'{prefix}schedulingData'
            COMPLDATA_1    = f'{prefix}completionData.lifeCycleDate'
            COMPLDATA_2    = f'{prefix}completionData.fields'
            COMPLDATA_3    = f'{prefix}completionData.excludeBinaryData' 
            

        BASIC   = [ {'key': OPTIONS.COREDATA_1.value},
                    {'key': OPTIONS.QUALIDATA_1.value},
                    {'key': OPTIONS.SCHEDUDATA_1.value},
                    {'key': OPTIONS.COMPLDATA_1.value}
                 ]
        
        EXTENDED = BASIC.copy() + [
                    {'key': OPTIONS.COMPLDATA_2.value},
                    {'key': OPTIONS.COMPLDATA_3.value}
                                  ]
    
    
    class WS_SRCH_EVT_RET_CODE(Enum):    
        SUCESS                  =  0 
        INTERNAL_ERROR          =  1
        MISSING_REQUEST_PARAM   =  151
        MISSING_DATE_INPUT      =  152
        INCOMPLETE_DATE_INPUT   =  153
        ONE_DATE_INPUT_INVALID  =  154
        RESULTS_GREATER_1000    =  155
        UNKNOWN_CONTRACT        =  157
        PARTIAL_RESULT          =  200
                                 
    
    
    @classmethod
    def ws_search_evts(cls, arg_start_date:datetime, arg_stop_date:datetime,
                            arg_date_constraint:DATE_CONSTRAINT, 
                            arg_populate_opt=WS_SRCH_EVTS_POPUL_OPT_SET.BASIC,
                            arg_zeep_session_no = 1):
        
        print(f'ws_serch_evts() sessionNo={arg_zeep_session_no}')
        
        requestArg =  {
                            "typeConstraint"   :   [],
                            "dateConstraints"  :   [
                                        {"name": arg_date_constraint.value,
                                        "dateRange":[arg_start_date.isoformat(),arg_stop_date.isoformat()] 
                                        }
                                                    ],
                            
                            "agentIdConstraint" : [],
                            "statusConstraint " : [],
                            "serviceOrderConstraint" : ""
                        }


        populate_opt_arg = arg_populate_opt.value
        
        
        resp_page_nbr      = 1
        first_result_idx   = 0
        total_entities_results = []
        
        # iterate over a multi-page response when applicable
        while True:
            
            print(f'\r s({arg_zeep_session_no}) ws_search_evts: querying page:{resp_page_nbr}',end='',flush=True)
            #print(f's({arg_zeep_session_no}) ws_search_evts: querying page:{resp_page_nbr}')
            
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                #current_session = None
                match arg_zeep_session_no:
                    case 1 : current_session = cls.zeep_ws_session
                    case 2:  current_session = cls.zeep_ws_session2
                search_results = current_session.service.searchEvents(requestArg,cls.WS_SEARCH_MAX_RESULTS_PER_PAGE,first_result_idx,populate_opt_arg)
            
            return_code = cls.WS_SRCH_EVT_RET_CODE(search_results.resultCode)
            match return_code : 
                case cls.WS_SRCH_EVT_RET_CODE.SUCESS :
                    total_entities_results += search_results.entities
                    search_results.entities = total_entities_results
                    break
                
                case cls.WS_SRCH_EVT_RET_CODE.PARTIAL_RESULT :
                    if not cls.searchAbort :
                        time.sleep(1) # intended to give the oportunity to the system to switch to another thread... 
                        resp_page_nbr += 1
                        first_result_idx += cls.WS_SEARCH_MAX_RESULTS_PER_PAGE # incrementing the first index for a multipage result 
                        total_entities_results += search_results.entities
                        continue    
                    else :
                        print('ws_searchEvents aborted !')
                        total_entities_results += search_results.entities
                        search_results.entities = total_entities_results
                        cls.searchAbort = False
                        break
                
                case _: # in case of an error
                    print(f'searchEvents() service returned an error: {return_code} ')
                    break

        return search_results
    
    @classmethod
    def parallel_ws_search_evts(cls,argStartDate, argStopDate, arg_date_constraint, arg_populate_opt):
        
        print('parallel_ws_search_evts() begin')
        
        time_shift = (argStopDate - argStartDate) / 2
        period_split = [(argStartDate,argStartDate + time_shift),(argStartDate + time_shift,argStopDate)]
        
        print(period_split)
        
        search_args = [(period[0], period[1], arg_date_constraint, arg_populate_opt,idx+1)  for idx, period in enumerate(period_split)]
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda p: PraxedoWS.ws_search_evts(*p),search_args))
        
        results[0].entities.extend(results[1].entities)
        
        return results[0]
    
    
    @classmethod
    def get_url_content(cls,arg_url):
        with warnings.catch_warnings():
            MAX_RETRY = 1
            retryCount = 0
            warnings.simplefilter('ignore')
            while retryCount <= 1 :   
                req_result = requests.get(arg_url,verify=False)
                retry = False
                match req_result.status_code:
                    case 200 : break # fine
                    case _ if req_result.status_code != 429 :
                        print(f"Failed to download !: ErrCode:{req_result.status_code} Reason={req_result.reason} (url)={arg_url[-38:]}")
                        return None
                    case 429 : # too many requests
                        retryCount += 1
                        if retryCount >= 2 : 
                            print('two many 429 errors : return None...')
                            return None
                        else : 
                            print('get_url_content():Err 429 - too many requests- Retry...')
                            time.sleep(5)
          
        return req_result.content
    