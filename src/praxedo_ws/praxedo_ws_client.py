from typing import NamedTuple
import time
import requests
from requests import Session
from requests.auth import HTTPBasicAuth
from zeep import Client
from zeep.transports import Transport
from datetime import datetime
from enum import Enum
import warnings



def get_url_content(arg_url):
    with warnings.catch_warnings():
        MAX_RETRY = 2
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
                    if retryCount >= MAX_RETRY : 
                        print('Max retry errors : return None...')
                        return None
                    else : 
                        print('get_url_content():Err 429 - too many requests- wait and retry...')
                        time.sleep(5)
        
    return req_result.content


class PraxedoWSClient:
     
    class WsCredential(NamedTuple):
        usr : str
        psw : str
     
    def __init__(self, biz_evt_wsdl_url:str, biz_attach_wsdl_url:str, ws_credential_arg : WsCredential):
    
        self.biz_evt_wsdl_url       = biz_evt_wsdl_url
        self.biz_attach_wsdl_url    = biz_attach_wsdl_url
        self.ws_credential          = ws_credential_arg
        
        self.http_session               : Session
        self.bizEvt_tranport            : Transport
        self.bizEvt_client              : Client # zeep client for business events management
        self.bizEvt_attach_transport    : Transport
        self.bizEvt_attach_client       : Client # zeep client for business event attachement management
        
        
        self.http_session.verify  = False
        self.searchAbort          = False
    
    def open_connection(self):
        """ Connect to the the service endpoint using the Zeep lib
        """
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            
            # authentication
            self.http_session = Session()
            self.http_session.auth = HTTPBasicAuth(self.ws_credential.usr, self.ws_credential.psw)  # Qual
                
            self.bizEvt_tranport            = Transport(session = self.http_session)
            self.bizEvt_attach_transport    = Transport(session = self.http_session)
                
            self.bizEvt_client         = Client(wsdl = self.biz_evt_wsdl_url,    transport = self.bizEvt_tranport)
            self.bizEvt_attach_client  = Client(wsdl = self.biz_attach_wsdl_url, transport = self.bizEvt_attach_transport)
    
    
    def close_connection(self):
        self.http_session.close()
        
    
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
    
    
    
    def list_attachments(self,arg_evt_ext_id):
        # print('ws_list_attachments()...')
    
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            list_attach_result = self.bizEvt_attach_client.service.listAttachments(arg_evt_ext_id)
        
        return list_attach_result.entities
        
    
    def get_attachement_content(self,arg_evt_attach_id):
        #print('ws_getAttachmentContent()...')
        
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            get_attach_content_result= self.bizEvt_attach_client.service.getAttachmentContent(arg_evt_attach_id)
    
    
        return get_attach_content_result.content
    
        #print(singleBisEvtAttach)
        
        #with open('./'+ aFileName, 'wb') as file:
        #   file.write(singleBisEvtAttach.content)
    
    
    GET_BIZEVT_RESULT_CODE =   {
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
    
    
    class GET_BIZEVT_POPUL_OPT_SET(Enum):

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
    
    
    
    def get_bizEvt(self,evt_id_list : list[str], arg_populate_opt = GET_BIZEVT_POPUL_OPT_SET.BASIC):

        # evt_id_list_arg = evt_id_list # [f"{arg_evt_id_str}"]
        
        populate_opt_arg =  arg_populate_opt.value
        
        print('Calling the getEvents service ...')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            get_evt_result = self.bizEvt_client.service.getEvents(evt_id_list,populate_opt_arg)

        return get_evt_result
    
    
    class SRCH_BIZEVT_POPUL_OPT_SET(Enum):

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
    
    
    class SRCH_BIZEVT_RET_CODE(Enum):    
        SUCESS                  =  0 
        INTERNAL_ERROR          =  1
        MISSING_REQUEST_PARAM   =  151
        MISSING_DATE_INPUT      =  152
        INCOMPLETE_DATE_INPUT   =  153
        ONE_DATE_INPUT_INVALID  =  154
        RESULTS_GREATER_1000    =  155
        UNKNOWN_CONTRACT        =  157
        PARTIAL_RESULT          =  200
                                 
    
    def abort_search_bizEvts(self):
        self.searchAbort = True
    
    
    def search_bizEvts(self, arg_start_date:datetime, arg_stop_date:datetime,
                            arg_date_constraint:DATE_CONSTRAINT, 
                            arg_populate_opt=SRCH_BIZEVT_POPUL_OPT_SET.BASIC):
        
        
        MAX_RESULTS_PER_PAGE = 50  #This is the actual maximum limit allowed by Praxedo
        print(f'ws_serch_evts()')
        
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
            
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
               
                search_results = self.bizEvt_client.service.searchEvents(requestArg,MAX_RESULTS_PER_PAGE,first_result_idx,populate_opt_arg)
            
            return_code = PraxedoWSClient.SRCH_BIZEVT_RET_CODE(search_results.resultCode)
            match return_code : 
                case PraxedoWSClient.SRCH_BIZEVT_RET_CODE.SUCESS :
                    total_entities_results += search_results.entities
                    search_results.entities = total_entities_results
                    break
                
                case PraxedoWSClient.SRCH_BIZEVT_RET_CODE.PARTIAL_RESULT :
                    if not self.searchAbort :
                        resp_page_nbr += 1
                        first_result_idx += MAX_RESULTS_PER_PAGE # incrementing the first index for a multipage result 
                        total_entities_results += search_results.entities
                        continue    
                    else :
                        print('ws_searchEvents aborted !')
                        total_entities_results += search_results.entities
                        search_results.entities = total_entities_results
                        self.searchAbort = False
                        break
                
                case _: # in case of an error
                    print(f'searchEvents() service returned an error: {return_code} ')
                    break

        return search_results
    
    
    
    