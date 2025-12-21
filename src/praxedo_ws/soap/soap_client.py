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


class PraxedoSoapClient:
     
    class DEFAULT_WSDL(NamedTuple):
        BIZ_EVT         = "https://eu6.praxedo.com/eTech/services/cxf/v6.1/BusinessEventManager?wsdl"
        BIZ_EVT_ATTACH  = 'https://eu6.praxedo.com/eTech/services/cxf/v6/BusinessEventAttachmentManager?wsdl'

    class WsCredential(NamedTuple):
        usr : str
        psw : str
     
    def __init__(self,  biz_evt_wsdl_url:str    = DEFAULT_WSDL.BIZ_EVT,
                        biz_attach_wsdl_url:str = DEFAULT_WSDL.BIZ_EVT_ATTACH):
        
        self.biz_evt_wsdl_url       = biz_evt_wsdl_url
        self.biz_attach_wsdl_url    = biz_attach_wsdl_url
        
        self.ws_credential              : PraxedoSoapClient.WsCredential
        self.http_session               : Session
        self.bizEvt_tranport            : Transport
        self.bizEvt_client              : Client # zeep client for business events management
        self.bizEvt_attach_transport    : Transport
        self.bizEvt_attach_client       : Client # zeep client for business event attachement management
        
        self.searchAbort          = False
    
    def connect(self,ws_credential_arg : WsCredential):
        """ Connect to the the service endpoint using the Zeep lib
        """

        self.ws_credential = ws_credential_arg

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            
            # authentication
            self.http_session           = Session()
            self.http_session.verify    = False
            self.http_session.auth      = HTTPBasicAuth(self.ws_credential.usr, self.ws_credential.psw)  # Qual
                
            self.bizEvt_tranport            = Transport(session = self.http_session)
            self.bizEvt_attach_transport    = Transport(session = self.http_session)
                
            self.bizEvt_client         = Client(wsdl = self.biz_evt_wsdl_url,    transport = self.bizEvt_tranport)
            self.bizEvt_attach_client  = Client(wsdl = self.biz_attach_wsdl_url, transport = self.bizEvt_attach_transport)
    
    
    def close_connection(self):
        self.http_session.close()
        
    
    class DATE_CONSTRAINT(NamedTuple):
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
    
    
    class WORK_ORDER_STATUS(Enum):
        NEW             = 0
        QUALIFIED       = 1
        PRE_SCHEDULED   = 2
        SCHEDULED       = 3
        IN_PROGRESS     = 4
        COMPLETED       = 5
        VALIDATED       = 6
        CANCELLED       = 7

    
    # if not completion date is found and the status is "COMPLETED" or "VALIDATED" then the real status is "CANCELLED"
    def set_cancel_status(self, entities_list:list):
        STATUS = PraxedoSoapClient.WORK_ORDER_STATUS
        for biz_evt in entities_list:
            comp_date = False
            for date in biz_evt.completionData.lifecycleTransitionDates : 
                if date['name'] == 'completionDate' : comp_date = True; break
            
            if not comp_date :
                match biz_evt.status : 
                    case STATUS.COMPLETED.name |STATUS.VALIDATED.name : biz_evt.status = STATUS.CANCELLED.name  

    
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
    
    
    class GET_WO_RESULT_CODE(Enum) :
        SUCCESS                     =  0
        INTERNAL_ERROR              =  1
        UNCOMPLETE_REQUEST          = 50                   
        TOO_MANY_EVENT_REQUESTED    = 51
        UNKNOWN_CONTRACT            = 157
                            
    
    
    """
    #region
        **** 21x possible options : see : https://support.praxedo.com/hc/fr/articles/115004095289-Gestion-des-interventions#getevents
        businessEvent.populate  .coreData        
                                .coreData.referentialData
                                .coreData.serviceOrderPosition
                                .qualificationData
                                .qualificationData.reopening
                                .qualificationData.extendedExpectedItems
                                .schedulingData
                                .schedulingData.appointmentDate
                                .schedulingData.populateInQualifiedStatus
                                .completionData.fields
                                .completionData.items
                                .completionData.extendedItems
                                .completionData.items.customAttributes
                                .completionData.lifeCycleDate
                                .completionData.excludeBinaryData
                                .annotations
                                .contractData
                                .annotations.withReportFields
                                .annotations.withCode
        extendedLastModificationDate
        businessEvent.feature.status.cancelled
    #endregion
        """
    
    
    class GET_WO_RESULT_OPTION(NamedTuple):

        class OPTIONS(NamedTuple):
            prefix = 'businessEvent.populate.'
            COREDATA_1     = f'{prefix}coreData'
            QUALIDATA_1    = f'{prefix}qualificationData'
            SCHEDUDATA_1   = f'{prefix}schedulingData'
            COMPLDATA_1    = f'{prefix}completionData.lifeCycleDate'
            COMPLDATA_2    = f'{prefix}completionData.fields'
            COMPLDATA_3    = f'{prefix}completionData.excludeBinaryData' 
            

        BASIC   = [ {'key': OPTIONS.COREDATA_1},
                    {'key': OPTIONS.QUALIDATA_1},
                    {'key': OPTIONS.SCHEDUDATA_1},
                    {'key': OPTIONS.COMPLDATA_1}
                 ]
        
        EXTENDED = BASIC.copy() + [
                    {'key': OPTIONS.COMPLDATA_2},
                    {'key': OPTIONS.COMPLDATA_3}
                                  ]
    
    
    
    def get_work_orders(self,evt_id_list : list[str], populate_opt = GET_WO_RESULT_OPTION.BASIC):

        RESULT_CODE = PraxedoSoapClient.GET_WO_RESULT_CODE
        
        populate_opt_arg =  populate_opt
        
        print('Calling the getEvents service ...')
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            get_evt_result = self.bizEvt_client.service.getEvents(evt_id_list,populate_opt_arg)

        result_code = RESULT_CODE(get_evt_result.resultCode) 
        
        if result_code.value > 0 : raise Exception(f'get_bizEvt returned an error : {result_code.name}')

        self.set_cancel_status(get_evt_result.entities)

        return get_evt_result
    
    
    class SRCH_WO_RESULT_OPTION(NamedTuple):

        """
        #region
        **** total of 20x possible options : see : https://support.praxedo.com/hc/fr/articles/115004095289-Gestion-des-interventions#searchevents
        businessEvent.populate  .coreData        
                                .coreData.referentialData
                                .coreData.serviceOrderPosition
                                .qualificationData
                                .qualificationData.reopening
                                .qualificationData.extendedExpectedItems
                                .schedulingData
                                .schedulingData.appointmentDate
                                .schedulingData.populateInQualifiedStatus
                                .completionData.items
                                .completionData.extendedItems
                                .completionData.items.customAttributes
                                .completionData.lifeCycleDate
                                .completionData.fields
                                .completionData.excludeBinaryData
                                .completionData.cancellation
                                .annotations
                                .annotations.withReportFields
                                .annotations.withCode
        extendedLastModificationDate
        #endregion
        """
        class OPTIONS(NamedTuple):
            prefix = 'businessEvent.populate.'
            CORE_1      = f'{prefix}coreData'
            QUALI_1     = f'{prefix}qualificationData'
            SCHED_1     = f'{prefix}schedulingData'
            CPLETION_1  = f'{prefix}completionData.lifeCycleDate'
            CPLETION_2  = f'{prefix}completionData.fields'
            CPLETION_3  = f'{prefix}completionData.excludeBinaryData'
            CPLETION_4  = f'{prefix}completionData.cancellation' 
            

        BASIC   = [ {'key': OPTIONS.CORE_1},
                    {'key': OPTIONS.QUALI_1},
                    {'key': OPTIONS.SCHED_1},
                    {'key': OPTIONS.CPLETION_1}
                 ]
        
        EXTENDED = BASIC.copy() + [
                    {'key': OPTIONS.CPLETION_2},
                    {'key': OPTIONS.CPLETION_3},
                    {'key': OPTIONS.CPLETION_4},
                                  ]
    
    
    class SRCH_WO_RETURN_CODE(Enum):    
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
    
    
    def search_work_orders(self,arg_date_constraint:DATE_CONSTRAINT,
                                arg_start_date:datetime, arg_stop_date:datetime,
                                arg_populate_opt=SRCH_WO_RESULT_OPTION.BASIC):
        
        
        MAX_RESULTS_PER_PAGE = 50  #This is the actual maximum limit allowed by Praxedo
        print(f'search_bizEvts:')
        
        requestArg =  {
                            "typeConstraint"   :   [],
                            "dateConstraints"  :   [
                                        {"name": arg_date_constraint,
                                        "dateRange":[arg_start_date.isoformat(),arg_stop_date.isoformat()] 
                                        }
                                                    ],
                            
                            "agentIdConstraint" : [],
                            "statusConstraint " : [],
                            "serviceOrderConstraint" : ""
                        }


        resp_page_nbr      = 1
        first_result_idx   = 0
        total_entities_results = []
        
        # iterate over a multi-page response when applicable
        while True:
            
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
               
                print(f'search_bizEvts: page {resp_page_nbr}')
                search_results = self.bizEvt_client.service.searchEvents(requestArg,MAX_RESULTS_PER_PAGE,first_result_idx,arg_populate_opt)
            
            return_code = PraxedoSoapClient.SRCH_WO_RETURN_CODE(search_results.resultCode)
            match return_code : 
                case PraxedoSoapClient.SRCH_WO_RETURN_CODE.SUCESS :
                    total_entities_results += search_results.entities
                    search_results.entities = total_entities_results
                    break
                
                case PraxedoSoapClient.SRCH_WO_RETURN_CODE.PARTIAL_RESULT :
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
        
        self.set_cancel_status(search_results.entities)

        return search_results
    
    
    
    