from enum import Enum
from typing import NamedTuple
import warnings
from zeep import helpers as zeepHelper
import requests
import pandas as pd
import orjson
from jsonQ import Query
import jsonpath
import time as sysTime
from datetime import date, time, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import math

# local imports
from praxedo_ws.soap import PraxedoSoapClient

def get_url_content(arg_url):
    with warnings.catch_warnings():
        MAX_RETRY = 2
        retryCount = 0
        warnings.simplefilter('ignore')
        while retryCount <= 1 :
            print(f'get_url_content: url:{arg_url}')   
            result = requests.get(arg_url,verify=False)
            retry = False
            match result.status_code:
                case 200 : break # fine
                case _ if result.status_code != 429 :
                    print(f"Failed to download !: ErrCode:{result.status_code} Reason={result.reason} (url)={arg_url[-38:]}")
                    return None
                case 429 : # too many requests
                    retryCount += 1
                    if retryCount >= MAX_RETRY : 
                        print('Max retry errors : return None...')
                        return None
                    else : 
                        print('get_url_content():Err 429 - too many requests- wait 5[s] and retry...')
                        print(f'url:{arg_url}')
                        sysTime.sleep(5)
        
    return result.content

def fetch_url_batch(arg_url_list : list[tuple[str,str]], arg_batch_size = 20):
    #BATCH_SIZE = 20
    # splitting the list into fetch batches
    fetch_batchs = [arg_url_list[i:i + arg_batch_size] for i in range(0, len(arg_url_list), arg_batch_size)]

    for idx, fetch_batch in enumerate(fetch_batchs):
            
        #print(f'\rprocessing batch : {idx+1}/{len(fetch_batchs)} ',end='',flush=True)
        
        url_list = [url_tuple[1] for url_tuple in fetch_batch] # extracting the url list from tuple
        # downloading a chunck of url concurently
        with ThreadPoolExecutor(max_workers=arg_batch_size) as executor:
            url_contents = list(executor.map(get_url_content, url_list))

        result = [(url_content, fetch_batch[idx][0]) for idx, url_content in enumerate(url_contents)] # type: ignore
        yield result

def delay_fetch_url_batch(arg_url_dict : dict, arg_batch_size: int = 20,  arg_delay : float = 0.0):

    results = {}
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with ThreadPoolExecutor(max_workers=arg_batch_size) as executor:
            # launching all tasks waiting "delay" between each start
            task_to_wo = {}
            task_nbr = len(arg_url_dict)
            for idx, wo_id in enumerate(arg_url_dict) :
                sysTime.sleep(arg_delay)
                task = executor.submit(get_url_content, arg_url_dict[wo_id])
                task_to_wo.update({task:wo_id})

            # wait for all tasks to finish
            for idx, task in enumerate(task_to_wo):
                task_result = task.result() # wait for this task to finish
                completed_wo_no = task_to_wo[task]
                results.update({completed_wo_no : task_result})
    
    return results

def get_week_days_sequence(week: int, year: int):
    """
    Return a sequence of tuple covering a given week
    Each element of the sequence is a day period representing by a tuple (start_date, stop_date)
    """
    one_day = timedelta(days=1)
    start_day = date.fromisocalendar(year, week, 1) - one_day  # the Sunday of the given week
    start_day = datetime.combine(start_day,time(hour=23, minute=59, second=0))

    period_list = [(start_day, start_day + one_day)]
    for idx in range(6):
        last = period_list[-1] # latest element of the list
        period_list += [(last[1], last[1] + one_day)]

    return period_list


class NORMALIZED_DF_RESULT(NamedTuple):
    wo_core         : pd.DataFrame
    wo_report       : pd.DataFrame
    wo_report_imgs  : pd.DataFrame

def normalize_ws_response(arg_wo_entities_list:list[object],arg_base_url = PraxedoSoapClient.DEFAULTS_URL.BASE):
    '''
    The function basically normalize a raw SOAP web service response to get a convinient shema manly separating work order and work order report information
    The returned result is a "normalized model" with 3x frames
    It does not load any extra information as its purpose is only to get convinient structure for further processing.

    Input / argument
    :param ws_result_entities: a list of wo as it is returned by the web service (getResult / searchResult)
    
   result: produce a json value frame of the work oders and 2x extra frames to store report and report images
    - wo_core : This is the reference/fact table wich contains core work order informations without the report fields
                every row represent a work order

    - wo_report : This contains all report information. Mostly a column with all report fields value as json and an uuid
                  that is useful to get the work order report pdf
                  every row represent a work order report
    
    - wo_report_imgs : This contains all report field images information
                        every row represent a report specifi image field with its field code and url

    '''

    '''
    Data frame schemas
    '''

    ''' wo_core 
    The wo_core schema is mainly defined by the "flatenization" of the soap result returned by the json_normalize() method
    The following identifyer are the few necessary colums id to allow the separaration of wo_report from the wo_core
    '''
    REF_WO_CORE_ID_COL           = 'id'
    REF_WO_CORE_STATUS_COL       = 'status'
    REF_WO_CORE_EXTENSION_COL    = 'extensions'
    REF_WO_CORE_CREA_DATE_COL    = 'coreData.creationDate'
    REF_WO_CORE_FIELDS_COL       = 'completionData.fields'
    REF_WO_CORE_LIFCY_DATES_COL  = 'completionData.lifecycleTransitionDates'
    REF_WO_CORE_REF_LOC_COL      = 'coreData.referentialData.location'
    REF_WO_CORE_QDATA_TYPE       = 'qualificationData.type.id'

    WO_CORE_UUID_COL             = 'uuid'
    WO_CORE_UUID_PROP            = 'businessEvent.extension.uuid'
    WO_CORE_LOCATION_NAME_COL    = 'coreData.referentialData.location.name'

    # lifecycle dates
    class WO_CORE_LIFCY_DATES_COL(Enum):
        COMMUNI      = 'communicationDate'
        APPOINT      = 'appointmentDate'
        SCHEDU       = 'schedulingDate'
        PDA_LOAD     = 'pdaLoadingDate'
        PDA_UNLOAD   = 'pdaUnloadingDate'
        START        = 'startDate'
        COMPLETION   = 'completionDate'
        VALIDATION   = 'validationDate'
        LAST_MODI    = 'lastModificationDate' 

    ''' wo_report
    '''
    WO_REPORT_ID_COL        = 'wo_id'
    WO_REPORT_URL_COL       = 'wo_report_url'
    WO_REPORT_FIELDS_COL    = 'wo_report_fields'

    ''' wo_report_imgs
    '''
    WO_REPORT_IMGS_ID           = 'wo_id'
    WO_REPORT_IMGS_FIELD_ID     = 'wo_report_field_id'
    WO_REPORT_IMGS_URL_COL      = 'wo_report_img_url'

    def pop_reindex(df:pd.DataFrame, col_name: str, new_pos:int):
        col_values = df.pop(col_name)
        df.insert(new_pos,col_name,col_values)

    # convert zeep objects to native python structures
    pyObj_entities = zeepHelper.serialize_object(arg_wo_entities_list)
    del arg_wo_entities_list

    # building a data frame by normalizing the list of wo object 
    # level = 2 is enough to get a majority of useful columns
    df_wo_core = pd.json_normalize(pyObj_entities,max_level=2) # type: ignore

    # reordering a few columns
    pop_reindex(df_wo_core,REF_WO_CORE_ID_COL,0)
    pop_reindex(df_wo_core,REF_WO_CORE_QDATA_TYPE,1)
    pop_reindex(df_wo_core,REF_WO_CORE_STATUS_COL,2)
    pop_reindex(df_wo_core,REF_WO_CORE_CREA_DATE_COL,3)

    # creating the location.name column by extracting the value from the "coreData:referentialData.location.name" column
    df_location_name = df_wo_core[REF_WO_CORE_REF_LOC_COL].map(lambda loc_val : loc_val['name'] if loc_val else None )
    src_location = df_wo_core.columns.get_loc(REF_WO_CORE_REF_LOC_COL)
    df_wo_core.insert(src_location + 1,WO_CORE_LOCATION_NAME_COL,df_location_name) # type: ignore

    # create the uuid column by extracting the value from the "extensions" column
    df_extensions = df_wo_core[REF_WO_CORE_EXTENSION_COL].map(lambda xtsion_tab : { xtsion_prop['key'] : xtsion_prop['value'] for xtsion_prop in xtsion_tab} )
    df_uuid = df_extensions.map(lambda xtsion_dict : xtsion_dict[WO_CORE_UUID_PROP] if WO_CORE_UUID_PROP in xtsion_dict else None)
    # insert the uuid column just after the extensions column
    df_wo_core.insert(df_wo_core.columns.get_loc(REF_WO_CORE_EXTENSION_COL) + 1, WO_CORE_UUID_COL, df_uuid) # type: ignore

    # expand the content of the "lifecycleTransitionDates" column into the lifecycle dates columns  
    # [1] tranform the 'lifecycleTransitionDates" collection into a single dictionary 
    df_lifcy = df_wo_core[REF_WO_CORE_LIFCY_DATES_COL].map(lambda lifcy_tab : { lifcy_elt['name'] : lifcy_elt['date'] for lifcy_elt in lifcy_tab})

    # [2] copy all date to every associated columns 
    for idx, lify_columns in enumerate(WO_CORE_LIFCY_DATES_COL) :
        new_column = df_lifcy.map(lambda lifcy_dict : lifcy_dict[lify_columns.value] if lify_columns.value in lifcy_dict else None )
        df_wo_core.insert(3 +idx,f'{REF_WO_CORE_LIFCY_DATES_COL}.{lify_columns.value}',new_column) 

    # [3] drop the original lifcycleTransitionDates column
    df_wo_core.drop(columns=[REF_WO_CORE_LIFCY_DATES_COL],inplace=True)

    # convert every "date" dolumns into ISO 8601 strings (also removing the useless [ms]/[us] component if any)
    for col_name in [col_name for col_name in df_wo_core.columns if col_name.lower().endswith('date') ] :
        df_wo_core[col_name] = df_wo_core[col_name].map(lambda date_obj : date_obj.isoformat(timespec='seconds') if date_obj else None )

    # .isoformat(timespec='seconds')

    #print('*** wo_core ****')
    #print(df_wo_core[REF_WO_CORE_FIELDS_COL])

    # this serialize all "non primitive" values to json
    def convert_struct_to_json(value):
        if isinstance(value,(list,dict)):
            if len(value) > 0:
                json_val = orjson.dumps(value, default= lambda val : 'null').decode('utf-8').strip('"')
                return json_val
            else: return None
        else: return value

    df_wo_core = df_wo_core.map(convert_struct_to_json)


    # [1] - Building the wo_report frame
    # creating the table by copying the work order "id"

    df_wo_report = pd.DataFrame(columns=[WO_REPORT_ID_COL,WO_REPORT_URL_COL,WO_REPORT_FIELDS_COL])
    df_wo_report[WO_REPORT_ID_COL] = df_wo_core[[REF_WO_CORE_ID_COL]].copy() # copy the 'id" column fron the wo_core to the wo_report

    # build the report url out of the uuid and base url
    df_wo_report[WO_REPORT_URL_COL] = df_wo_core[WO_CORE_UUID_COL].map(lambda uuid : f'{arg_base_url}/rest/api/v1/workOrder/uuid:{uuid}/render' )

    # copy the 'fields" column from the wo_core frame
    df_wo_report[WO_REPORT_FIELDS_COL] = df_wo_core[REF_WO_CORE_FIELDS_COL].copy()

    # dropping the report fields column from the original frame
    df_wo_core.drop(columns=[REF_WO_CORE_FIELDS_COL],inplace=True)

    #print('*** wo_report ****')
    #print(df_wo_report)

    
    # [2] Building the wo_report_img data frame
    df_wo_report_imgs = pd.DataFrame(columns=[WO_REPORT_IMGS_ID,WO_REPORT_IMGS_FIELD_ID,WO_REPORT_IMGS_URL_COL])

    #for report_field_row in df_wo_report[WO_REPORT_FIELDS_COL] :
    for index, wo_report_row in df_wo_report.iterrows():
        img_fields = jsonpath.findall("$[? (@.extensions[0].key == 'binaryData.available') && (@.extensions[0].value == 'true')]",wo_report_row[WO_REPORT_FIELDS_COL])
        for img_field in img_fields :
            field_id = img_field['id'] # type: ignore
            img_url  = img_field['value'] # type: ignore
            df_wo_report_imgs.loc[len(df_wo_report_imgs)] =  [wo_report_row[WO_REPORT_ID_COL],field_id,img_url]
    
    #print('*** wo_report_imgs ****')
    #print(df_wo_report_imgs)

    # returning a named tuple with a the reference to the 3x frame
    
    result = NORMALIZED_DF_RESULT(wo_core = df_wo_core, wo_report = df_wo_report, wo_report_imgs = df_wo_report_imgs)

    return result