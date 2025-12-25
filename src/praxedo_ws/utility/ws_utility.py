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
from dataclasses import dataclass


def get_url_content(arg_url):
    with warnings.catch_warnings():
        MAX_RETRY = 2
        retryCount = 0
        warnings.simplefilter('ignore')
        while retryCount <= 1 :   
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
                        print('get_url_content():Err 429 - too many requests- wait and retry...')
                        sysTime.sleep(5)
        
    return result.content

def batch_fetch_url(arg_url_list : list[tuple[str,str]], arg_batch_size = 20):
    #BATCH_SIZE = 20
    # splitting the list into fetch batches
    fetch_batchs = [arg_url_list[i:i + arg_batch_size] for i in range(0, len(arg_url_list), arg_batch_size)]

    for idx, fetch_batch in enumerate(fetch_batchs):
            
        print(f'\rprocessing batch : {idx+1}/{len(fetch_batchs)} ',end='',flush=True)
        
        url_list = [url_tuple[1] for url_tuple in fetch_batch] # extracting the url list from tuple
        # downloading a chunck of url concurently
        with ThreadPoolExecutor(max_workers=arg_batch_size) as executor:
            url_contents = list(executor.map(get_url_content, url_list))

        result = [(fetch_batch[idx][0], url_content) for idx, url_content in enumerate(url_contents)] # type: ignore
        yield result


def get_week_days(week: int, year: int):
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

def normalize_ws_response(arg_wo_entities_list:list[object]):
    '''
    The function basically normalize a raw SOAP web service response to separate work order and work order report information
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
                        every row correspond to a work order report and represent an image field with its field code and url

    '''

    '''
    Data frame schemas
    '''

    ''' wo_core 
    The schema for the wo_core frame is entierly defined by the soap result.
    The result is then "flatenized" by the json_normalize() method
    The following identifyer are the few necessary colums id to allow the separaration of wo_report from the wo_core
    '''
    REF_WO_CORE_ID_COL           = 'id'
    REF_WO_CORE_EXTENSION_COL    = 'extensions'
    REF_WO_CORE_FIELDS_COL       = 'completionData.fields'

    ''' wo_report
    '''
    WO_REPORT_ID_COL         = 'wo_id'
    WO_REPORT_UUID_COL       = 'wo_uuid'
    WO_REPORT_FIELDS_COL     = 'wo_report_fields'
    WO_REPORT_PDF_BIN_COL    = 'wo_report_pdf_bin'

    ''' wo_report_imgs
    '''
    WO_REPORT_IMGS_ID           = 'wo_id'
    WO_REPORT_IMGS_FIELD_ID     = 'wo_report_field_id'
    WO_REPORT_IMGS_URL_COL      = 'wo_report_img_url'
    WO_REPORT_IMGS_BIN_COL      = 'wo_report_img_bin'


    # convert zeep objects to native python structures
    pyObj_entities = zeepHelper.serialize_object(arg_wo_entities_list)

    # building a data frame out of the result. 
    # level = 2 is enough to get enough useful columns
    df_wo_core = pd.json_normalize(pyObj_entities,max_level=2) # type: ignore

    #print('*** wo_core ****')
    #print(df_wo_core[REF_WO_CORE_FIELDS_COL])

    # this serialize non primitive value to json
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

    df_wo_report = pd.DataFrame(columns=[WO_REPORT_ID_COL,WO_REPORT_UUID_COL,WO_REPORT_FIELDS_COL,WO_REPORT_PDF_BIN_COL])
    df_wo_report[WO_REPORT_ID_COL] = df_wo_core[[REF_WO_CORE_ID_COL]]

    # extract the uuid from the "extensions" wo property
    def extract_uuid(json_val):
        query = Query(orjson.loads(json_val))
        uuid_prop = query.where("key == businessEvent.extension.uuid").tolist()
        if uuid_prop :
            return_val = uuid_prop[0]['value'] 
            return return_val
        return 'null'

    df_wo_report[WO_REPORT_UUID_COL] = df_wo_core[REF_WO_CORE_EXTENSION_COL].apply(extract_uuid)

    df_wo_report[WO_REPORT_FIELDS_COL] = df_wo_core[REF_WO_CORE_FIELDS_COL].copy()

    # dropping the report fields column from the original frame
    df_wo_core.drop(columns=[REF_WO_CORE_FIELDS_COL])

    # define an empty pdf report column
    df_wo_report[WO_REPORT_PDF_BIN_COL] = None

    #print('*** wo_report ****')
    #print(df_wo_report)

    
    # [2] Building the wo_report_img data frame
    df_wo_report_imgs = pd.DataFrame(columns=[WO_REPORT_IMGS_ID,WO_REPORT_IMGS_FIELD_ID,WO_REPORT_IMGS_URL_COL,WO_REPORT_IMGS_BIN_COL])

    for index, row in df_wo_report.iterrows():
        img_field_list = jsonpath.findall("$[? (@.extensions[0].key == 'binaryData.available') && (@.extensions[0].value == 'true')]",row[WO_REPORT_FIELDS_COL])
        for img_field in img_field_list :
            field_id = img_field['id'] # type: ignore
            img_url  = img_field['value'] # type: ignore
            df_wo_report_imgs.loc[len(df_wo_report_imgs)] =  [row[WO_REPORT_ID_COL],field_id,img_url,None]
    
    #print('*** wo_report_imgs ****')
    #print(df_wo_report_imgs)

    # returning a named tuple with a the reference to the 3x frame
    
    result = NORMALIZED_DF_RESULT(wo_core = df_wo_core, wo_report = df_wo_report, wo_report_imgs = df_wo_report_imgs)

    return result