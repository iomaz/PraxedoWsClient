from collections import namedtuple
from zeep import helpers as zeepHelper
import pandas as pd
import orjson
from jsonQ import Query
import jsonpath


from datetime import date, timedelta

def date_range_from_week(week: int, year: int):
    """
    Return the (start_date, end_date) for an ISO week.
    ISO week starts on Monday (weekday=1) and ends on Sunday (weekday=7).
    """
    # Monday of the ISO week
    start = date.fromisocalendar(year, week, 1)  # 1 = Monday
    start = start - timedelta(days=1)
    end   = start + timedelta(days=7)
    return start, end


def get_dates_from_week(week : int, year: int):
    
    jan1 = date(year, 1, 1)
    # Move back to the most recent Sunday (including Jan 1 if it is Sunday)
    offset = (jan1.weekday() - 6) % 7
    first_sunday = jan1 - timedelta(days=offset)

    # Compute start Sunday for the requested week
    start_sunday = first_sunday + timedelta(weeks=week - 1)
    next_sunday = start_sunday + timedelta(days=7)

    return start_sunday, next_sunday


def get_wo_raw_model(ws_result_entities:list[object]):
    '''
    The function basically process a raw SOAP web service response and separate work order and work order report information
    The result is a "ws raw model" with 3x tables
    It does not load any extra information and its purpose is to get convinient structure for further processing.

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
    pyObj_entities = zeepHelper.serialize_object(ws_result_entities)

    # building a data frame out of the result. 
    # level = 2 is enough to get enough useful columns
    df_wo_core = pd.json_normalize(pyObj_entities,max_level=2) # type: ignore

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

    print('*** wo_report ****')
    print(df_wo_report)

    
    # [2] Building the wo_report_img data frame
    df_wo_report_imgs = pd.DataFrame(columns=[WO_REPORT_IMGS_ID,WO_REPORT_IMGS_FIELD_ID,WO_REPORT_IMGS_URL_COL,WO_REPORT_IMGS_BIN_COL])

    for index, row in df_wo_report.iterrows():
        img_field_list = jsonpath.findall("$[? (@.extensions[0].key == 'binaryData.available') && (@.extensions[0].value == 'true')]",row[WO_REPORT_FIELDS_COL])
        for img_field in img_field_list :
            field_id = img_field['id'] # type: ignore
            img_url  = img_field['value'] # type: ignore
            df_wo_report_imgs.loc[len(df_wo_report_imgs)] =  [row[WO_REPORT_ID_COL],field_id,img_url,None]
    
    print('*** wo_report_imgs ****')
    print(df_wo_report_imgs)

    # returning a named tuple with a the reference to the 3x frame
    Result = namedtuple('result',['wo_core','wo_report','wo_report_imgs'])
    result = Result(wo_core = df_wo_core, wo_report = df_wo_report, wo_report_imgs = df_wo_report_imgs)

    return result