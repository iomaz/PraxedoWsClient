from enum import Enum, unique
from zeep import helpers as zeepHelper
import pandas as pd
import orjson
from jsonQ import Query
import jsonpath

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


def build_core_model_from_ws_result(ws_result_entities:list[object]):
    
    # convert zeep objects to native python structures
    pyObj_entities = zeepHelper.serialize_object(ws_result_entities)

    # building a data frame out of the result. level = 1 is enough to have a natural accesss to useful fields
    df_wo_core = pd.json_normalize(pyObj_entities,max_level=2) # type: ignore

    def convert_complex_val_to_json(value):
        if isinstance(value,(list,dict)):
            if len(value) > 0:
                json_val = orjson.dumps(value, default= lambda val : 'null').decode('utf-8').strip('"')
                return json_val
            else: return None
        else: return value

    df_wo_core = df_wo_core.map(convert_complex_val_to_json)

    # transforming the resulting data frame to match the "core model" which consist in 4x tables
    # [1] wo_core  
    # [2] wo_attach
    # [3] wo_report
    # [4] wo_report_imgs

    # [1] - Building the wo_report frame
    # creating the table by copying the work order "id"
    WO_CORE_ID_COL           = 'id'
    WO_CORE_EXTENSION_COL    = 'extensions'
    WO_CORE_FIELDS_COL       = 'completionData.fields'

    WO_REPORT_ID_COL         = 'wo_id'
    WO_REPORT_UUID_COL       = 'wo_uuid'
    WO_REPORT_FIELDS_COL     = 'wo_report_fields'
    WO_REPORT_PDF_BIN_COL    = 'wo_report_pdf_bin'

    df_wo_report = pd.DataFrame(columns=[WO_REPORT_ID_COL,WO_REPORT_UUID_COL,WO_REPORT_FIELDS_COL,WO_REPORT_PDF_BIN_COL])
    df_wo_report[WO_REPORT_ID_COL] = df_wo_core[[WO_CORE_ID_COL]]

    def extract_uuid(json_val):
        query = Query(orjson.loads(json_val))
        uuid_prop = query.where("key == businessEvent.extension.uuid").tolist()
        if uuid_prop :
            return_val = uuid_prop[0]['value'] 
            return return_val
        return 'null'

    df_wo_report[WO_REPORT_UUID_COL] = df_wo_core[WO_CORE_EXTENSION_COL].apply(extract_uuid)

    df_wo_report[WO_REPORT_FIELDS_COL] = df_wo_core[WO_CORE_FIELDS_COL].copy()
    df_wo_core.drop(columns=[WO_CORE_FIELDS_COL])

    # define an empty pdf report column
    df_wo_report[WO_REPORT_PDF_BIN_COL] = None

    print('*** wo_report ****')
    print(df_wo_report)

    
    # [2] Building the wo_report_img data frame
    WO_REPORT_IMGS_ID           = 'wo_id'
    WO_REPORT_IMGS_FIELD_ID     = 'wo_report_field_id'
    WO_REPORT_IMGS_URL_COL      = 'wo_report_img_url'
    WO_REPORT_IMGS_BIN_COL      = 'wo_report_img_bin'
    df_wo_report_imgs = pd.DataFrame(columns=[WO_REPORT_IMGS_ID,WO_REPORT_IMGS_FIELD_ID,WO_REPORT_IMGS_URL_COL,WO_REPORT_IMGS_BIN_COL])

    for index, row in df_wo_report.iterrows():
        img_field_list = jsonpath.findall("$[? (@.extensions[0].key == 'binaryData.available') && (@.extensions[0].value == 'true')]",row[WO_REPORT_FIELDS_COL])
        for img_field in img_field_list :
            field_id = img_field['id'] # type: ignore
            img_url  = img_field['value'] # type: ignore
            df_wo_report_imgs.loc[len(df_wo_report_imgs)] =  [row[WO_REPORT_ID_COL],field_id,img_url,None]
    
    print('*** wo_report_imgs ****')
    print(df_wo_report_imgs)