from zeep import helpers as zeepHelper
import pandas as pd
import orjson
from jsonQ import Query

def build_core_model_from_ws_result(ws_result_entities:list[object]):
    
    # convert zeep objects to native python structures
    pyObj_result = zeepHelper.serialize_object(ws_result_entities)

    # building a data frame out of the result. level = 1 is enough to have a natural accesss to useful fields
    df = pd.json_normalize(pyObj_result,max_level=1)

    # jsonifying all frame values
    json_df = df.map(lambda value : orjson.dumps(value, default= lambda val : 'null').decode('utf-8').strip('"'))

    # splitting the resulting data frame to match the "core model" which consist in 4x tables
    # [1] wo_core  (wo = work order)
    # [2] wo_attach
    # [3] wo_report
    # [4] wo_report_img

    # [1] - Building the wo_report frame
    # creating the table by copying the work order "id"
    WO_ID_COL           = 'id'
    WO_EXTENSION_COL    = 'extensions'
    WO_UUID_COL         = 'uuid'

    wo_report = json_df[[WO_ID_COL]].copy()

    def extract_uuid(json_val):
        query = Query(orjson.loads(json_val))
        uuid_prop = query.where("key == businessEvent.extension.uuid").tolist()
        if uuid_prop :
            return_val = uuid_prop[0]['value'] 
            return return_val
        return 'null'

    wo_report[WO_UUID_COL] = json_df[WO_EXTENSION_COL].apply(extract_uuid)

    print('resulting data frame')
    print(wo_report)