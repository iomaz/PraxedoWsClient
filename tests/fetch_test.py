from pprint import pprint
import sqlite3
import os, sys
import hashlib
import base64
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

# local imports
from praxedo_ws.soap import PraxedoSoapClient
from praxedo_ws.utility import *
from test_common import *

PROD_USER = PraxedoSoapClient.WsCredential(usr='WSDEM',
                                            psw='WsdemWsdem2358')

praxedoWS = PraxedoSoapClient()
praxedoWS.connect(PROD_USER)


def fetch_wo_week_by_day(arg_week_nbr:int, arg_year:int):
    
    # getting all search periods
    week_days_period = get_week_days_sequence(arg_week_nbr, arg_year)

    EXTENDED_RESULT = PraxedoSoapClient.SRCH_WO_RESULT_OPTION.EXTENDED
    WO_COMPLETED    = PraxedoSoapClient.DATE_CONSTRAINT.COMPLETION
    # searching and fetching each day and get the 
    for day_start, day_stop in week_days_period:
        wo_entities = praxedoWS.search_work_orders(WO_COMPLETED,day_start, day_stop,EXTENDED_RESULT) # type: ignore
        yield wo_entities

total_duration = SimplePerfClock()
day_duration = SimplePerfClock()
nz_results = []
total_wo_nbr = 0
total_duration.start()
day_duration.start()
print('fetch whole week begin')
print('day 1...')
for day_idx, one_day_wo_list in enumerate(fetch_wo_week_by_day(52,2024)):
    result_size = len(one_day_wo_list)
    if result_size > 0:
        total_wo_nbr += result_size
        # normalize the result
        nz_result = normalize_ws_response(one_day_wo_list)
        # accumulate the results into a list
        nz_results.append(nz_result)
        #DEBUG
        if day_idx == 0 : break
    
    day_duration.stop()
    print(f'day {day_idx+1}:total duration:{day_duration.total_time_str()} number of work orders: {result_size}')
    #if idx == 1 : break # DEBUG
    print(f'day {day_idx+2}...')
    day_duration.start()

# merging results
if len(nz_results) > 0:
    wo_core         = pd.concat([elt.wo_core for elt in nz_results])
    wo_report       = pd.concat([elt.wo_report for elt in nz_results])
    wo_report_imgs  = pd.concat([elt.wo_report_imgs for elt in nz_results])

    print(f'total work order results nbr:{total_wo_nbr}')

    # extracting json int value from given key
    def json_extract_int_val_from_key(arg_key_str:str, arg_json_content:str):
        json_match = jsonpath.findall(f'$[? (@.id == "{arg_key_str}")]',arg_json_content)
        if json_match: return int(json_match[0]['value']) # type: ignore
        else : return None

    WO_REPORT_FIELDS_COL    = 'wo_report_fields'
    REPORT_SAP_OR_FIELD     = 'F_SUB_CT_Or'
    REPORT_SAP_LC_FIELD     = 'F_SUB_CT_LC'
    WO_REPORT_OR_COL        = 'wo_sap_or'
    WO_REPORT_LC_COL        = 'wo_sap_lc'

    WO_CORE_LOCATION_NAME_COL   = 'coreData.referentialData.location.name'
    wo_report[WO_REPORT_OR_COL] = wo_core[WO_CORE_LOCATION_NAME_COL].map(lambda name_val : int(name_val))
    wo_report[WO_REPORT_LC_COL] = wo_report[WO_REPORT_FIELDS_COL].map(lambda json_content : json_extract_int_val_from_key(REPORT_SAP_LC_FIELD,json_content))

    # adding two extra column "wo_report_pdf_digest_sha-512" and "wo_report_pdf_byte_size"
    WO_REPORT_REPORT_BYTE_SIZE_COL              = 'wo_report_pdf_byte_size'
    wo_report[WO_REPORT_REPORT_BYTE_SIZE_COL]   = None
    WO_REPORT_REPORT_DIGEST_COL                 = 'wo_report_pdf_sha512_digest'
    wo_report[WO_REPORT_REPORT_DIGEST_COL]      = None


# writing the normalized form to the db
if len(nz_results) > 0:
# write the result to db
    with sqlite3.connect(f'fetch_result.sqlite3') as sqlite_db:

        wo_core_dtype = {'id': 'INTEGER PRIMARY KEY'} # making the "id" column a primary key
        wo_core.to_sql('wo_core', sqlite_db, dtype=wo_core_dtype, if_exists='replace',index=False) # type: ignore
        
        wo_report_dtype = {'wo_id':'INTEGER UNIQUE REFERENCES wo_core(id)'}  # making "wo_id" a foreign key
        wo_report.to_sql('wo_report', sqlite_db, dtype=wo_report_dtype, if_exists='replace',index=False) # type: ignore

        wo_report_imgs_dtype = {'wo_id':'INTEGER REFERENCES wo_report(wo_id)'} # making "wo_id" a foreign key
        wo_report_imgs.to_sql('wo_report_imgs', sqlite_db, dtype=wo_report_imgs_dtype, if_exists='replace',index=False) # type: ignore

        sqlite_db.commit()
    
        # clearing memory
        wo_core = wo_report = wo_report_imgs = None

        #DEBUG emptying the whole wo_report_bin column
        #sql_clear_pdf_bin = 'UPDATE wo_report SET wo_report_pdf_bin = NULL'
        #sqlite_db.execute(sql_clear_pdf_bin)
        #sqlite_db.commit()

        pdf_fetch_duration = SimplePerfClock()
        pdf_fetch_duration.start()
        # download pdf report and update the wo_report table
        BATCH_SIZE = 25
        sql_pdf_rows = 'SELECT wo_id, wo_report_url FROM wo_report WHERE wo_report_pdf_byte_size IS NULL'
        sql_pdf_rows_count =  f'SELECT COUNT(*) FROM ({sql_pdf_rows})'
        

        total_fetch_nbr, = sqlite_db.execute(sql_pdf_rows_count).fetchone()
        print(f'downloading :{total_fetch_nbr} files...')

        def b64_sha512_digest(data:bytes):
            digest = hashlib.sha512(data).digest()
            return base64.b64encode(digest).decode('utf-8')   

        total_batch_nbr = (total_fetch_nbr + BATCH_SIZE -1) // BATCH_SIZE
        select_cur = sqlite_db.execute(sql_pdf_rows)
        for batch_idx in range(total_batch_nbr) : 
            # downloading the report pdf files
            batch = select_cur.fetchmany(BATCH_SIZE)
            url_dict = { wo_id : url for wo_id, url in batch }
            report_contents = delay_fetch_url_batch(url_dict,BATCH_SIZE,0.5)
            print(f'\rdownloaded : { (batch_idx+1) *BATCH_SIZE}/{total_batch_nbr * BATCH_SIZE} {math.floor(((batch_idx+1) /total_batch_nbr)*100)}% elapsed time:{pdf_fetch_duration.elapsed_time_str()}',end='',flush=True)

            sql_update_rows = 'UPDATE wo_report SET wo_report_pdf_byte_size = ?, wo_report_pdf_sha512_digest = ? WHERE wo_id = ?'
            #computing the report binary size and digest
            data_update = tuple((len(report_contents[wo_no]), b64_sha512_digest(report_contents[wo_no]), wo_no) for wo_no in report_contents)
            
            update_cur = sqlite_db.executemany(sql_update_rows,data_update)
            sqlite_db.commit()
    
    pdf_fetch_duration.stop()
    print(f'total: wo nbr:{total_wo_nbr} pdf fetch duration:{pdf_fetch_duration.total_time_str()}')

total_duration.stop()
print(f'total process duration:{total_duration.total_time_str()}')

