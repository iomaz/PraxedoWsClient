from pprint import pprint
import sqlite3
import os, sys
import hashlib
import base64
from pathlib import Path
from pympler import asizeof
import math

# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

# local imports
from praxedo_ws.soap import PraxedoSoapClient
from praxedo_ws.utility import *
from test_common import *


PROD_USER = PraxedoSoapClient.UserCredential(usr='WSDEM',
                                           psw='WsdemWsdem2358')

PROD_USER2 = PraxedoSoapClient.UserCredential(usr='WSDEM2',
                                            psw='WsdemWsdem2358')


praxedoWS = PraxedoSoapClient()
praxedoWS.connect(PROD_USER,PROD_USER2)

# DEBUG
mem_total_raw_data = 0

def b64_sha512_digest(data:bytes):
    digest = hashlib.sha512(data).digest()
    return base64.b64encode(digest).decode('utf-8') 


def fetch_wo_week_by_page(arg_week_nbr:int, arg_year:int):
    
    global mem_total_raw_data #DEBUG

    # getting all search periods
    week_day_list = get_week_days_sequence(arg_week_nbr, arg_year)

    EXTENDED_RESULT = PraxedoSoapClient.SRCH_WO_RESULT_OPTION.EXTENDED
    WO_COMPLETED    = PraxedoSoapClient.DATE_CONSTRAINT.COMPLETION
    # searching and fetching each day
     
    for day_idx, day_period in enumerate(week_day_list):
        day_start, day_stop = day_period
        # print(f'fetch_wo_week_by_day: day:{day_idx+1}')
        for page_idx, result_page in enumerate(praxedoWS.search_work_orders_per_page(WO_COMPLETED,day_start, day_stop,EXTENDED_RESULT)) : # type: ignore
            mem_total_raw_data += asizeof.asizeof(result_page) # DEBUG
            yield day_idx+1, page_idx, result_page

def fetch_attachments_list(arg_wo_id_list):
    
    # downloading the attachements list for each wo
    #print(f'fetching attachments list ...')
    total_attach_nbr = 0
    for idx, wo_id in enumerate(arg_wo_id_list) :
        
        # print(f'\r{idx+1}/{len(arg_wo_id_list)} attachment for the wo_id:{wo_id}',end='',flush=True)
        
        time.sleep(0.05) # limiting the rate to stay under the max frequency allowed by the service
        attach_list = praxedoWS.list_attachments(wo_id)
        
        for attach_info in attach_list :
            #print('add attach list item...')
            attach_id  = str(attach_info['id'])
            attach_name = str(attach_info['name'])
            attach_size = int(attach_info['size'])
            # adding a new row to the dataframe
            wo_attachments.loc[len(wo_attachments)] =  [int(wo_id), attach_id, attach_name, attach_size, None]
            total_attach_nbr += 1

    #print(f'fetched {total_attach_nbr} attachment informations')
    return total_attach_nbr


# building the extra wo_attachments dataframe
WO_ATTACH_WO_ID_COL = 'wo_id'
WO_ATTACH_ID_COL    = 'attach_id'
WO_ATTACH_NAME_COL  = 'attach_name'
WO_ATTACH_SIZE_COL  = 'attach_byte_zize'
WO_ATTACH_GIGEST_COL = 'attach_sha512_digest'  
wo_attachments = pd.DataFrame(columns=[WO_ATTACH_WO_ID_COL, WO_ATTACH_ID_COL, WO_ATTACH_NAME_COL, WO_ATTACH_SIZE_COL, WO_ATTACH_GIGEST_COL])

total_duration = SimplePerfClock()
week_duration = SimplePerfClock()
page_duration = SimplePerfClock()
nz_results = []
total_wo_nbr = 0
total_attach_nbr = 0
total_duration.start()
week_duration.start()
page_duration.start()
print('requesting all wo for a week ...')
for wo_page_result in fetch_wo_week_by_page(1,2022) :
    day_idx, page_idx, wo_list = wo_page_result
    page_wo_nbr = len(wo_list)
    if page_wo_nbr > 0:
        
        total_wo_nbr += page_wo_nbr
        # normalize the result
        nz_result = normalize_ws_response(wo_list)
        
        # fetching attachments list for all fetched wo
        attach_nbr = fetch_attachments_list(nz_result.wo_core['id'])
        total_attach_nbr += attach_nbr
        
        #print(f'day:{day_idx} page:{page_idx+1} -> work orders:{result_size} attach:{attach_nbr}')
        
        # accumulate the results into a list
        nz_results.append(nz_result)
        del wo_page_result
        del nz_result
    
        #DEBUG
        # if page_idx == 5 : break
    
    page_duration.stop()
    print(f'day:{day_idx} page:{page_idx+1} -> Total wo:{page_wo_nbr} attach:{attach_nbr} duration:{page_duration.total_time_str()}\
        elapsed time :{week_duration.elapsed_time_str()}')
    #if page_idx == 10  : break # DEBUG
    page_duration.start()

week_duration.stop()
print(f'Total week request duration:{week_duration.total_time_str()}')

# merging results
total_nz_results = len(nz_results)
if total_nz_results > 0:
    wo_core         = pd.concat([elt.wo_core for elt in nz_results])
    wo_report       = pd.concat([elt.wo_report for elt in nz_results])
    wo_report_imgs  = pd.concat([elt.wo_report_imgs for elt in nz_results])

    print(f'total work orders:{total_wo_nbr} attachments:{total_attach_nbr}')

    # extracting json int value from given key
    def json_extract_int_val_from_key(arg_key_str:str, arg_json_content:str):
        json_match = jsonpath.findall(f'$[? (@.id == "{arg_key_str}")]',arg_json_content)
        if json_match: return int(json_match[0]['value']) # type: ignore
        else : return pd.NA

    WO_REPORT_FIELDS_COL    = 'wo_report_fields'
    REPORT_SAP_OR_FIELD     = 'F_SUB_CT_Or'
    REPORT_SAP_LC_FIELD     = 'F_SUB_CT_LC'
    WO_REPORT_OR_COL        = 'wo_sap_or'
    WO_REPORT_LC_COL        = 'wo_sap_lc'
    REF_WO_CORE_COMPLETION_DATE_COL = 'completionData.lifecycleTransitionDates.completionDate'
    WO_COMPLETION_DATE_COL  = 'wo_completion_date'

    # adding the "wo_sap_or" and "wo_sap_lc" columns
    WO_CORE_LOCATION_NAME_COL   = 'coreData.referentialData.location.name'
    wo_report[WO_REPORT_OR_COL] = wo_core[WO_CORE_LOCATION_NAME_COL].map(lambda name_val : int(name_val) if name_val else pd.NA)
    wo_report[WO_REPORT_LC_COL] = wo_report[WO_REPORT_FIELDS_COL].map(lambda json_content : json_extract_int_val_from_key(REPORT_SAP_LC_FIELD,json_content))

    # adding two extra columns for later use "wo_report_pdf_digest_sha-512" and "wo_report_pdf_byte_size"
    WO_REPORT_BYTE_SIZE_COL              = 'wo_report_pdf_byte_size'
    wo_report[WO_REPORT_BYTE_SIZE_COL]   = pd.NA
    WO_REPORT_DIGEST_COL                 = 'wo_report_pdf_sha512_digest'
    wo_report[WO_REPORT_DIGEST_COL]      = pd.NA

    # adding the "wo_completion_date" by copying from wo_core
    wo_report.insert(1,WO_COMPLETION_DATE_COL,wo_core[REF_WO_CORE_COMPLETION_DATE_COL].copy())

    # fetching all atachments lists
    #fetch_attachments_list(wo_core['id'])
    
    
    mem_wo_core     = wo_core.memory_usage(deep=True).sum() / 1000
    mem_wo_report   = wo_report.memory_usage(deep=True).sum() / 1000 
    mem_wo_report_imgs = wo_report_imgs.memory_usage(deep=True).sum() / 1000
    mem_total_dataframe = mem_wo_core + mem_wo_report + mem_wo_report_imgs

    mem_total_df = asizeof.asizeof(nz_results)
    
    print(f'''
    Memory consumption :
    Total raw ws data : {mem_total_raw_data / 1000} [KB]
    Total normalized dataframes : {mem_total_df / 1000} [KB]
    DataFrames 
    wo_core :{mem_wo_core}[KB]  
    wo_report:{mem_wo_report}[KB]
    wo_report_imgs:{mem_wo_report_imgs}[KB] 
    Total :{mem_total_dataframe} [KB]''')

    del nz_results
    
# writing the normalized form to the db
if total_nz_results > 0:
# write the result to db
    with sqlite3.connect(f'fetch_result.sqlite3') as sqlite_db:

        wo_core_dtype = {'id': 'INTEGER PRIMARY KEY'} # making the "id" column a primary key
        wo_core.to_sql('wo_core', sqlite_db, dtype=wo_core_dtype, if_exists='replace',index=False) # type: ignore
        
        wo_report_dtype = {'wo_id':'INTEGER UNIQUE REFERENCES wo_core(id)', 'wo_report_pdf_byte_size':'INTEGER'}  # making "wo_id" a foreign key
        wo_report.to_sql('wo_report', sqlite_db, dtype=wo_report_dtype, if_exists='replace',index=False) # type: ignore

        wo_report_imgs_dtype = {'wo_id':'INTEGER REFERENCES wo_report(wo_id)'} # making "wo_id" a foreign key
        wo_report_imgs.to_sql('wo_report_imgs', sqlite_db, dtype=wo_report_imgs_dtype, if_exists='replace',index=False) # type: ignore

        wo_attachments_dtype = {'wo_id':'INTEGER REFERENCES wo_report(wo_id)'} # making "wo_id" a foreign key
        wo_attachments.to_sql('wo_attachments', sqlite_db, dtype=wo_attachments_dtype, if_exists='replace',index=False) # type: ignore
        
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
        sql_pdf_rows = 'SELECT wo_id, wo_report_url, wo_completion_date, wo_sap_or, wo_sap_lc  FROM wo_report WHERE wo_report_pdf_byte_size IS NULL'
        sql_pdf_rows_count =  f'SELECT COUNT(*) FROM ({sql_pdf_rows})'
        

        total_fetch_nbr, = sqlite_db.execute(sql_pdf_rows_count).fetchone()
        print(f'downloading :{total_fetch_nbr} files...')
  
        total_batch_nbr = (total_fetch_nbr + BATCH_SIZE -1) // BATCH_SIZE
        sql_cursor = sqlite_db.execute(sql_pdf_rows)
        for batch_idx in range(total_batch_nbr) : 
            # downloading the report pdf files batch by batch
            result_rows = sql_cursor.fetchmany(BATCH_SIZE) # getting a chunk of rows from the db
            url_dict = { row[0] : row[1] for row in result_rows }
            report_contents = delay_fetch_url_batch(url_dict,BATCH_SIZE,0.5)
            print(f'\rdownloaded : { (batch_idx+1) *BATCH_SIZE}/{total_fetch_nbr} {math.floor(((batch_idx+1) /total_batch_nbr)*100)}% elapsed time:{pdf_fetch_duration.elapsed_time_str()}',end='',flush=True)

            sql_wo_report_update = 'UPDATE wo_report SET wo_report_pdf_byte_size = ?, wo_report_pdf_sha512_digest = ? WHERE wo_id = ?'
            #computing the report binary size and digest
            #data_update = tuple((len(report_contents[wo_id]), b64_sha512_digest(report_contents[wo_id]), wo_id) for wo_id in report_contents)
            data_update = [(len(report_contents[wo_id]), b64_sha512_digest(report_contents[wo_id]), wo_id) for wo_id in report_contents]
            
            update_cur = sqlite_db.executemany(sql_wo_report_update,data_update)
            sqlite_db.commit()

            # writing the pdf report file to disk
             
            print(f'writing files to disk...')
            BASE_ARCHIVE_DIR = Path('./PRAXEDO_ARCHIVE')
            for row in result_rows:
                wo_id = row[0]
                wo_date = datetime.fromisoformat(row[2])
                sap_or = row[3]
                sap_lc = row[4]
                file_name = f'{wo_date.strftime(r'%Y-%m-%d')}_OT-{wo_id}-FI_OR-{sap_or}_LC-{sap_lc}.pdf'
                dir_path = BASE_ARCHIVE_DIR / str(wo_date.year) / f'{wo_date.month:02d}'
                dir_path.mkdir(parents=True, exist_ok=True)
                full_path = dir_path / file_name
                print(f'report to disk : {file_name}')
                full_path.write_bytes(report_contents[wo_id]) # writing the pdf report to the disk

                # getting the list of attachements files
                try : 
                    attach_list = wo_attachments.query('wo_id == @wo_id')
                    for idx, row in enumerate(attach_list.itertuples()) :
                        attach_bin = praxedoWS.get_attachement_content(row.attach_id)
                        digest = b64_sha512_digest(attach_bin)
                        
                        # updating the dataframe and db with the digest
                        update_mask = wo_attachments['attach_id'] == row.attach_id
                        wo_attachments.loc[update_mask,'attach_sha512_digest'] = digest
                        sql_digest_update = 'UPDATE wo_attachments SET attach_sha512_digest = ? WHERE attach_id = ?'
                        sqlite_db.execute(sql_digest_update,(digest, row.attach_id))
                        
                        file_prefix = f'{wo_date.strftime(r'%Y-%m-%d')}_OT-{wo_id}-PJ{idx+1}_'
                        file_name = file_prefix + row.attach_name # type: ignore
                        full_path = dir_path / file_name
                        print(f'attachment to disk : {file_name}')
                        full_path.write_bytes(attach_bin)
                except Exception as e :
                    print(f'exception while downlowding attachments.. {e} ')
            

    pdf_fetch_duration.stop()
    print(f'total: wo nbr:{total_wo_nbr} pdf fetch duration:{pdf_fetch_duration.total_time_str()}')

total_duration.stop()
print(f'total process duration:{total_duration.total_time_str()}')

