from pprint import pprint
import sqlite3
import os, sys
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
for idx, one_day_wo_list in enumerate(fetch_wo_week_by_day(2,2025)):
    result_size = len(one_day_wo_list)
    if result_size > 0:
        total_wo_nbr += result_size
        # normalize the result
        nz_result = normalize_ws_response(one_day_wo_list)
        # accumulate the results into a list
        nz_results.append(nz_result)
    
    day_duration.stop()
    print(f'day {idx+1}:total duration:{day_duration.get_duration_str()} number of work orders: {result_size}')
    #if idx == 1 : break # DEBUG
    print(f'day {idx+2}...')
    day_duration.start()

# merging results
if len(nz_results) > 0:
    wo_core         = pd.concat([elt.wo_core for elt in nz_results])
    wo_report       = pd.concat([elt.wo_report for elt in nz_results])
    wo_report_imgs  = pd.concat([elt.wo_report_imgs for elt in nz_results])

    print(f'total work order results nbr:{total_wo_nbr}')

# writing the normalized form to the db
if len(nz_results) > 0:
# write the result to db
    with sqlite3.connect(f'fetch_result.sqlite3') as conn:

        # making the "id" column a primary key
        wo_core_dtype = {'id': 'INTEGER PRIMARY KEY'}
        wo_core.to_sql('wo_core', conn, dtype=wo_core_dtype, if_exists='replace',index=False) # type: ignore
        
        wo_report_dtype = {'wo_id':'INTEGER UNIQUE REFERENCES wo_core(id)'}
        wo_report.to_sql('wo_report', conn, dtype=wo_report_dtype, if_exists='replace',index=False) # type: ignore

        wo_report_imgs_dtype = {'wo_id':'INTEGER UNIQUE REFERENCES wo_report(wo_id)'}
        wo_report_imgs.to_sql('wo_report_imgs', conn, dtype=wo_report_imgs_dtype, if_exists='replace',index=False) # type: ignore


        # clearing memory
        wo_core = wo_report = wo_report_imgs = None

        # download pdf report and update the wo_report table
        BATCH_SIZE = 20
        select_sql = 'SELECT wo_id, wo_report_url FROM wo_report WHERE wo_report_pdf_bin IS NULL'
        count_sql =  f'SELECT COUNT(*) FROM ({select_sql})'
        update_sql = 'UPDATE wo_report SET wo_report_pdf_bin = ? WHERE wo_id = ?'

        total_fetch_nbr, = conn.execute(count_sql).fetchone()
        print(f'total number of reports to fetch :{total_fetch_nbr}')

        select_cur = conn.execute(select_sql)
        for batch_nbr in range((total_fetch_nbr + BATCH_SIZE -1) // BATCH_SIZE) : 
            # downloading the report pdf files
            batch = select_cur.fetchmany(BATCH_SIZE)
            contents = next(batch_fetch_url(batch,BATCH_SIZE))
            updat_cur = conn.executemany(update_sql,contents)
    
    total_duration.stop()
    print(f'total: wo nbr:{total_wo_nbr} fetch duration:{total_duration.get_duration_str()}')

# pprint(total_wo_nz_result)
