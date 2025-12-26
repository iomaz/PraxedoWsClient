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


def fetch_work_order_week_by_day(arg_week_nbr:int, arg_year:int):
    
    # getting all search periods
    week_days_period = get_week_days(arg_week_nbr, arg_year)

    EXTENDED_RESULT = PraxedoSoapClient.SRCH_WO_RESULT_OPTION.EXTENDED
    WO_COMPLETED    = PraxedoSoapClient.DATE_CONSTRAINT.COMPLETION
    # searching and fetching each day and get the 
    for day_start, day_stop in week_days_period:
        wo_entities = praxedoWS.search_work_orders(WO_COMPLETED,day_start, day_stop,EXTENDED_RESULT) # type: ignore
        yield wo_entities

week_duration = SimplePerfClock()
day_duration = SimplePerfClock()
nz_results = []
total_wo_nbr = 0
week_duration.start()
day_duration.start()
for idx, one_day_wo_list in enumerate(fetch_work_order_week_by_day(4,2019)):
    result_size = len(one_day_wo_list)
    if result_size > 0:
        total_wo_nbr += result_size
        # normalize the result
        nz_result = normalize_ws_response(one_day_wo_list)
        # accumulate the results into a list
        nz_results.append(nz_result)
        # DEBUG
        if idx == 0: break
    
    day_duration.stop()
    print(f'day {idx+1}:total duration:{day_duration.get_duration_str()} number of work orders: {result_size}')
    #if idx == 1 : break # DEBUG
    day_duration.start()

# merging results
if len(nz_results) > 0:
    wo_core         = pd.concat([elt.wo_core for elt in nz_results])
    wo_report       = pd.concat([elt.wo_report for elt in nz_results])
    wo_report_imgs  = pd.concat([elt.wo_report_imgs for elt in nz_results])

week_duration.stop()
print(f'total: wo nbr:{total_wo_nbr} fetch duration:{week_duration.get_duration_str()}')

if len(nz_results) > 0:
    wo_report_pdf_urls = list(zip(wo_report['wo_id'],wo_report['wo_report_url'])) # type: ignore

    report_contents = next(batch_fetch_url(wo_report_pdf_urls,2))

    with open(f'OT_{report_contents[0][0]}.pdf', "wb") as file:
        file.write(report_contents[0][1]) # type: ignore

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

# pprint(total_wo_nz_result)
