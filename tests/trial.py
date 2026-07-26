# -- Import hack ----------------------------------------------------------
import os, sys
# Add src to sys.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
#-----------------------------------------------------------------------------

from datetime import datetime

if __name__ == "__main__":
    print('program start')
    
    curr_date_str = datetime.now().isoformat(timespec='seconds')
    print(f'current date = {curr_date_str}')
    
    print('program end')