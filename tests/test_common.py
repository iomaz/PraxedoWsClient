
import time

class SimplePerfClock:
    '''
    Role
    compute the time between start() and stop()
    '''    
    start_time  : float
    stop_time   : float

    def start(self):
        self.start_time = time.perf_counter()
    
    def stop(self):
        self.stop_time = time.perf_counter()
    
    def get_duration_str(self):
        exec_time = self.stop_time - self.start_time # unit [s]
        # exec_time_str = f'{exec_time:.2f}[s]'
        exec_time_60 = int(exec_time)//60
        
        exec_time_str = f'{ str(exec_time_60) + "[min]" if exec_time_60 > 0 else ""}{round(exec_time - exec_time_60*60)}[s]'
        
        return exec_time_str