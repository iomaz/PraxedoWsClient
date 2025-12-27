
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
    
    def elapsed_time_str(self):
        time_from_start = time.perf_counter() - self.start_time # unit [s]
        minutes = int(time_from_start)//60
        
        exec_time_str = f'{ str(minutes) + "[min]" if minutes > 0 else ""}{round(time_from_start - minutes*60)}[s]'
        return exec_time_str

    def total_time_str(self):
        duration = self.stop_time - self.start_time # unit [s]
        minutes = int(duration)//60
        
        exec_time_str = f'{ str(minutes) + "[min]" if minutes > 0 else ""}{round(duration - minutes*60)}[s]'
        return exec_time_str