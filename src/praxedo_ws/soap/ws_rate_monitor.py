
from datetime import datetime, timedelta
from collections import deque


class WsRateMonitor :
    
    def __init__(self, arg_max_hour_rate) :
        self.call_events = deque(maxlen = arg_max_hour_rate +1)        

    def sig_call(self) :
        self.call_events.append(datetime.now())
       
    def clear(self):
        self.call_events.clear()
        
    def compute_hour_rate(self) :
        
        now = datetime.now()
        one_hour = timedelta(hours = 1)
        
        # searching all event in the 1 hour window
        for refIdx, callEvt in enumerate(self.call_events) :
            evt_age = (now - callEvt) / one_hour
            if evt_age <= 1 : break
        
        return len(self.call_events) - refIdx
        