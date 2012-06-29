'''
Created on Nov 3, 2011

@author: scott_jackson
'''
import time

class stopWatch():
        
    def __init__(self):
        self._startTime = time.clock()
        self._accumulatedTime = 0
        return
           
    def start(self):
        self._startTime = time.clock()
        return     
    
    def stop(self):
        self._accumulatedTime += time.clock() - self._startTime
        self._startTime = 0
        return     
   
    def read(self):
        if self._startTime != 0:
            return(time.clock() - self._startTime) + self._accumulatedTime
        else:
            return(self._accumulatedTime)
       
    def reset(self):
        self.__init__()
        return

