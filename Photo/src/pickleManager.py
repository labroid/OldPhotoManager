'''
Created on Dec 25, 2011

@author: scott_jackson
'''
import sys
import os.path
from cPickle import Pickler, Unpickler
from photoUtils import printNow
from stopwatch import stopWatch

class photoPickler:
    def __init__(self, picklePath):
        self.pickleExists = False
        self.picklePath = picklePath
        self.pickler = ''
        self.unpickler = ''
        self.pickler_fp = ''
        self.unpickler_fp = ''
        self.checkPickleExists()
        
    def checkPickleExists(self):
        print "Checking for existing pickle..."
        if os.path.exists(self.picklePath):
            self.pickleExists = True
        else:
            self.pickleExists = False 
        return()
        
    def getPickleFile(self, fileMode):
        try:
            pickle_fp = open(self.picklePath, fileMode)
        except IOError as (errno, strerror):
            print 'Problem opening path:', self.picklePath, strerror
            sys.exit(1)
        return(pickle_fp)
    
    def loadPickle(self):
        timer = stopWatch()
        timer.start()
        printNow("Unpacking pickle.")
        pickle_fp = self.getPickleFile('rb')
        unpickler = Unpickler(pickle_fp)
        package = unpickler.load()
        pickle_fp.close()
        printNow("Unpickle done.  Elapsed time: " + str(timer.read()))
        return(package)
            
    def dumpPickle(self, archive):
        timer = stopWatch()
        timer.start()
        printNow("Pickling latest results.")
        pickle_fp = self.getPickleFile('wb')
        pickler = Pickler(pickle_fp, protocol=2)
        pickler.dump(archive)
        pickle_fp.close()
        printNow("Pickling complete.  Elapsed time: " + str(timer.read()) + " seconds.")
        return()