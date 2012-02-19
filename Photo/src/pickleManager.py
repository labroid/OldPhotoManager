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
        
    def checkPickleExists(self, verbose = False):
        if verbose:  print "Checking for existing pickle..."
        if os.path.exists(self.picklePath):
            self.pickleExists = True
        else:
            self.pickleExists = False 
        return()
        
    def getPickleFile(self, fileMode):
        try:
            pickle_fp = open(self.picklePath, fileMode)
#        except IOError as (errno, strerror):
#            print 'Problem opening path:', self.picklePath, strerror
        except:
            print "Problem opening:", self.picklePath
            sys.exit(1)
        return(pickle_fp)
    
    def loadPickle(self, verbose = False):
        timer = stopWatch()
        timer.start()
        if verbose: printNow("Unpacking pickle.")
        pickle_fp = self.getPickleFile('rb')
        unpickler = Unpickler(pickle_fp)
        package = unpickler.load()
        pickle_fp.close()
        if verbose: printNow("Unpickle done.  Elapsed time: " + str(timer.read()))
        return(package)
            
    def dumpPickle(self, archive, verbose = False):
        timer = stopWatch()
        timer.start()
        if verbose: printNow("Pickling latest results.")
        pickle_fp = self.getPickleFile('wb')
        pickler = Pickler(pickle_fp, protocol=2)
        pickler.dump(archive)
        pickle_fp.close()
        if verbose: printNow("Pickling complete.  Elapsed time: " + str(timer.read()) + " seconds.")
        return()