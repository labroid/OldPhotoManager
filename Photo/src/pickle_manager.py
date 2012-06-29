'''
Created on Dec 25, 2011

@author: scott_jackson
'''
import sys
import os.path
import logging
from cPickle import Pickler, Unpickler
from photo_utils import print_now
from stopwatch import stopWatch

class photo_pickler:
    def __init__(self, picklePath):
        self.pickleExists = False
        self.picklePath = picklePath
        self.pickler = ''
        self.unpickler = ''
        self._pickler_fp = ''
        self._unpickler_fp = ''
        self.check_pickle_path_exists()
        
    def check_pickle_path_exists(self):
        if os.path.exists(self.picklePath):
            self.pickleExists = True
        else:
            self.pickleExists = False 
        return()
        
    def getPickleFile(self, fileMode):
        try:
            pickle_fp = open(self.picklePath, fileMode)
        except IOError as (errno, strerror):
            print 'Exiting: Fatal Error opening pickle at:', self.picklePath, strerror
            sys.exit(1)
        except:
            print "Exiting:  Fatal Error opening pickle at:", self.picklePath
            sys.exit(1)
        return(pickle_fp)
    
    def loadPickle(self):
        logger = logging.getLogger(__name__)
        timer = stopWatch()
        timer.start()
        logger.info("Unpacking pickle at {0}".format(self.picklePath))
        pickle_fp = self.getPickleFile('rb')
        unpickler = Unpickler(pickle_fp)
        package = unpickler.load()
        pickle_fp.close()
        logger.info("Unpickle done.  Elapsed time: {0} seconds".format(timer.read()))
        return(package)
            
    def dumpPickle(self, archive):
        logger = logging.getLogger(__name__)
        timer = stopWatch()
        timer.start()
        logger.info("Pickling latest results.")
        pickle_fp = self.getPickleFile('wb')
        pickler = Pickler(pickle_fp, protocol=2)
        pickler.dump(archive)
        pickle_fp.close()
        logger.info("Pickling complete.  Elapsed time: {0} seconds".format(timer.read()))
        return()