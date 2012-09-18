'''
Created on Dec 25, 2011

@author: scott_jackson
'''
import sys
import os.path
import logging
import pprint
from cPickle import Pickler, Unpickler

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
        logger = logging.getLogger()
        if os.path.exists(self.picklePath):
            self.pickleExists = True
        else:
            self.pickleExists = False
            logger.warn('Pickle path does not exist: {0}'.format(self.picklePath))
        return()
        
    def getPickleFile(self, fileMode):
        logger = logging.getLogger()
        try:
            pickle_fp = open(self.picklePath, fileMode)
        except IOError as (errno, strerror):
            logger.critical('Exiting: Fatal Error opening pickle at:{0}'.format(self.picklePath, strerror))
            sys.exit(1)
        except:
            logger.critical("Exiting:  Fatal Error opening pickle at:".format(self.picklePath))
            sys.exit(1)
        return(pickle_fp)
    
    def loadPickle(self):
        logger = logging.getLogger()
        logger.info("Unpacking pickle at {0}".format(self.picklePath))
        pickle_fp = self.getPickleFile('rb')
        unpickler = Unpickler(pickle_fp)
        pprint.pprint(sys.modules)
        package = unpickler.load()
        pickle_fp.close()
        logger.info("Pickle at {0} unpacked.".format(self.picklePath))
        return(package)
            
    def dumpPickle(self, archive):
        logger = logging.getLogger()
        logger.info("Pickling latest results to {0}.".format(self.picklePath))
        pickle_fp = self.getPickleFile('wb')
        pickler = Pickler(pickle_fp, protocol=2)
        pickler.dump(archive)
        pickle_fp.close()
        logger.info("Pickling complete to {0}".format(self.picklePath))
        return()