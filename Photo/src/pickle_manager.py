'''
Created on Dec 25, 2011

@author: scott_jackson

TODO:  make pickle print pretty.  Also consider zipping it.
'''
import sys
import os.path
import logging
from photo_data import photo_collection, node_info

from cPickle import Pickler, Unpickler
#import jsonpickle
#import json

#class photo_pickler(photo_collection):
class photo_pickler():
    def __init__(self, picklePath):
        self.logger = logging.getLogger()
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
            self.logger.warn('Pickle path does not exist: {0}'.format(self.picklePath))
        return()
        
    def getPickleFile(self, fileMode):
        try:
            pickle_fp = open(self.picklePath, fileMode)
        except IOError as (errno, strerror):
            self.logger.critical('Exiting: Fatal Error opening pickle at:{0}'.format(self.picklePath, strerror))
            sys.exit(1)
        except:
            self.logger.critical("Exiting:  Fatal Error opening pickle at:".format(self.picklePath))
            sys.exit(1)
        return(pickle_fp)
    
    def loadPickle(self):
        self.logger.info("Unpacking pickle at {0}".format(self.picklePath))
        pickle_fp = self.getPickleFile('rb')
        unpickler = Unpickler(pickle_fp)
        package = unpickler.load()
#        package = jsonpickle.decode(pickle_fp.read())
#        package = json.load(pickle_fp)
        pickle_fp.close()
        self.logger.info("Pickle at {0} unpacked.".format(self.picklePath))
        return(package)
            
    def dumpPickle(self, archive):
        self.logger.info("Pickling latest results to {0}.".format(self.picklePath))
        pickle_fp = self.getPickleFile('wb')
        pickler = Pickler(pickle_fp, protocol=2)
        pickler.dump(archive)
#        pickle_fp.write(jsonpickle.encode(archive))
#        json.dump(archive, pickle_fp)
        pickle_fp.close()
        self.pickleExists = True
        self.logger.info("Pickling complete to {0}".format(self.picklePath))
        return()