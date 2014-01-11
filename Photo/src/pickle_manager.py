'''
Created on Dec 25, 2011

@author: scott_jackson

TODO:  make pickle print pretty.  Also consider zipping it.
'''
import sys
import os.path
import logging
import photo_data

from cPickle import Pickler, Unpickler
from pickle import PickleError, UnpicklingError
#import jsonpickle
#import json

class photo_pickler():
    def __init__(self, picklePath):
        self.pickleExists = False
        self.picklePath = picklePath
        self.check_pickle_path_exists()
        
    def check_pickle_path_exists(self):
        if os.path.exists(self.picklePath):
            self.pickleExists = True
        else:
            self.pickleExists = False
            logging.warn('Pickle path does not exist: {0}'.format(self.picklePath))
        return(self.pickleExists)
        
    def _getPickleFile(self, fileMode):
        try:
            pickle_fp = open(self.picklePath, fileMode)
        except IOError as (errno, strerror):
            logging.critical('Exiting: Fatal Error opening pickle at:{0}'.format(self.picklePath, strerror))
            sys.exit(1)
        except:
            logging.critical("Exiting:  Fatal Error opening pickle at:".format(self.picklePath))
            sys.exit(1)
        return(pickle_fp)
    
    def loadPickle(self):
        if not self.check_pickle_path_exists():
            logging.error("Pickle does not exist at {}".format(self.picklePath))
            sys.exit(1)
        logging.info("Unpacking pickle at {0}".format(self.picklePath))
        pickle_fp = self._getPickleFile('rb')
        unpickler = Unpickler(pickle_fp)
        try:
            package = unpickler.load()
        except (UnpicklingError, AttributeError, EOFError, ImportError, IndexError) as e:
            err_msg = "Unpickling failed.  Error message: {}".format(repr(e))
            logging.critical(err_msg)
            print err_msg
            sys.exit(1)
        finally:
            pickle_fp.close()
        logging.info("Pickle at {0} unpacked.".format(self.picklePath))
        return(package)
            
    def dumpPickle(self, archive):
        logging.info("Pickling latest results to {0}.".format(self.picklePath))
        pickle_fp = self._getPickleFile('wb')
        pickler = Pickler(pickle_fp, protocol=2)
        pickler.dump(archive)
#        pickle_fp.write(jsonpickle.encode(archive))
#        json.dump(archive, pickle_fp)
        pickle_fp.close()
        self.pickleExists = True
        logging.info("Pickling complete to {0}".format(self.picklePath))
        return()