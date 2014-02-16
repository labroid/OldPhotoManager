'''
Created on Dec 25, 2011

@author: scott_jackson

TODO:  make pickle print pretty.  Also consider zipping it.
'''

#pylint: disable=line-too-long

import sys
import os.path
import logging

from cPickle import Pickler, Unpickler
from pickle import PickleError, UnpicklingError

class PhotoPickler(object):
    def __init__(self, pickle_path):
        self.pickle_path = pickle_path        
        self._check_pickle_path_exists()
        
    def _check_pickle_path_exists(self):
        if os.path.exists(self.pickle_path):
            self.pickle_exists = True
        else:
            self.pickle_exists = False
            logging.warn('Pickle path does not exist: {0}'.format(self.pickle_path))
        return
        
    def _get_pickle_file_pointer(self, file_mode):
        try:
            pickle_fp = open(self.pickle_path, file_mode)
        except IOError as (_, strerror):
            logging.critical('Exiting: Fatal Error opening pickle at:{0}'.format(self.pickle_path, strerror))
            sys.exit(1)
        except:
            logging.critical("Exiting:  Fatal Error opening pickle at:".format(self.pickle_path))
            sys.exit(1)
        return(pickle_fp)
    
    def load_pickle(self):
        if not self._check_pickle_path_exists():
            logging.error("Pickle does not exist at {}".format(self.pickle_path))
            sys.exit(1)
        logging.info("Unpacking pickle at {0}".format(self.pickle_path))
        pickle_fp = self._get_pickle_file_pointer('rb')
        unpickler = Unpickler(pickle_fp)
        try:
            package = unpickler.load()
        except (UnpicklingError, AttributeError, EOFError, ImportError, IndexError) as err:
            err_msg = "Unpickling failed.  Error message: {}".format(repr(err))
            logging.critical(err_msg)
            print err_msg
            sys.exit(1)
        finally:
            pickle_fp.close()
        logging.info("Pickle at {0} unpacked.".format(self.pickle_path))
        return(package)
            
    def dump_pickle(self, archive):
        logging.info("Pickling latest results to {0}.".format(self.pickle_path))
        pickle_fp = self._get_pickle_file_pointer('wb')
        pickler = Pickler(pickle_fp, protocol=2)
        try:
            pickler.dump(archive)
        except PickleError as err:
            logging.critical("Pickling failure.  Error: {}".format(repr(err)))
        finally:
            pickle_fp.close()
        self.pickle_exists = True
        logging.info("Pickling complete to {0}".format(self.pickle_path))
        return()