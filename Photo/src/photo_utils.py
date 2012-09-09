'''
Created on Dec 25, 2011

@author: scott_jackson
'''
import sys
import ConfigParser
import socket
import logging

def print_now(string):
    print string
    sys.stdout.flush()
    return()

class environment:
    '''Read environment file and establish environment variable dictionary'''
    def __init__(self, config_file):
        self.__options = {}     
        config = ConfigParser.ConfigParser()
        config.read(config_file)
        machine_name = socket.gethostname()
        if machine_name not in config.sections():
            raise NameError("Machine name {0} not known: add info to configuration file".format(machine_name))
        for option in config.options(machine_name):
            self.__options[option] = config.get(machine_name, option)
        self.__options['machinename'] = machine_name
        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
        logging.basicConfig(filename = self.__options['logfile'], format = LOG_FORMAT, level = logging.DEBUG, filemode = 'w')
        self.logger = logging.getLogger()
        self.logger.info("Configuration file read; environment variables established for machine {0}".format(machine_name))
        return
                          
    def get(self,variable):
        if variable in self.__options:
            return(self.__options[variable])
        else:
            err_msg = "Environment variable doesn't exist: {0}".format(variable)
            self.logger.error(err_msg)
            raise NameError(err_msg)
        