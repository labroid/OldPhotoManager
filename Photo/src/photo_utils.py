'''
Created on Dec 25, 2011

@author: scott_jackson
'''
import sys
import ConfigParser
import socket
import pprint

def print_now(string):
    print string
    sys.stdout.flush()
    return()

class environment:
    def __init__(self):
        #Identify the environment
        self.options = {}
        config = ConfigParser.ConfigParser()
        config.read('Configuration')
        machineNames = config.sections()
        machineName= socket.gethostname()
        if machineName not in machineNames:
            print "Machine name {0} not known: add info to configuration file".format(machineName)
            sys.exit(1)
        optionList = config.options(machineName)
        for option in optionList:
            self.options[option]=config.get(machineName, option)

