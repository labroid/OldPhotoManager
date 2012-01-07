'''
Created on Dec 25, 2011

@author: scott_jackson
'''
class MachineItems():
    def __init__(self):
        self.archive = ''
        self.archivePickle = ''
    
#Machine names and paths
machineInfo={}
machineInfo['smithers'] = MachineItems()
machineInfo['smithers'].archive = "/home/shared/Photos/2011"
machineInfo['smithers'].archivePickle = "/home/scott/PhotoPythonFiles/pickle.txt"
machineInfo['4DAA1001312'] = MachineItems()
machineInfo['4DAA1001312'].archive = "C:\Users\scott_jackson\Pictures"
machineInfo['4DAA1001312'].archivePickle = "C:\Users\scott_jackson\Desktop\PhotoPickle.txt"