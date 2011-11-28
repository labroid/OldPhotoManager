'''
Created on Oct 18, 2011

@author: scott_jackson
'''
import hashlib

def fileMD5sum(filePath):
    try:
        fp = open(filePath, 'rb')
    except:
        #TODO:  Log a file opening failure here
        return("FFFFFFFFFFFFFFFF")
    m = hashlib.md5()
    while True:
        data = fp.read(8192)
        if not data:
            break
        m.update(data)
    return m.hexdigest() 

def stringMD5sum(string):
    m = hashlib.md5()
    m.update(string)
    return m.hexdigest()