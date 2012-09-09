'''
Created on Oct 18, 2011

@author: scott_jackson
'''
import hashlib
import logging
import os

def fileMD5sum(filePath):
    logger = logging.getLogger()
    try:
        fp = open(filePath, 'rb')
    except:
        logger.warning("Couldn't open file {0}.  Setting to default".format(filePath))
        return("FFFFFFFFFFFFFFFF")
    m = hashlib.md5()
    while True:
        data = fp.read(8192)
        if not data:
            break
        m.update(data)
    fp.close()
    return m.hexdigest() 

def stringMD5sum(string):
    m = hashlib.md5()
    m.update(string)
    return m.hexdigest()

def truncatedMD5sum(filepath, length = 1048578):
    ''' given a length (default to 1M bit), compute the MD5 sum on the
    string formed by the first length/2 and last length/2 characters of
    the file.  Used, for example, to get a pretty good signature for a
    large .mov file
    '''
    logger = logging.getLogger()
    try:
        fp = open(filepath, 'rb')
    except:
        logger.warning("Couldn't open file {0}.  Setting MD5 sum to default".format(filepath))
        return("FFFFFFFFFFFFFFFF")
    if os.path.getsize(filepath) > length:
        data = fp.read(length/2)
        fp.seek(-length/2, os.SEEK_END)
        data = data + fp.read(length/2)
    else:
        data = fp.read()
    fp.close()
    truncated_sum = stringMD5sum(data)
    return truncated_sum 
        