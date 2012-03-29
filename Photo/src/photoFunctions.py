'''
Created on Nov 23, 2011

@author: scott_jackson

Finding if a node is represented in an archive
    [TODO: take care of special case of a file and not a directory]
    traverse candidate tree and get sizes and mtimes (SumFileandDirSize())
    traverse candidate tree
        for each file 
            get EXIF tags
            search archive for file with same timestamp
            if no files with same timestamp in archive
                mark candidate file for inclusion in archive
            else
                if file checksums are identical
                    mark candidate as included
                else
                    compute photo signature (thumbnail checksum, tag checksum)
                    if thumbnail checksums different [Watch out, thumnail might be start/lengt]
                        mark candidate for inclusion in archive
                    else
                        if tags are identical
                            mark candidate as included
                            note strangeness (thumbnails and tags same but file checksum different)
                        else
                            mark for review by user
            accululate status for root (all_included, all_include)
        
        for each directory
            accumulate status for root (all_included, all_include)
                mark directory as included
            
        for root mark with accumulated status
'''

import os
import datetime
import logging
import pyexiv2
import pprint
from fileMD5sum import stringMD5sum, fileMD5sum

class candidateData():
    def __init__(self):
        self.path = ''
        self.fileMD5same = False
        self.thumbMD5same = False
        self.thumbAndTagsSame = False
        
    def clear(self):
        self.__init__()

def isNodeInArchive(archive, node):
    if archive.path == node.path:
        print "Error:  Node and Archive have the same root"
        nodeInArchive = False
        return(nodeInArchive) #By definition the node is in the archive since they are the same.  However return False so no one assumes it is a copy and deletes the Archive
    for root, dirs, files in os.walk(node.path, topdown=False):
        allFilesInArchive = True
        candidate = candidateData()
        for file in files:
            FileInArchive = False
            nodeFile = os.path.join(root,file)
#            timeCandidates = [k for k, v in archive.data.iteritems() if v.timestamp == node.data[nodeFile].timestamp]
#            timeCandidates = findSameTimestamp(archive, node.data[nodeFile].timestamp)
            node.data[nodeFile].candidates = []
            for archiveFile in archive.data.keys(): 
                if nodeFile != archiveFile:  #Avoids checking oneself if Node is in Archive - but allows discovery of duplicates elsewhere in archive
                    candidate.clear()
                    if archive.data[archiveFile].thumbnailMD5 == node.data[nodeFile].thumbnailMD5:
                        candidate.path = archiveFile
                        candidate.thumbMD5same = True
                        if archive.data[archiveFile].userTags == node.data[nodeFile].userTags:
                            candidate.thumbAndTagsSame = True
                            if fileMD5sum(archiveFile) == fileMD5sum(nodeFile):  #Might be faster to use MD5sum from data structure
                                candidate.fileMD5same = True
                                node.data[nodeFile].inArchive = True
                                FileInArchive = True
                        node.data[nodeFile].candidates.append(candidate)
            print nodeFile,"?=?",archiveFile,":",node.data[nodeFile].candidates
            allFilesInArchive = allFilesInArchive and FileInArchive
        allDirsInArchive = True
        node.data[root].inArchive = True
        nodeInArchive = True
        for dir in dirs:
            nodeInArchive = node.data[dir].inArchive and nodeInArchive
    return(nodeInArchive)
#            pprint.pprint(timeCandidates)
    #This should return something (list of candidates?)
            
def findSameTimestamp(collection, targetTime):
    candidateList = []
#    filterstring = os.path.sep + targetTime.strftime('%Y') + os.path.sep + targetTime.strftime('%Y%m%d')
#    filterstring = os.path.sep + targetTime.strftime('%Y%m%d')
#    print "filterstring=",filterstring
    for filename in collection.data.keys():
        print filename, "-->",collection.data[filename].tags['EXIF DateTimeOriginal'],"<--",collection.data[filename].timestamp, targetTime
        if collection.data[filename].timestamp == targetTime:
            candidateList.append(filename)

#        if filterstring in filename:
#            candidateList.append(filename)
    return(candidateList)
            

#    candidateFiles = filter(lambda x:filterstring in x, collection.data.keys())
#    print "candidateFiles=",candidateFiles
#    for file in candidateFiles:
#        print "Candidate:",file
    
    
                
#def getTagsFromFile(filename):    #Look into pyexif2 here for better speed?
#    try:
#        fp = open(filename,'rb')
#    except:
#        print "getTagsFromFile():", filename, "can't be opened."
#        return({})
#    tags = EXIF.process_file(fp, details = False)
##    fp.close()  #Runs faster w/o closing the files.  At least on 4500 files.
#    return(tags)

def getTagsFromFile(filename):
    logger = logging.getLogger('getTagsFromFile')
    try:
        metadata = pyexiv2.ImageMetadata(filename)
        metadata.read()
#    except ValueError:
#        print "getTagsFromFile():", filename, "ValueError"
#        return(None)
#    except KeyError:
#        print "getTagsFromFile():", filename, "KeyError"
#        return(None)
#    except TypeError:
#        print "getTagsFromFile():", filename, "TypeError"
#        return(None)
    except IOError as err:  #The file contains data of an unknown image type or file missing or can't be opened
        logger.warning("getTagesFromFile(): %s IOError, errno = %s, strerror = %s args = %s", filename, str(err.errno), err.strerror, err.args)
        return(None)
    except:
        logger.error("getTagesFromFile(): %s Unknown Error Trapped, errno = %s, strerror = %s args = %s", filename, str(err.errno), err.strerror, err.args)
        return(None)
    return(metadata)

def getTimestampFromTags(tags):
    if 'Exif.Photo.DateTimeOriginal' in tags.exif_keys:
        timestamp = tags['Exif.Photo.DateTimeOriginal'].value
    else:
        timestamp = datetime.datetime.strptime('1800:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
    return(timestamp)

def thumbnailMD5sum(tags):
    if len(tags.previews) > 0:
        return stringMD5sum(tags.previews[0].data)
    else:
        return stringMD5sum("")
    
def getUserTagsFromTags(tags):
        if 'Xmp.dc.subject' in tags.xmp_keys:
            return(tags['Xmp.dc.subject'].value)
        else:
            return('')
        
            
#def getTimestampFromTagsOld(tags):
#    defaultTimestamp = datetime.datetime.strptime('1950:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
#    if 'EXIF DateTimeOriginal' in tags:
#        try:
#            timestamp = datetime.datetime.strptime(tags['EXIF DateTimeOriginal'].values,'%Y:%m:%d %H:%M:%S')
#            success = 0 #Make this importable constant
#            message = ''
#        except ValueError:
#            message = tags['EXIF DateTimeOriginal'].values
#            timestamp = defaultTimestamp
#            success = 1 #Make this importable constant
#    else:
#        message = 'No Timestamp'
#        timestamp = defaultTimestamp
#        success = 2 #Make this importable constant
#    return(timestamp, success, message)
