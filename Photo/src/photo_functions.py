'''
Created on Nov 23, 2011

@author: scott_jackson
'''

import os
import datetime
import logging
import pyexiv2
import collections
import time
from fileMD5sum import stringMD5sum, fileMD5sum
from photo_utils import print_now
import stopwatch

#def isNodeInArchive(archive, node):
#    if archive.path == node.path:
#        print "Error:  Node and Archive have the same root"
#        return(False) #By definition the node is in the archive since they are the same.  However return False so no one assumes it is a copy and deletes the Archive
#    for root, dirs, files in os.walk(node.path, topdown=False):
#        allFilesInArchive = True
##        candidate = candidateData()  #This might be better just using variables and not an object
#        for file in files:
#            FileInArchive = False
#            nodeFile = os.path.join(root,file)
##            timeCandidates = [k for k, v in archive.data.iteritems() if v.timestamp == node.data[nodeFile].timestamp]
##            timeCandidates = findSameTimestamp(archive, node.data[nodeFile].timestamp)
#            node.data[nodeFile].candidates = []
#            for archiveFile in archive.data.keys():
#                if nodeFile != archiveFile:  #Avoids checking oneself if Node is in Archive - but allows discovery of duplicates elsewhere in archive
#                    candidatePath = candidateThumbMD5same = candidateThumbAndTagsSame = candidateFileMD5same = None
#                    if archive.data[archiveFile].thumbnailMD5 == node.data[nodeFile].thumbnailMD5:
#                        candidatePath = archiveFile
#                        candidateThumbMD5same = True
#                        if archive.data[archiveFile].userTags != node.data[nodeFile].userTags:
#                            candidateThumbAndTagsSame = False
#                        else:
#                            candidateThumbAndTagsSame = True
#                            if fileMD5sum(archiveFile) == fileMD5sum(nodeFile):  #Recomputes archive MD5 repeatedly
#                                candidateFileMD5same = True
#                                node.data[nodeFile].inArchive = True
#                                FileInArchive = True
#                            else:
#                                candidateFileMD5same = False
#                                node.data[nodeFile].inArchive = False
#                        node.data[nodeFile].candidates.append([candidatePath, candidateThumbMD5same, candidateThumbAndTagsSame, candidateFileMD5same])
#            allFilesInArchive = allFilesInArchive and FileInArchive
##            print ".",
##    print nodeFile,"?=?",archiveFile,":",node.data[nodeFile].candidates,"MD5",candidateFileMD5same,"Tags",candidateThumbAndTagsSame
#    allDirsInArchive = True
#    node.data[root].inArchive = True
#    nodeInArchive = True
#    for dir in dirs:
#        nodeInArchive = node.data[dir].inArchive and nodeInArchive
#    return(nodeInArchive)
##            pprint.pprint(timeCandidates)
#    #This should return something (list of candidates?)



def count_unique_photos(archive):
    print "Counting unique photos"
    timer = stopwatch.stopWatch()
    dup_count = 0
    dircount = 0
    filecount = 0
    timer.start()
    photo_set = set()
    for archive_file in archive.data.keys():
        if archive.data[archive_file].dirflag:
            dircount += 1
        else:
            filecount += 1
            md5 = archive.data[archive_file].thumbnailMD5
            if md5 in photo_set:
                dup_count += 1
            else:
                photo_set.add(md5)
    unique_count = len(photo_set)
    print "Total time:", timer.read(), "or", timer.read() / (filecount + dircount) * 1000000.0, "us/file"
    uniques = collections.namedtuple('Uniques',['dircount','filecount','unique_count','dup_count','dup_fraction'])
    return(uniques(dircount, filecount, unique_count, dup_count, dup_count * 1.0 / filecount))


      
def isNodeInArchive(archive, node):  #Check "in archive" logic and make sure it is right!!
    if archive.path == node.path:
        print "Error:  Node and Archive must have different root paths"
        return(False) #By definition the node is in the archive since they are the same.  However return False so no one assumes it is a copy and deletes the Archive    

#Create a dictionary using thumbMD5s as the keys for fast lookup    
    archiveTable = {}
    for archiveFile in archive.data.keys():
        if not archive.data[archiveFile].thumbnailMD5 in archiveTable: 
            archiveTable[archive.data[archiveFile].thumbnailMD5] = [archiveFile]
        else:
            archiveTable[archive.data[archiveFile].thumbnailMD5].append(archiveFile)
            
    allFilesInArchive = True  #Seed value; logic will falsify if any files missing     
    for root, dirs, files in os.walk(node.path, topdown=False):
        for nodeFile in files:
            candidateFile = os.path.join(root,nodeFile)
            node.data[candidateFile].candidates = []
            node.data[candidateFile].inArchive = False #Assume it is not in archive unless proven otherwise
            if node.data[candidateFile].thumbnailMD5 in archiveTable:
                for archiveFile in archiveTable[node.data[candidateFile].thumbnailMD5]:
                    if archiveFile != candidateFile:  #Don't compare to oneself in case candidate path is in archive path  
                        if archive.data[archiveFile].userTags != node.data[candidateFile].userTags:
                            candidateThumbAndTagsSame = False
                        else:
                            candidateThumbAndTagsSame = True
                            if archive.getFileMD5(archiveFile) == node.getFileMD5(candidateFile):
                                node.data[candidateFile].inArchive = True
                        node.data[candidateFile].candidates.append([archiveFile, candidateThumbAndTagsSame, node.data[candidateFile].inArchive])
            allFilesInArchive = allFilesInArchive and node.data[candidateFile].inArchive
    
        if not allFilesInArchive:
            nodeInArchive = False
        else:
            nodeInArchive = True
            for nodeDir in dirs:
                nodeInArchive = node.data[nodeDir].inArchive and nodeInArchive
            node.data[root].inArchive = nodeInArchive
    return(node.data[root].inArchive) 
            
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
