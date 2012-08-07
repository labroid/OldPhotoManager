'''
Created on Nov 23, 2011

@author: scott_jackson
'''

import os
import sys
import logging
import datetime
from fileMD5sum import stringMD5sum
from pickle_manager import photo_pickler
from photoData import photoData
      
      
def populate_duplicate_candidates(archive, node, archive_path = None, node_path = None):
    ''' 
    Replaces the .candidates list property for each element of the node_path as follows:
    if archive and node are the same:
        if archive_path and node_path are same:
            .candidate property is updated for all but file 'selfs'
        else:
            .candidate property is updated for all but files under node_path tree
    else:
        .candidate property is updated for all files
    TODO:  This still needs work; not all collection/file combinations are supported
    '''    
    logger = logging.getLogger()
    if archive_path == None:
        archive_path = archive.path
    if node_path == None:
        node_path = node.path
    #Create a dictionary using file signatures as the keys for fast lookup
    logger.info("Building hash table for {0}".format(archive.path))    
    archiveTable = {}
    for archiveFile in archive.data.keys():
        if archive.data[archiveFile].signature in archiveTable: 
            archiveTable[archive.data[archiveFile].signature].append(archiveFile)
        else:
            archiveTable[archive.data[archiveFile].signature] = [archiveFile]
    logger.info("Done building hash table for {0}".format(archive.path))
#    for archivefile in archive.data.keys():
#        if archive.data[archivefile].signature in archiveTable:
#            print archivefile, archiveTable[archive.data[archivefile].signature]
#            archive.data[archivefile].candidates = archiveTable[archive.data[archivefile].signature]
#        else:
#            archive.data[archivefile].candidates = []
    logger.info("Finding duplicates...")
    for nodefile in node.data.keys():
        if node.data[nodefile].signature in archiveTable:
#            print nodefile, archiveTable[node.data[nodefile].signature]
            node.data[nodefile].candidates = archiveTable[node.data[nodefile].signature]
        else:
            node.data[nodefile].candidates = []
    logger.info("Done finding duplicates")
    return
    for nodefile in node.data.keys():
        if archive == node:
            if archive_path == node_path:  #Remove 'self' from list
                if nodefile in node.data[nodefile].candidates:
                    node.data[nodefile].candidates.remove(nodefile)
            else:  #Remove all candidates that share the node_path root
                logger.error("***STUBBED OFF*** Finish this function before doing subdirectory compares between collections")
    logger.info("Done populating candidate properties")
                
def isNodeInArchive(archive, node):  #Check "in archive" logic and make sure it is right!!  
    if archive.path == node.path and archive.host == node.host:  #Made this host sensitive; make sure still works for internal compares
        print "Error:  Node and Archive must have different root paths"
        return(False) #By definition the node is in the archive since they are the same.  However return False so no one assumes it is a copy and deletes the Archive    

#Create a dictionary using thumbMD5s as the keys for fast lookup    
    archiveTable = {}
    for archiveFile in archive.data.keys():
        if archive.data[archiveFile].signature in archiveTable: 
            archiveTable[archive.data[archiveFile].signature].append(archiveFile)
        else:
            archiveTable[archive.data[archiveFile].signature] = [archiveFile]
            
    allFilesInArchive = True  #Seed value; logic will falsify if any files missing     
    for root, dirs, files in os.walk(node.path, topdown=False):
        for nodeFile in files:
            candidateFile = os.path.join(root,nodeFile)
            node.data[candidateFile].candidates = []
            node.data[candidateFile].inArchive = False #Assume it is not in archive unless proven otherwise
            if node.data[candidateFile].signature in archiveTable:
                for archiveFile in archiveTable[node.data[candidateFile].signature]:
                    if archiveFile != candidateFile:  #Don't compare to oneself in case candidate path is in archive path  
                        if archive.data[archiveFile].userTags != node.data[candidateFile].userTags:
                            candidateThumbAndTagsSame = False
                        else:
                            candidateThumbAndTagsSame = True
                            if archive.get_file_signature(archiveFile) == node.get_file_signature(candidateFile):
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
            
#def findSameTimestamp(collection, targetTime):
#    candidateList = []
##    filterstring = os.path.sep + targetTime.strftime('%Y') + os.path.sep + targetTime.strftime('%Y%m%d')
##    filterstring = os.path.sep + targetTime.strftime('%Y%m%d')
##    print "filterstring=",filterstring
#    for filename in collection.data.keys():
#        print filename, "-->",collection.data[filename].tags['EXIF DateTimeOriginal'],"<--",collection.data[filename].timestamp, targetTime
#        if collection.data[filename].timestamp == targetTime:
#            candidateList.append(filename)
#
##        if filterstring in filename:
##            candidateList.append(filename)
#    return(candidateList)
            

#    candidateFiles = filter(lambda x:filterstring in x, collection.data.keys())
#    print "candidateFiles=",candidateFiles
#    for file in candidateFiles:
#        print "Candidate:",file
    
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
        return stringMD5sum("0")
    
def getUserTagsFromTags(tags):
    if 'Xmp.dc.subject' in tags.xmp_keys:
        return(tags['Xmp.dc.subject'].value)
    else:
        return('NA')
        
def get_photo_data(node_path, pickle_path, node_update = True):
    ''' Create instance of photo data given one of three cases:
    1.  Supply only node_path:  Create photo data instance
    2.  Supply only pickle_path:  load pickle.  Abort if pickle empty.
    3.  Supply both node_path and pickle_path:  Try to load pickle. 
                                                If exists:
                                                    load pickle
                                                    update pickle unless asked not to
                                                else:
                                                    create photo data instance and create pickle
    
    all other cases are errors
    '''
    logger = logging.getLogger()
    if node_path is not None and pickle_path is None:
        logger.info("Creating photoUnitData instance for {0}".format(node_path))
        node = photoData(node_path)
    elif node_path is None and pickle_path is not None:
        logger.info("Unpacking pickle at {0}".format(pickle_path))
        pickle = photo_pickler(pickle_path)
        node = pickle.loadPickle()
    elif node_path is not None and pickle_path is not None:
        pickle = photo_pickler(pickle_path)
        if pickle.pickleExists:
            logger.info("Loading pickle at {0} for {1}".format(pickle.picklePath, node_path))
            node = pickle.loadPickle()
            if node_update:
                logger.info("Refreshing photo data in pickle  **stubbed off**")
#                node.refresh()          
        else:
            logger.info("Scanning node {0}".format(node_path))
            node = photoData(node_path)
            node.pickle = pickle
            node.dump_pickle()
    else:
        logger.critical("function called with arguments:\"{0}\" and \"{1}\"".format(node_path, pickle_path))
        sys.exit(1)
    return(node)

def listZeroLengthFiles(photos):
    zeroLengthNames = []
    for target in photos.data.keys():
        if photos.data[target].size == 0:
            zeroLengthNames.append(target)
    return(zeroLengthNames)
    
def get_statistics(photos):
    class statistics:
        def __init__(self):
            self.dircount = 0
            self.filecount = 0
            self.unique_count = 0
            self.dup_count = 0
            self.dup_fraction = 0
    logger = logging.getLogger()
    stats = statistics()
    photo_set = set()
    for archive_file in photos.data.keys():
        if photos.data[archive_file].isdir:
            stats.dircount += 1
        else:
            stats.filecount += 1
            md5 = photos.data[archive_file].signature
            if md5 in photo_set:
                stats.dup_count += 1
            else:
                photo_set.add(md5)
    stats.unique_count = len(photo_set)
    stats.dup_fraction = stats.dup_count * 1.0 / stats.filecount
    logger.info("Collection statistics:  Directories = {0}, Files = {1}, Unique signatures = {2}, Duplicates = {3}, Duplicate Fraction = {4:.2%}".format(
        stats.dircount, stats.filecount, stats.unique_count, stats.dup_count, stats.dup_fraction))    
    return(stats)
            
def print_statistics(photos):
    result = get_statistics(photos)
    print "Directories: {0}, Files: {1}, Unique photos: {2}, Duplicates: {3} ({4:.2%})".format(result.dircount, result.filecount, result.unique_count, result.dup_count, result.dup_fraction)
    return

def print_zero_length_files(photos):
    zeroFiles = listZeroLengthFiles(photos)
    if len(zeroFiles) == 0:
        print "No zero-length files."
    else:
        print "Zero-length files:"
        for names in zeroFiles:
            print names
        print ""
    
def populate_tree_sizes(photos, top = ''):
    '''Recursively descends photo tree structure and computes/populates sizes
    '''
    if top == '':
        top = photos.path
    if os.path.isfile(top):
        return photos.data[top].size
    cumulative_size = 0
    for dirpath in photos.data[top].dirpaths:
        cumulative_size += populate_tree_sizes(photos, dirpath)
    for filepath in photos.data[top].filepaths:
        cumulative_size += photos.data[filepath].size
    photos.data[top].size = cumulative_size
    return cumulative_size      

def populate_tree_signatures(photos, top = ''):
    '''Recursively descends photo data structure and computes aggregated signatures
    Assumes all files have signatures populated; computes for signatures for directory structure
    '''
    if top == '':
        top = photos.path
    if os.path.isfile(top):
        return photos.data[top].signature
    cumulative_signature = ''
    for dirpath in photos.data[top].dirpaths:
        cumulative_signature += populate_tree_signatures(photos, dirpath)
    for filepath in photos.data[top].filepaths:
        cumulative_signature += photos.data[filepath].signature
    cumulative_signature = stringMD5sum(cumulative_signature)
    photos.data[top].signature = cumulative_signature
    return cumulative_signature     
    
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
