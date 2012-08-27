'''
Created on Nov 23, 2011

@author: scott_jackson
'''

import os
import sys
import logging
import datetime
from copy import deepcopy
from fileMD5sum import stringMD5sum
from pickle_manager import photo_pickler
from photoData import photoData
      
def populate_duplicate_candidates(archive, node, archive_path = None, node_path = None):
    ''' 
    Replaces the .candidates list property for each element of the node_path as follows:
    if archive and node are the different:
        .candidate property is updated for all files
    else:
        if archive_path and node_path are same:
            .candidate property is updated for all and 'self' is excluded from list
        else:
            .candidate property is updated for all files under node_path tree
    TODO:  This still needs work; not all collection/file combinations are supported
    '''    
    
    logger = logging.getLogger()
    
    if archive_path == None:
        archive_path = archive.path
    if node_path == None:
        node_path = node.path
        
    logger.info("Building signature hash table for {0}".format(archive.path)) 
    archiveTable = {}
    for archiveFile in archive.photo.keys():
        if archive.photo[archiveFile].signature in archiveTable:
            archiveTable[archive.photo[archiveFile].signature].append(archiveFile)
        else:
            archiveTable[archive.photo[archiveFile].signature] = [archiveFile]
            
    logger.info("Finding duplicates...")
    if archive != node:
        for nodefile in node.photo.keys():
            if node.photo[nodefile].signature in archiveTable:
                node.photo[nodefile].candidates = deepcopy(archiveTable[node.photo[nodefile].signature])
            else:
                node.photo[nodefile].candidates = []
    else:
        if archive_path == node_path:
            for nodefile in node.photo.keys():
                if node.photo[nodefile].signature in archiveTable:
                    if len(archiveTable[node.photo[nodefile].signature]) > 1: 
                        print "Duplicate:",nodefile, "Candidates",archiveTable[node.photo[nodefile].signature]
                    node.photo[nodefile].candidates = deepcopy(archiveTable[node.photo[nodefile].signature])
                else:
                    node.photo[nodefile].candidates = []
            for nodefile in node.photo.keys():
                if nodefile in node.photo[nodefile].candidates:
                    node.photo[nodefile].candidates.remove(nodefile)
        else:  #Remove all candidates that share the node_path root
            logger.error("***STUBBED OFF*** TODO Finish this function before doing subdirectory compares between collections")
            print "***STUBBED OFF*** TODO subdirectory searches not yet supported."
            sys.exit(1)
    logger.info("Done populating candidate properties")
                
def is_node_in_archive(archive, node, archive_path = None, node_path = None):  #TODO finish this function...just started and it's a mess.
    '''Determine if node is in archive.
    if archive and node are different:
        return True if node_path of node is contained within archive_path of archive
    else:
        return True if node_path of 
    '''
    
    if archive_path == None:
        archive_path = archive.path
    if node_path == None:
        node_path = node.path
    allFilesInArchive = True  #Seed value; logic will falsify if any files missing     
    
    for dirpath in node.photo[node_path].dirpaths:
       allFilesInArchive = allFilesInArchive and is_node_in_archive(archive, node, archive_path, node_path) 
    for filepath in photos.photo[top].filepaths:
        allFilesInArchive = allFilesInArchive    
        
    for root, dirs, files in os.walk(node.path, topdown=False):
        for nodeFile in files:
            candidateFile = os.path.join(root,nodeFile)
            node.photo[candidateFile].candidates = []
            node.photo[candidateFile].inArchive = False #Assume it is not in archive unless proven otherwise
            if node.photo[candidateFile].signature in archiveTable:
                for archiveFile in archiveTable[node.photo[candidateFile].signature]:
                    if archiveFile != candidateFile:  #Don't compare to oneself in case candidate path is in archive path  
                        if archive.photo[archiveFile].userTags != node.photo[candidateFile].userTags:
                            candidateThumbAndTagsSame = False
                        else:
                            candidateThumbAndTagsSame = True
                            if archive.get_file_signature(archiveFile) == node.get_file_signature(candidateFile):
                                node.photo[candidateFile].inArchive = True
                        node.photo[candidateFile].candidates.append([archiveFile, candidateThumbAndTagsSame, node.photo[candidateFile].inArchive])
            allFilesInArchive = allFilesInArchive and node.photo[candidateFile].inArchive
    
        if not allFilesInArchive:
            nodeInArchive = False
        else:
            nodeInArchive = True
            for nodeDir in dirs:
                nodeInArchive = node.photo[nodeDir].inArchive and nodeInArchive
            node.photo[root].inArchive = nodeInArchive
    return(node.photo[root].inArchive) 
    for dirpath in photos.photo[top].dirpaths:
        cumulative_size += populate_tree_sizes(photos, dirpath)
    for filepath in photos.photo[top].filepaths:
        cumulative_size += photos.photo[filepath].size
    photos.photo[top].size = cumulative_size
    return cumulative_size      
#def OLDisNodeInArchive(archive, node):  #Check "in archive" logic and make sure it is right!!  
#    if archive.path == node.path and archive.host == node.host:  #Made this host sensitive; make sure still works for internal compares
#        print "Error:  Node and Archive must have different root paths"
#        return(False) #By definition the node is in the archive since they are the same.  However return False so no one assumes it is a copy and deletes the Archive    
#
##Create a dictionary using thumbMD5s as the keys for fast lookup    
#    archiveTable = {}
#    for archiveFile in archive.photo.keys():
#        if archive.photo[archiveFile].signature in archiveTable: 
#            archiveTable[archive.photo[archiveFile].signature].append(archiveFile)
#        else:
#            archiveTable[archive.photo[archiveFile].signature] = [archiveFile]
#            
#    allFilesInArchive = True  #Seed value; logic will falsify if any files missing     
#    for root, dirs, files in os.walk(node.path, topdown=False):
#        for nodeFile in files:
#            candidateFile = os.path.join(root,nodeFile)
#            node.photo[candidateFile].candidates = []
#            node.photo[candidateFile].inArchive = False #Assume it is not in archive unless proven otherwise
#            if node.photo[candidateFile].signature in archiveTable:
#                for archiveFile in archiveTable[node.photo[candidateFile].signature]:
#                    if archiveFile != candidateFile:  #Don't compare to oneself in case candidate path is in archive path  
#                        if archive.photo[archiveFile].userTags != node.photo[candidateFile].userTags:
#                            candidateThumbAndTagsSame = False
#                        else:
#                            candidateThumbAndTagsSame = True
#                            if archive.get_file_signature(archiveFile) == node.get_file_signature(candidateFile):
#                                node.photo[candidateFile].inArchive = True
#                        node.photo[candidateFile].candidates.append([archiveFile, candidateThumbAndTagsSame, node.photo[candidateFile].inArchive])
#            allFilesInArchive = allFilesInArchive and node.photo[candidateFile].inArchive
#    
#        if not allFilesInArchive:
#            nodeInArchive = False
#        else:
#            nodeInArchive = True
#            for nodeDir in dirs:
#                nodeInArchive = node.photo[nodeDir].inArchive and nodeInArchive
#            node.photo[root].inArchive = nodeInArchive
#    return(node.photo[root].inArchive) 
#            
#def findSameTimestamp(collection, targetTime):
#    candidateList = []
##    filterstring = os.path.sep + targetTime.strftime('%Y') + os.path.sep + targetTime.strftime('%Y%m%d')
##    filterstring = os.path.sep + targetTime.strftime('%Y%m%d')
##    print "filterstring=",filterstring
#    for filename in collection.photo.keys():
#        print filename, "-->",collection.photo[filename].tags['EXIF DateTimeOriginal'],"<--",collection.photo[filename].timestamp, targetTime
#        if collection.photo[filename].timestamp == targetTime:
#            candidateList.append(filename)
#
##        if filterstring in filename:
##            candidateList.append(filename)
#    return(candidateList)
            

#    candidateFiles = filter(lambda x:filterstring in x, collection.photo.keys())
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
        return stringMD5sum(tags.previews[0].photo)
    else:
        return stringMD5sum("0")
    
def getUserTagsFromTags(tags):
    if 'Xmp.dc.subject' in tags.xmp_keys:
        return(tags['Xmp.dc.subject'].value)
    else:
        return('NA')
        
def get_photo_data(node_path, pickle_path, node_update = True):
    ''' Create instance of photo photo given one of three cases:
    1.  Supply only node_path:  Create photo photo instance
    2.  Supply only pickle_path:  load pickle.  Abort if pickle empty.
    3.  Supply both node_path and pickle_path:  Try to load pickle. 
                                                If exists:
                                                    load pickle
                                                    update pickle unless asked not to
                                                else:
                                                    create photo photo instance and create pickle
    
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
                logger.info("Refreshing photo photo in pickle  **stubbed off**")
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
    for target in photos.photo.keys():
        if photos.photo[target].size == 0:
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
    for archive_file in photos.photo.keys():
        if photos.photo[archive_file].isdir:
            stats.dircount += 1
        else:
            stats.filecount += 1
            md5 = photos.photo[archive_file].signature
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
    
def populate_tree_sizes(photos, top = ''):  #TODO: I'd like to log from this function, but it is recursive and I don't know when I'm at the root call
    '''Recursively descends photo tree structure and computes/populates sizes
    '''
    if top == '':
        top = photos.path
    if os.path.isfile(top):
        return photos.photo[top].size
    cumulative_size = 0
    for dirpath in photos.photo[top].dirpaths:
        cumulative_size += populate_tree_sizes(photos, dirpath)
    for filepath in photos.photo[top].filepaths:
        cumulative_size += photos.photo[filepath].size
    photos.photo[top].size = cumulative_size
    return cumulative_size      

def populate_tree_signatures(photos, top = ''):  #TODO: I'd like to log from this function, but it is recursive and I don't know when I'm at the root call
    '''Recursively descends photo photo structure and computes aggregated signatures
    Assumes all files have signatures populated; computes for signatures for directory structure
    '''
    if top == '':
        top = photos.path
    if os.path.isfile(top):
        return photos.photo[top].signature
    cumulative_signature = ''
    for dirpath in photos.photo[top].dirpaths:
        cumulative_signature += populate_tree_signatures(photos, dirpath)
    for filepath in photos.photo[top].filepaths:
        cumulative_signature += photos.photo[filepath].signature
    cumulative_signature = stringMD5sum(cumulative_signature)
    photos.photo[top].signature = cumulative_signature
    return cumulative_signature     
    
def print_indented_string(photos, path, indent_level):
    INDENT_WIDTH = 3 #Number of spaces for each indent_level
    print "{0}{1} {2} {3}".format(" " * INDENT_WIDTH * indent_level, os.path.basename(path), photos.photo[path].signature, photos.photo[path].size)

def print_tree(photos, top = '', indent_level = 0):
    '''Print Photo collection using a tree structure'''
    if top == '':
        top = photos.path
    print_indented_string(photos, top, indent_level)
    indent_level += 1
    for filepath in photos.photo[top].filepaths:
        print_indented_string(photos, filepath, indent_level)
    for dirpath in photos.photo[top].dirpaths:
        print_tree(photos, dirpath, indent_level)
        