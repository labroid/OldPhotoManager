'''
Created on Nov 23, 2011

@author: scott_jackson
'''

import os
import sys
import logging
import datetime
from copy import deepcopy
from pickle_manager import photo_pickler
import photo_data
import MD5sums
      
def populate_duplicate_candidates(archive, node, archive_path = None, node_path = None):
    ''' 
	Replaces the .signature_match list property for each element of the node_path as follows:
	if archive and node are the different:
		.signatures_match and .signature_and_tags_match property is updated for all files
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
        if archive[archiveFile].signature in archiveTable:
            archiveTable[archive[archiveFile].signature].append(archiveFile)
        else:
            archiveTable[archive[archiveFile].signature] = [archiveFile]
            
    logger.info("Finding duplicates...")
    file_count = 0
    if archive != node:
        for nodepath in node.photo.keys():
            signature = node[nodepath].signature
            node[nodepath].signature_match = []
            node[nodepath].signature_and_tags_match = []
            node[nodepath].inArchive = False
            if signature in archiveTable:
                for candidate in archiveTable[signature]:
                    if node[nodepath].userTags == archive[candidate].userTags:
                        node[nodepath].signature_and_tags_match.append(candidate)
                        if not node[nodepath].isdir:
                            node[nodepath].inArchive = True
                    else:
                        node[nodepath].signature_match.append(candidate)
    else:  #TODO:  This needs to be rewritten to match archive != node approach
        if archive_path == node_path:
            for file_count, nodepath in enumerate(node.photo.keys(), start = 1):
                if file_count % 1000 == 0:
                    print "File count:", file_count
                signature = node[nodepath].signature
                node[nodepath].signature_match = []
                node[nodepath].signature_and_tags_match = []
                node[nodepath].inArchive = False
                if signature in archiveTable:
                    for candidate in archiveTable[signature]:
                        if candidate != nodepath:  #Do not record yourself as a match
                            if node[nodepath].userTags == archive[candidate].userTags:
                                node[nodepath].signature_and_tags_match.append(candidate)
                            else:
                                node[nodepath].signature_match.append(candidate) 
        else:  #Remove all signature_match that share the node_path root
            logger.error("***STUBBED OFF*** TODO Finish this function before doing subdirectory compares between collections")
            print "***STUBBED OFF*** TODO subdirectory searches not yet supported."
            sys.exit(1)
    logger.info("Done populating candidate properties")
                
                
def is_node_in_archive(archive, node, archive_path = None, node_path = None):
    '''Determine if node is in archive.
    '''
    
    if archive_path == None:
        archive_path = archive.path
    if node_path == None:
        node_path = node.path
        
    allFilesInArchive = True  #Seed value; logic will falsify this value if any files are missing     
    
    if '20100108' in node_path:
        pass
    if not node[node_path].isdir:
        return(len(node[node_path].signature_and_tags_match) > 0)
    
    for dirpath in node[node_path].dirpaths:
        allFilesInArchive = is_node_in_archive(archive, node, archive_path, dirpath) and allFilesInArchive 
    for filepath in node[node_path].filepaths:
        allFilesInArchive = allFilesInArchive and node[filepath].inArchive
    node[node_path].inArchive = allFilesInArchive
    return(allFilesInArchive)

    
def getTimestampFromTags(tags):
    if 'Exif.Photo.DateTimeOriginal' in tags.exif_keys:
        timestamp = tags['Exif.Photo.DateTimeOriginal'].value
    else:
        timestamp = datetime.datetime.strptime('1800:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
    return(timestamp)

def thumbnailMD5sum(tags):
    if len(tags.previews) > 0:
        temp = MD5sums.stringMD5sum(tags.previews[0].data)
    else:
        temp = MD5sums.stringMD5sum("0")
    return(temp)
    
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
        logger.info("Creating photo_collection instance for {0}".format(node_path))
        node = photo_data.create_collection(node_path)
    elif node_path is None and pickle_path is not None:
        logger.info("Unpacking pickle at {0}".format(pickle_path))
        pickle = photo_pickler(pickle_path)
        node = pickle.loadPickle()
    elif node_path is not None and pickle_path is not None:
        pickle = photo_pickler(pickle_path)
        if pickle.pickleExists:
            logger.info("Loading pickle at {0} for {1}".format(pickle.picklePath, node_path))
            node = pickle.loadPickle()
#            if node_update:
#                logger.info("Refreshing photo photo in pickle  **stubbed off**")
##                node.refresh()          
        else:
            logger.info("Scanning node {0}".format(node_path))
            node = photo_data.create_collection(node_path)
            pickle = photo_pickler(pickle_path)
            pickle.dumpPickle(node)
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
        return photos[top].size
    
    cumulative_size = 0
    for dirpath in photos[top].dirpaths:
        cumulative_size += populate_tree_sizes(photos, dirpath)
    for filepath in photos[top].filepaths:
        cumulative_size += photos[filepath].size
    photos[top].size = cumulative_size
    return cumulative_size      

def populate_tree_signatures(photos, top = ''):  #TODO: I'd like to log from this function, but it is recursive and I don't know when I'm at the root call
    '''Recursively descends photo photo structure and computes aggregated signatures
    Assumes all files have signatures populated; computes for signatures for directory structure
    '''
    if top == '':
        top = photos.path
        
    if os.path.isfile(top):
        return photos[top].signature
    
    cumulative_signature = ''
    for dirpath in photos[top].dirpaths:
        cumulative_signature += populate_tree_signatures(photos, dirpath)
    for filepath in photos[top].filepaths:
        cumulative_signature += photos[filepath].signature
    cumulative_MD5 = MD5sums.stringMD5sum(cumulative_signature)
    photos[top].signature = cumulative_MD5
    return cumulative_MD5
    
def print_tree(photos, top = None, indent_level = 0, show_empty_files = False):
    '''Print Photo collection using a tree structure'''
    INDENT_WIDTH = 3 #Number of spaces for each indent level
    if top is None:
        top = photos.path
    print "{0}{1} {2} {3} {4}".format(" " * INDENT_WIDTH * indent_level, os.path.basename(top), photos[top].inArchive, photos[top].size, photos[top].signature_and_tags_match)
    indent_level += 1
    for filepath in photos.photo[top].filepaths:
        if photos[filepath].size != 0:
            print "{0}{1} {2} {3} {4}".format(" " * INDENT_WIDTH * indent_level, os.path.basename(filepath), photos[filepath].inArchive, photos[filepath].size, photos[filepath].signature_and_tags_match)
    for dirpath in photos.photo[top].dirpaths:
        print_tree(photos, dirpath, indent_level)
        