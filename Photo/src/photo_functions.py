'''
Created on Nov 23, 2011

@author: scott_jackson
'''

import os
import sys
import logging
import datetime
#from operator import itemgetter
#from copy import deepcopy
from pickle_manager import photo_pickler
import photo_data
import MD5sums

logger = logging.getLogger()


def build_hash_dict(archive, archive_path, hash_dict = {}, top = True):
    '''Recursive function to build a hash dictionary with keys of file signatures and values 
       being a list of files with that signature
    '''
    if top:
        logger.info("Building signature hash dictionary for {0}".format(archive.path))
    for dirpath in archive[archive_path].dirpaths:
        build_hash_dict(archive, dirpath, hash_dict, False)
    for archive_file in archive[archive_path].filepaths:
        if archive[archive_file].signature in hash_dict:
            hash_dict[archive[archive_file].signature].append(archive_file)
        else:
            hash_dict[archive[archive_file].signature] = [archive_file]
    return hash_dict

def populate_duplicate_candidates(archive, node, archive_path = None, node_path = None):
    logger.info("Populating duplicate candidates...")
    if archive_path == None:
        archive_path = archive.path
    if node_path == None:
        node_path = node.path
        
    #Clear all duplicate states
    for nodepath in node.photo.keys():
        node[nodepath].signature_match = []
        node[nodepath].signature_and_tags_match = []
        node[nodepath].inArchive = False
        
    archive_dict = build_hash_dict(archive, archive_path)
    populate_duplicates(node, node_path, archive, archive_path, archive_dict)
    
    
def populate_duplicates(node, node_path, archive, archive_path, archive_dict, top = True, mode = 'none'):
    if top:
        logger.info("Populating Duplicates...")
        #if node and archive are different, record all duplicates
        #if node and archive are same, and root same, record duplicates if not self
        #if node and archive are same, and root different, record duplicates if in different tree
        if node != archive:
            mode = 'all'
        else:
            if node_path == archive_path:
                mode = 'not self'
            else:
                mode = 'different tree'
                
    for dirpath in node[node_path].dirpaths:
        populate_duplicates(node, dirpath, archive, archive_path, archive_dict, False, mode)
    for filepath in node[node_path].filepaths:
        signature = node[filepath].signature
        if signature in archive_dict:
            for candidate in archive_dict[signature]:
                if mode == 'all' or (mode == 'not self' and filepath != candidate) or mode == 'different tree' and not node_path in candidate:
                    if node[filepath].userTags == archive[candidate].userTags:
                        node[filepath].signature_and_tags_match.append(candidate)
                    else:
                        node[filepath].signature_match.append(candidate)
    if top:
        logger.info("Done populating duplicates.")
      
def is_node_in_archive(node, node_path = None, top = True):
    '''Determine if node_path is in archive.
    '''
    if top:
        if node_path == None:
            node_path = node.path
        logger.info("Determining if node is in archive...")
        
    if not node[node_path].isdir:
        if len(node[node_path].signature_and_tags_match) > 0:
            node[node_path].inArchive = True
            logger.info("Done determining if node is in archive. Degenerate case: node_path is a file.")
            return(True)
    
    allFilesInArchive = True  #Seed value; logic will falsify this value if any files are missing     
    for dirpath in node[node_path].dirpaths:
        allFilesInArchive = is_node_in_archive(node, dirpath, False) and allFilesInArchive 
    for filepath in node[node_path].filepaths:
        if len(node[filepath].signature_and_tags_match) > 0:
            node[filepath].inArchive = True
        else:
            if os.path.basename(filepath) in ['.picasa.ini', 'Picasa.ini', 'picasa.ini', 'Thumbs.db']:  #TODO Make this configurable
                node[filepath].inArchive = True
            else:
                node[filepath].inArchive = False
                allFilesInArchive = False
    node[node_path].inArchive = allFilesInArchive
    if top:
        logger.info("Done determining if node is in archive.")
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
            if node_update:
                photo_data.update_collection(node)
                pickle.dumpPickle(node)
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
    
    
def populate_tree_sizes(photos, top = '', root_call = True):
    '''Recursively descends photo tree structure and computes/populates sizes
    '''
    if root_call:
        logger.info("Computing cumulative sizes for file tree.") 
        if top == '':
            top = photos.path
        
    if os.path.isfile(top):
        return photos[top].size
    
    cumulative_size = 0
    for dirpath in photos[top].dirpaths:
        cumulative_size += populate_tree_sizes(photos, dirpath, root_call = False)
    for filepath in photos[top].filepaths:
        cumulative_size += photos[filepath].size
    photos[top].size = cumulative_size
    return cumulative_size      


def populate_tree_signatures(photos, top = '', root_call = True):
    '''Recursively descends photo photo structure and computes aggregated signatures
    Assumes all files have signatures populated; computes for signatures for directory structure
    '''
    if root_call:
        logger.info("Populating cumulative tree signatures for file tree.")
        if top == '':
            top = photos.path
        
    if os.path.isfile(top):
        return photos[top].signature
    
    cumulative_signature = ''
    for dirpath in photos[top].dirpaths:
        cumulative_signature += populate_tree_signatures(photos, dirpath, root_call = False)
    for filepath in photos[top].filepaths:
        cumulative_signature += photos[filepath].signature
    cumulative_MD5 = MD5sums.stringMD5sum(cumulative_signature)
    photos[top].signature = cumulative_MD5
    return cumulative_MD5
    
    
def print_tree(photos, top = None, indent_level = 0):
    '''Print Photo collection using a tree structure'''
    '''I Broke this getting fancy printing automatic delete!  Needs to be completed or reverted'''
    INDENT_WIDTH = 3 #Number of spaces for each indent level
    if top is None:
        top = photos.path
        
    print "{0}{1} {2} {3} {4}".format(" " * INDENT_WIDTH * indent_level, top, photos[top].inArchive, photos[top].size, photos[top].signature_and_tags_match)
        
    indent_level += 1
    for filepath in photos.photo[top].filepaths:
        print "{0}{1} {2} {3} {4}".format(" " * INDENT_WIDTH * indent_level, filepath, photos[filepath].inArchive, photos[filepath].size, photos[filepath].signature_and_tags_match)
    for dirpath in photos.photo[top].dirpaths:
        print_tree(photos, dirpath, indent_level)
    
def find_duplicate_nodes(photos, top = None): #This is broken.  Resulting list is a list of tuples with some lists thrown in, including empty nested lists.  Doh!
    if top is None:
        top = photos.path
       
    duplicates = []
    for dirpath in photos[top].dirpaths:
        if photos[dirpath].inArchive:
            duplicates.append((dirpath, photos[dirpath].size))
        else:
            duplicates.append(find_duplicate_nodes(photos, dirpath))
    for filepath in photos[top].filepaths:
        if photos[filepath].inArchive:
            duplicates.append((filepath, photos[filepath].size))
    return (duplicates)       
            
def print_largest_duplicates(photos):            
#Find largest duplicate nodes
    #Sort by size
    duplicates = find_duplicate_nodes(photos)
    big_ones = sorted(duplicates, key = lambda dupe: dupe[1], reverse = True)
    print "Big ones:"
    for x in big_ones:
        print x, photos[x[0]].inArchive, photos[x[0]].signature_and_tags_match
        