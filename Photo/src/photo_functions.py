'''
Created on Nov 23, 2011

@author: scott_jackson
'''

import os
import sys
import logging
import datetime
from pickle_manager import photo_pickler
import photo_data
import MD5sums

logger = logging.getLogger()

class photo_state:
    def __init__(self):
        self.signature_and_tags_match = False
        self.signatures_match = False

class node_state:
    def __init__(self):
        self.size = 0
        self.signature = ''
        self.all_included = False
        self.none_included = False
        
def build_hash_dict(archive, archive_path, hash_dict = {}, top = True):
    '''Recursive function to build a hash dictionary with keys of file signatures and values 
       of 'list of files with that signature'
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
#***********Start refactoring here**********
#Remember to populate tree sizes and signatures somewhere in here; it was removed from photo_data      
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
      
def check_node_inclusion_status(node, node_path = none):
    '''Determine if node_path is in archive.
    '''
    logger.info("Determining if node is duplicated...")
    if node_path == None:
        node_path = node.path
    dup_status = {}
    if not node[node_path].isdir:
        if len(node[node_path].signature_and_tags_match) > 0:
            dup_status[node_path].
            [node_path].inArchive = True
            logger.info("Done determining if node is duplicated. Degenerate case: node_path is a file.")
            return(True)
    
def recurse_node_inclusion_check(node, node_path = None, top = True):
    '''Recurse through tree recording status of nodes
    '''
    
    allFilesInArchive = True  #Seed value; logic will falsify this value if any files are missing     
    for dirpath in node[node_path].dirpaths:
        allFilesInArchive = recurse_node_inclusion_check(node, dirpath, False) and allFilesInArchive 
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
        logger.info("Done determining if node is duplicated.")
    return(allFilesInArchive)

    

        

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
        