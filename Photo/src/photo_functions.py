'''
Created on Nov 23, 2011

@author: scott_jackson
'''

import os
import sys
import logging
import MD5sums
import pickle_manager
from photo_data import photo_collection, node_info

logger = logging.getLogger()

class node_state(object):
    def __init__(self):
        self.all_in_archive = False
        self.none_in_archive = False
        self.signature_and_tags_match = []
        self.signatures_match = []
        
class results(object):
    def __init__(self):
        self.node = dict()
        self.mode = None
        
    def __getitem__(self, key):
        return self.node[key]
    
    def __setitem__(self, key, value):
        self.node[key] = value
        
def initialize_result_structure(node, node_path = None, result = None):
    if not result: #'result' will exist after first call.  Existence used to detect first call in recursion.
        logger.info('Initializing result structure')
        result = results()
        if node_path is None:
            node_path = node.path
#TODO Do I need to do something here if node_path is a file??            
#        if node_path is a file:
#            result[node_path] = photo_state()
#            return(result)
    result.node[node_path] = node_state()
    for dirpath in node[node_path].dirpaths:
        initialize_result_structure(node, dirpath, result)
    for filepath in node[node_path].filepaths:
        result.node[filepath] = node_state()
    return(result)
        
def set_comparison_type(archive, node, result, archive_path = None, node_path = None):
    #if result and archive are different, record all duplicates
    #if result and archive are same, and root same, record duplicates if not self
    #if result and archive are same, and root different, record duplicates only if in different tree
    if archive_path is None:
        archive_path = archive.path
    if node_path is None:
        node_path = node.path
    if node != archive:   #TODO: Don't know the elegant way to enumerate choices like C's "include" constant defs
        result.mode = 'all'
    else:
        if node_path == archive_path:
            result.mode = 'not self'
        else:
            result.mode = 'different tree'
    return

def populate_tree_sizes(photos, top = None, root_call = True):
    '''Recursively descends photo tree structure and computes/populates sizes
    '''
    if root_call:
        logger.info("Computing cumulative sizes for file tree.") 
        if top is None:
            top = photos.path
    if not photos[top].isdir:
        return photos[top].size
    
    cumulative_size = 0
    for dirpath in photos[top].dirpaths:
        cumulative_size += populate_tree_sizes(photos, dirpath, root_call = False)
    for filepath in photos[top].filepaths:
        cumulative_size += photos[filepath].size
    photos[top].size = cumulative_size
    return cumulative_size      

def populate_tree_signatures(photos, top = None, root_call = True):
    '''Recursively descends photo photo structure and computes aggregated signatures
    Assumes all files have signatures populated; computes for signatures for directory structure
    '''
    if root_call:
        logger.info("Populating cumulative tree signatures for file tree.")
        if top is None:
            top = photos.path
    if not photos[top].isdir:
        return photos[top].signature
    cumulative_signature = ''
    for dirpath in photos[top].dirpaths:
        cumulative_signature += populate_tree_signatures(photos, dirpath, root_call = False)
    for filepath in photos[top].filepaths:
        cumulative_signature += photos[filepath].signature
    cumulative_MD5 = MD5sums.stringMD5sum(cumulative_signature)
    photos[top].signature = cumulative_MD5
    return cumulative_MD5
        
def build_hash_dict(archive, archive_path, hash_dict = None):
    '''Recursive function to build a hash dictionary with keys of file signatures and values 
       of 'list of files with that signature'
       [This is recursive as opposed to running through photos because of the ability to descend an archive_path]
    '''
    if hash_dict is None:
        logger.info("Building signature hash dictionary for {0}".format(archive.path))
        hash_dict = {}
    for dirpath in archive[archive_path].dirpaths:
        build_hash_dict(archive, dirpath, hash_dict)
    for archive_file in archive[archive_path].filepaths:
        if archive[archive_file].signature in hash_dict:
            hash_dict[archive[archive_file].signature].append(archive_file)
        else:
            hash_dict[archive[archive_file].signature] = [archive_file]
    return hash_dict

def populate_duplicate_candidates(archive, node, result, archive_dict = None, archive_path = None, node_path = None):
    if not archive_dict: #Used to identify first call in recursion
        logger.info("Populating duplicate candidates...")
        if archive_path is None:
            archive_path = archive.path
        if node_path is None:
            node_path = node.path
        #Clear all duplicate states
        for nodepath in result.node.keys():
            result[nodepath].signature_match = []
            result[nodepath].signature_and_tags_match = []
            result[nodepath].all_in_archive = False
            result[nodepath].none_in_archive = False
        archive_dict = build_hash_dict(archive, archive_path)
    for dirpath in node[node_path].dirpaths:
        populate_duplicate_candidates(archive, node, result, archive_dict, archive_path, dirpath)
    for filepath in node[node_path].filepaths:
        signature = node[filepath].signature
        if signature in archive_dict:
            for candidate in archive_dict[signature]:
                if result.mode == 'all' or (result.mode == 'not self' and filepath != candidate) or (result.mode == 'different tree' and not node_path in candidate):
                    if node[filepath].userTags == archive[candidate].userTags:
                        result[filepath].signature_and_tags_match.append(candidate)
                    else:
                        result[filepath].signature_match.append(candidate)
    if not archive_dict:
        logger.info("Done populating duplicates.")
    return
    
def node_inclusion_check(node, result, node_path = None, top = True):
    '''Recurse through tree recording status of nodes
    '''
    if top:
        logger.info("Determining if node is duplicated.")
    all_in_archive = True  #Seed value; logic will falsify this value if any files are missing     
    none_in_archive = True #Seed value
    for dirpath in node[node_path].dirpaths:
        [all_in_tree, none_in_tree] = node_inclusion_check(node, result, dirpath, False)
        all_in_archive = all_in_archive and all_in_tree
        none_in_archive = none_in_archive and none_in_tree
    for filepath in node[node_path].filepaths:
        if result[filepath].signature_and_tags_match: #True if the signature_and_tags_match list isn't empty
            result[filepath].all_in_archive = True
            result[filepath].none_in_archive = False
            #all_in_archive = all_in_archive and True   #Shown here for completeness, commented since it is a boolean identity
            none_in_archive = False
        else:
            if os.path.basename(filepath) in ['.picasa.ini', 'Picasa.ini', 'picasa.ini', 'Thumbs.db']:  #TODO Make this configurable
                result[filepath].all_in_archive = True
                result[filepath].none_in_archive = False
            else:
                result[filepath].all_in_archive = False
                result[filepath].none_in_archive = True
                all_in_archive = False
    result[node_path].all_in_archive = all_in_archive
    result[node_path].none_in_archive = none_in_archive
    if top:
        logger.info("Done determining if node is duplicated.")
    return(all_in_archive, none_in_archive)

def is_node_in_archive(node, archive, node_path = None, archive_path = None):
    logger.info("Checking if node is in archive")
    if node_path is None:
        node_path = node.path
    if archive_path is None:
        archive_path = node.path
    result = prepare_datasets(node, archive, node_path, archive_path)
    node_inclusion_check(node, result, node_path)
    print_tree(node, result)
    
def prepare_datasets(node, archive, node_path = None, archive_path = None):
    if node_path is None:
        node_path = node.path
    if archive_path is None:
        archive_path = archive.path
    result = initialize_result_structure(node)
    set_comparison_type(archive, node, result)
    populate_tree_sizes(archive)
    populate_tree_signatures(archive)
    if archive != node:
        populate_tree_sizes(node)
        populate_tree_signatures(node)
    populate_duplicate_candidates(archive, node, result)
    return (result)
    
def print_tree(photos, result, top = None, indent_level = 0):
    '''Print Photo collection using a tree structure'''
    if top is None:
        top = photos.path
    if indent_level == 0:  #Used to detect first call of recursion
        print "Photo Collection at {0}:{1} pickled at {2}".format(photos.host, photos.path, photos.pickle)    
    print_tree_line(photos, result, top, indent_level)
    indent_level += 1
    for filepath in photos[top].filepaths:
        print_tree_line(photos, result, filepath, indent_level)
    for dirpath in photos[top].dirpaths:
        print_tree(photos, result, dirpath, indent_level)
    return
    
def print_tree_line(photos, result, path, indent_level):
    INDENT_WIDTH = 3 #Number of spaces for each indent level
    if photos[path].isdir:
        print "{0}{1} {2} {3} {4} {5}".format(" " * INDENT_WIDTH * indent_level, path, result[path].all_in_archive, result[path].none_in_archive, photos[path].size, photos[path].signature)
    else:
        print "{0}{1} {2} {3} {4} {5} {6} {7}".format(" " * INDENT_WIDTH * indent_level, path, result[path].all_in_archive, photos[path].size, photos[path].signature, photos[path].userTags, result[path].signature_and_tags_match, result[path].signatures_match)
            
#Refactor good to here

    
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
            
def print_largest_duplicates(photos, result):            
    all_in_archive_list = [x for x in result.node.keys() if result.node[x].all_in_archive]
    big_ones = sorted(some_variable, key = lambda dupe: dupe[1], reverse = True)
    print "Big ones:"
    for x in big_ones:
        print x, photos[x[0]].inArchive, photos[x[0]].signature_and_tags_match

def main():
#    logfile = "/home/scott/Desktop/PythonPhoto/log.txt"
    logfile = "C:\Users\scott_jackson\Desktop\lap_log.txt"
#    node = "C:\Users\scott_jackson\Desktop\newpickleorigupdate.txt"
    node_pickle_file = "C:\Users\scott_jackson\Desktop\lap_pickle.txt"
    node_path = "C:\Users\scott_jackson\Pictures\Process\\20111123"
#    archive_pickle_file = "C:\Users\scott_jackson\Desktop\jsonpickle.txt"
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(filename = logfile, format = LOG_FORMAT, level = logging.DEBUG, filemode = 'w')
    node_pickle = pickle_manager.photo_pickler(node_pickle_file)
    node = node_pickle.loadPickle()
#    archive_pickle = pickle_manager.photo_pickler(archive_pickle_file)
#    archive = archive_pickle.loadPickle()
    is_node_in_archive(node, node, node_path)
    print "Done!"

if __name__ == "__main__":
    sys.exit(main())
        