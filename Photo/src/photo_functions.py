'''
Created on Nov 23, 2011

@author: scott_jackson
'''
#pylint: disable=line-too-long

import os
import os.path
import sys
import logging
import collections
import tables as tbl
import MD5sums
import pickle_manager
from photo_data import photo_collection, node_info

logger = logging.getLogger()

class node_state(object):
    def __init__(self):
        self.all_in_archive = False
        self.none_in_archive = False
        self.md5_match = []
        self.signature_match = []
        
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
    #if node and archive are different, record all duplicates
    #if node and archive are same, and root same, record duplicates if not self
    #if node and archive are same, and root different, record duplicates only if in different tree
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

def populate_tree_md5(photos, top = None, root_call = True):
    '''Recursively descends photo photo structure and computes aggregated md5
    Assumes all files have md5 populated; computes md5 for directory structure
    '''
    if root_call:
        logger.info("Populating cumulative tree md5 for file tree.")
        if top is None:
            top = photos.path
    if not photos[top].isdir:
        return photos[top].md5
    cumulative_md5_string = ''
    for dirpath in photos[top].dirpaths:
        cumulative_md5_string += populate_tree_md5(photos, dirpath, root_call = False)
    #Need something here like if len(photos[top].,filepaths) == 0:
        #dont accumulate the md5, but pass it up the chain unchanged
        #this keeps directory nodes from adding md5 content to directories with no siblings
    for filepath in photos[top].filepaths:
        cumulative_md5_string += photos[filepath].md5
    cumulative_md5 = MD5sums.stringMD5sum(cumulative_md5_string)
    photos[top].md5 = cumulative_md5
    return cumulative_md5
        
def build_hash_dict(archive, archive_path, hash_dict = None):
    '''Recursive function to build a hash dictionary with keys of file signatures and values 
       of 'list of files with that signature'
       [This is recursive as opposed to running through photos because of the ability to descend an archive_path]
    '''
    if hash_dict is None:  #First iteration of recursion
        logger.info("Building md5 hash dictionary for {0}".format(archive.path))
        hash_dict = collections.defaultdict(list)
    for dirpath in archive[archive_path].dirpaths:
        build_hash_dict(archive, dirpath, hash_dict)
    for archive_file in archive[archive_path].filepaths:
        hash_dict[archive[archive_file].md5].append(archive_file)
    hash_dict[archive[archive_path].md5].append(archive_path)
    return hash_dict

def populate_duplicate_candidates(archive, node, result, archive_dict = None, archive_path = None, node_path = None):
    if not archive_dict: #First iteration in recursion
        logger.info("Populating duplicate candidates...")
        if archive_path is None:
            archive_path = archive.path
        if node_path is None:
            node_path = node.path
        #Clear all duplicate states from result
        for nodepath in result.node.keys():
            result[nodepath].signature_match = []
            result[nodepath].md5_match = []
            result[nodepath].all_in_archive = False
            result[nodepath].none_in_archive = False
        archive_dict = build_hash_dict(archive, archive_path)
    for dirpath in node[node_path].dirpaths:
        populate_duplicate_candidates(archive, node, result, archive_dict, archive_path, dirpath)
    for filepath in node[node_path].filepaths:
        md5 = node[filepath].md5
        if md5 in archive_dict:
            for candidate in archive_dict[md5]:
                if result.mode == 'all' or (result.mode == 'not self' and filepath != candidate) or (result.mode == 'different tree' and not node.path in candidate): 
                    result[filepath].md5_match.append(candidate)
    if node_path == "/home/shared/Photos/Upload/2009/10/30":
        pass
    if node[node_path].md5 in archive_dict:
        for candidate in archive_dict[node[node_path].md5]:
            if result.mode == 'all' or (result.mode == 'not self' and node_path != candidate) or (result.mode == 'different tree' and not node.path in candidate):
                result[node_path].md5_match.append(candidate)
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
        if result[filepath].md5_match: #True if the md5_match list isn't empty
            result[filepath].all_in_archive = True
            result[filepath].none_in_archive = False
            #all_in_archive = all_in_archive and True   #Shown here for completeness, commented out since it is a boolean identity
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
    [all_in_archive, none_in_archive] = node_inclusion_check(node, result, node_path)
    return (result, all_in_archive, none_in_archive)

#TODO:  Need to show same signatures, different tags!
    
def prepare_datasets(node, archive, node_path = None, archive_path = None):
    if node_path is None:
        node_path = node.path
    if archive_path is None:
        archive_path = archive.path
    result = initialize_result_structure(node)
    set_comparison_type(archive, node, result)
    populate_tree_sizes(archive)
    populate_tree_md5(archive)
    if archive != node:
        populate_tree_sizes(node)
        populate_tree_md5(node)
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

def print_top_level_duplicates_tree(photos, result, top = None, indent_level = 0):
    '''Print Photo collection using a tree structure only showing the top node of an 'included' subtree'''
    if top is None:
        top = photos.path
    if indent_level == 0:  #Used to detect first call of recursion
        print "Photo Collection at {0}:{1} pickled at {2}".format(photos.host, photos.path, photos.pickle)    
    print_tree_line(photos, result, top, indent_level)
    if result[top].all_in_archive and photos[top].isdir:
        return  #Stop descending as soon as you find a directory that is in the archive from there down...
    indent_level += 1
    for filepath in photos[top].filepaths:
        print_tree_line(photos, result, filepath, indent_level)
    for dirpath in photos[top].dirpaths:
        print_top_level_duplicates_tree(photos, result, dirpath, indent_level)
    return
    
def print_tree_line(photos, result, path, indent_level):
    INDENT_WIDTH = 3 #Number of spaces for each indent level
    if photos[path].isdir:
        print "{0}{1} {2} {3} {4} {5}".format(" " * INDENT_WIDTH * indent_level, path, result[path].all_in_archive, result[path].none_in_archive, photos[path].size, photos[path].signature)
    else:
        print "{0}{1} {2} {3} {4} {5} {6} {7}".format(" " * INDENT_WIDTH * indent_level, path, result[path].all_in_archive, photos[path].size, photos[path].signature, photos[path].userTags, result[path].md5_match, result[path].signature_match)
        
def create_json_tree(photos, result, top = None):
    '''Recursively create json representation of file tree for use by jstree'''
    if top is None:
        top = photos.path
    json_tree = {"data" : {"title":os.path.basename(top),"icon":set_icon(top, result)}, "children" : []}
    for filepath in photos[top].filepaths:
        json_tree["children"].append({"data" : {"title" : "{0} {1} {2}".format(os.path.basename(filepath), result[filepath].md5_match, result[filepath].signature_match), "icon":set_icon(filepath, result)}})
    for dirpath in photos[top].dirpaths:
        json_tree["children"].append(create_json_tree(photos, result, dirpath))
    return json_tree

def set_icon(path, result):
    if result[path].all_in_archive:
        icon = "./green_button.png"
    elif result[path].none_in_archive:
        icon = "./red_button.png"
    else:
        icon = "./yellow_button.png"
    return icon

class Particle(tbl.IsDescription):
    node = tbl.StringCol(250)
    size  = tbl.Int64Col()      # Signed 64-bit integer
    md5sum  = tbl.StringCol(20)     # Unsigned short integer
    all_in_archive  = tbl.StringCol(10)      # unsigned byte
    md5_match    = tbl.StringCol(300)      # integer
    
def main():
#TODO:  Parse options:  -u update, -p print tree -j print json -n node
#    logfile = "/home/scott/Desktop/PythonPhoto/log.txt"
    logfile = "C:/Users/scott_jackson/Documents/Programming/PhotoManager/lap_log.txt"
#    node = "C:\Users\scott_jackson\Desktop\newpickleorigupdate.txt"
#    node_pickle_file = "C:\Users\scott_jackson\Desktop\lap_pickle.txt"
#    node_path = "C:\Users\scott_jackson\Pictures\Process\\20111123"
#    node_pickle_file = "C:/Users/scott_jackson/Documents/Programming/PhotoManager/smitherspickle.txt"
    archive_pickle_file = "C:/Users/scott_jackson/Documents/Programming/PhotoManager/smitherspickle.txt"
    node_pickle_file = "C:\\Users\\scott_jackson\\Documents\\Programming\\PhotoManager\\lap_100CANON_pickle.txt"
#    node_path = "/home/shared/Photos/2008"
#    archive_pickle_file = "C:\Users\scott_jackson\Desktop\jsonpickle.txt"
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(filename = logfile, format = LOG_FORMAT, level = logging.DEBUG, filemode = 'w')
    node_pickle = pickle_manager.photo_pickler(node_pickle_file)
    node = node_pickle.loadPickle()
    archive_pickle = pickle_manager.photo_pickler(archive_pickle_file)
    archive = archive_pickle.loadPickle()
    node = archive  #<-*****************Watch this!!!!!!!!!!!!!!++++++++==============
    [result, all_in_archive, none_in_archive] = is_node_in_archive(node, node, "/media/rmv1/Photos/2009", "/media/rmv1/Photos/2009")
    for row in result.node.keys():
        print row, "|", node[row].size, "|", node[row].md5, "|", result[row].all_in_archive, "|", result[row].none_in_archive, "|", result[row].md5_match, "|", result[row].signature_match

    print "****************************"
#    print_top_level_duplicates_tree(node, result)
    print_tree(node, result)
    
if __name__ == "__main__":
    sys.exit(main())
        