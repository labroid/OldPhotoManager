'''
Created on Nov 23, 2011

@author: scott_jackson
'''

import os
import sys
import logging
import datetime
from pickle_manager import photo_pickler
import MD5sums

logger = logging.getLogger()

class photo_state:
    def __init__(self):
        self.signature_and_tags_match = []
        self.signatures_match = []

class node_state:
    def __init__(self):
        self.size = 0
        self.signature = ''
        self.all_included = False
        self.none_included = False
        
class results:
    def __init__(self):
        self.photo = dict()
        self.node = dict()
        self.top = ''
        
class results:
    def __init__(self):
        self.
def generate_result_structure(node, node_path, top = True, result = None):
    if top:
        result = results()
        if node_path is None:
            node_path = node.path
        result.top = node_path
#Do I need to do something here if node_path is a file??            
#        if node_path is a file:
#            result[node_path] = photo_state()
#            return(result)
    for dirpath in node[node_path].dir.dirpaths:
        result.node[dirpath] = node_state()
        generate_result_structure(node, dirpath, top = False, result)
    for filepath in node[node_path].dir.filepaths:
        result.photo[filepath] = photo_state()
    return (result)

def populate_tree_sizes(results, top = None, root_call = True):
    '''Recursively descends photo tree structure and computes/populates sizes
    '''
    if root_call:
        logger.info("Computing cumulative sizes for file tree.") 
        if top is None:
            top = results.path
        
    if results
    if os.path.isfile(top):  #How do you check file type with new structure??
        return results[top].size
    
    cumulative_size = 0
    for dirpath in results[top].dirpaths:
        cumulative_size += populate_tree_sizes(results, dirpath, root_call = False)
    for filepath in results[top].filepaths:
        cumulative_size += results[filepath].size
    results[top].size = cumulative_size
    return cumulative_size      
        
def build_hash_dict(archive, archive_path, hash_dict = {}, top = True):
    '''Recursive function to build a hash dictionary with keys of file signatures and values 
       of 'list of files with that signature'
       [This is recursive as opposed to running through photos because of the ability to descend an archive_path]
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
#Remember to populate tree sizes and signatures somewhere in here; it was removed from photo_obj      
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
        #if node and archive are same, and root different, record duplicates only if in different tree
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
                if mode == 'all' or (mode == 'not self' and filepath != candidate) or (mode == 'different tree' and not node_path in candidate):
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

def main():
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(filename = "/home/scott/Desktop/PythonPhoto/log.txt", format = LOG_FORMAT, level = logging.DEBUG, filemode = 'w')
    node = "C:\Users\scott_jackson\Desktop\newpickleorigupdate.txt"
    result = generate_result_structure(node, node_path = None)
    print "Done!"

if __name__ == "__main__":
    sys.exit(main())
        