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
import MD5sums
import pickle_manager
from photo_data import photo_collection, node_info

logger = logging.getLogger()

class PhotoFunctions(object):

    class NodeState(object):
        def __init__(self):
            self.all_in_archive = False
            self.none_in_archive = False
            self.md5_match = []
            self.signature_match = []
    
    def __init__(self, candidate, archive, candidate_path = None, archive_path = None):
        self.node = dict()
        self.mode = ''
        self.candidate = candidate
        self.archive = archive
        if candidate_path is None:
            candidate_path = candidate.path
        if archive_path is None:
            archive_path = archive.path
            
        logger.info('Initializing result structure')               
        self.initialize_result_structure(self.candidate_path)
        self.set_comparison_type()
        self.populate_tree_sizes(archive)
        self.populate_tree_md5(archive)
        if archive != candidate:
            self.populate_tree_sizes(candidate)
            self.populate_tree_md5(candidate)
        self.populate_duplicate_candidates(archive, candidate, self.result)
        return ()
    
    def initialize_result_structure(self, path):
    #TODO Do I need to do something here if candidate_path is a file??  I don't think so - I think this should work            
        self.node[path] = self.NodeState()
        for dirpath in self.candidate[path].dirpaths:
            self.initialize_result_structure(dirpath)
        for filepath in self.candidate[path].filepaths:
            self.node[filepath] = self.NodeState()
        return()
            
    def set_comparison_type(self):
        #if candidate and archive are different, record all duplicates
        #if candidate and archive are same, and root same, record duplicates if not self
        #if candidate and archive are same, and root different, record duplicates only if in different tree
        if self.candidate != self.archive:  #TODO would like to use enumerated type here, but not available in Python 2.7 I think
            self.mode = 'all'
        else:
            if self.candidate_path == self.archive_path:
                self.mode = 'not self'
            else:
                self.mode = 'different tree'
        return
    
    def populate_tree_sizes(self, photos, top = None):
        '''Recursively descends photo tree structure and computes/populates sizes
        '''
        if top is None:  #This is initial recursive call
            logger.info("Computing cumulative sizes for file tree.") 
            top = photos.path
            
        if not photos[top].isdir:
            return photos[top].size
        
        cumulative_size = 0
        for dirpath in photos[top].dirpaths:
            cumulative_size += self.populate_tree_sizes(photos, dirpath)
        for filepath in photos[top].filepaths:
            cumulative_size += photos[filepath].size
        photos[top].size = cumulative_size
        return cumulative_size      
    
    def populate_tree_md5(self, photos, top = None):
        '''Recursively descends photo photo structure and computes aggregated md5
        Assumes all files have md5 populated; computes md5 for directory structure
        '''
        if top is None:  #This is initial recursive call
            logger.info("Populating cumulative tree md5 for file tree.")
            top = photos.path
                
        if not photos[top].isdir:
            return photos[top].md5
        
        cumulative_md5_string = ''
        for dirpath in photos[top].dirpaths:
            cumulative_md5_string += self.populate_tree_md5(photos, dirpath)
        #TODO:  Need something here like if len(photos[top].,filepaths) == 0:  Or maybe directories with no files but only sub directories don't get reported or are highlighted in the UI
            #dont accumulate the md5, but pass it up the chain unchanged
            #this keeps directory nodes from adding md5 content to directories with no siblings
        for filepath in photos[top].filepaths:
            cumulative_md5_string += photos[filepath].md5
        cumulative_md5 = MD5sums.stringMD5sum(cumulative_md5_string)
        photos[top].md5 = cumulative_md5
        return cumulative_md5
            
    def build_hash_dict(self, path = None, hash_dict = None):
        '''Recursive function to build a hash dictionary with keys of file signatures and values 
           of 'list of files with that signature'
           [This is recursive as opposed to running through photos because of the ability to descend an archive_path]
        '''
        if hash_dict is None:  #First iteration of recursion
            logger.info("Building md5 hash dictionary for {0}".format(self.archive_path))
            path = self.archive_path
            hash_dict = collections.defaultdict(list)
            
        for dirpath in self.archive[path].dirpaths:
            self.build_hash_dict(dirpath, hash_dict)
        for filepath in self.archive[path].filepaths:
            hash_dict[self.archive[filepath].md5].append(filepath)
        hash_dict[self.archive[path].md5].append(path)
        return hash_dict
    
    def populate_duplicate_candidates(self, path = None):
        if path is None: #First iteration in recursion
            logger.info("Populating duplicate candidates...")
            path = self.candidate_path
            #Clear all duplicate states from result
            for nodepath in self.node.keys():
                self.node[nodepath].signature_match = []
                self.node[nodepath].md5_match = []
                self.node[nodepath].all_in_archive = False
                self.node[nodepath].none_in_archive = False
            archive_dict = self.build_hash_dict(self.archive_path)
            
        for dirpath in self.node[path].dirpaths:
            self.populate_duplicate_candidates(dirpath)
        for filepath in self.node[path].filepaths:
            md5 = self.node[filepath].md5
            if md5 in archive_dict:
                for candidate in archive_dict[md5]:
                    if self.mode == 'all' or (self.mode == 'not self' and filepath != candidate) or (self.mode == 'different tree' and not self.candidate_path in candidate): #TODO I don't think this is right
                        self.node[filepath].md5_match.append(candidate)
        if self.candidate_path == "/home/shared/Photos/Upload/2009/10/30":
            pass
        if node[candidate_path].md5 in archive_dict:
            for candidate in archive_dict[node[candidate_path].md5]:
                if result.mode == 'all' or (result.mode == 'not self' and candidate_path != candidate) or (result.mode == 'different tree' and not node.path in candidate):
                    result[candidate_path].md5_match.append(candidate)
        return
        
    def node_inclusion_check(self, node, result, candidate_path = None, top = True):
        '''Recurse through tree recording status of nodes
        '''
        if top:
            logger.info("Determining if node is duplicated.")
        all_in_archive = True  #Seed value; logic will falsify this value if any files are missing     
        none_in_archive = True #Seed value
        for dirpath in node[node_path].dirpaths:
            [all_in_tree, none_in_tree] = self.node_inclusion_check(node, result, dirpath, False)
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
    
    def is_node_in_archive(self, node, archive, node_path = None, archive_path = None):
        logger.info("Checking if node is in archive")
        if node_path is None:
            node_path = node.path
        if archive_path is None:
            archive_path = node.path
        result = self.prepare_datasets(node, archive, node_path, archive_path)
        [all_in_archive, none_in_archive] = self.node_inclusion_check(node, result, node_path)
        return (result, all_in_archive, none_in_archive)
    
    #TODO:  Need to show same signatures, different tags!
        

    def print_tree(self, photos, result, top = None, indent_level = 0):
        '''Print Photo collection using a tree structure'''
        if top is None:
            top = photos.path
        if indent_level == 0:  #Used to detect first call of recursion
            print "Photo Collection at {0}:{1} pickled at {2}".format(photos.host, photos.path, photos.pickle)    
        self.print_tree_line(photos, result, top, indent_level)
        indent_level += 1
        for filepath in photos[top].filepaths:
            self.print_tree_line(photos, result, filepath, indent_level)
        for dirpath in photos[top].dirpaths:
            self.print_tree(photos, result, dirpath, indent_level)
        return
    
    def print_top_level_duplicates_tree(self, photos, result, top = None, indent_level = 0):
        '''Print Photo collection using a tree structure only showing the top node of an 'included' subtree'''
        if top is None:
            top = photos.path
        if indent_level == 0:  #Used to detect first call of recursion
            print "Photo Collection at {0}:{1} pickled at {2}".format(photos.host, photos.path, photos.pickle)    
        self.print_tree_line(photos, result, top, indent_level)
        if result[top].all_in_archive and photos[top].isdir:
            return  #Stop descending as soon as you find a directory that is in the archive from there down...
        indent_level += 1
        for filepath in photos[top].filepaths:
            self.print_tree_line(photos, result, filepath, indent_level)
        for dirpath in photos[top].dirpaths:
            self.print_top_level_duplicates_tree(photos, result, dirpath, indent_level)
        return
        
    def print_tree_line(self, photos, result, path, indent_level):
        INDENT_WIDTH = 3 #Number of spaces for each indent level
        if photos[path].isdir:
            print "{0}{1} {2} {3} {4} {5}".format(" " * INDENT_WIDTH * indent_level, path, result[path].all_in_archive, result[path].none_in_archive, photos[path].size, photos[path].signature)
        else:
            print "{0}{1} {2} {3} {4} {5} {6} {7}".format(" " * INDENT_WIDTH * indent_level, path, result[path].all_in_archive, photos[path].size, photos[path].signature, photos[path].userTags, result[path].md5_match, result[path].signature_match)
            
    def create_json_tree(self, photos, result, top = None):
        '''Recursively create json representation of file tree for use by jstree'''
        if top is None:
            top = photos.path
        json_tree = {"data" : {"title":os.path.basename(top),"icon":self.set_icon(top, result)}, "children" : []}
        for filepath in photos[top].filepaths:
            json_tree["children"].append({"data" : {"title" : "{0} {1} {2}".format(os.path.basename(filepath), result[filepath].md5_match, result[filepath].signature_match), "icon":self.set_icon(filepath, result)}})
        for dirpath in photos[top].dirpaths:
            json_tree["children"].append(self.create_json_tree(photos, result, dirpath))
        return json_tree
        
def main():
#TODO:  Parse options:  -u update, -p print tree -j print json -n node
#    logfile = "/home/scott/Desktop/PythonPhoto/log.txt"
    logfile = "C:/Users/scott_jackson/Documents/Programming/PhotoManager/lap_log.txt"
#    candidate = "C:\Users\scott_jackson\Desktop\newpickleorigupdate.txt"
#    candidate_pickle_file = "C:\Users\scott_jackson\Desktop\lap_pickle.txt"
#    candidate_path = "C:\Users\scott_jackson\Pictures\Process\\20111123"
#    candidate_pickle_file = "C:/Users/scott_jackson/Documents/Programming/PhotoManager/smitherspickle.txt"
    archive_pickle_file = "C:/Users/scott_jackson/Documents/Programming/PhotoManager/smitherspickle.txt"
    candidate_pickle_file = "C:\\Users\\scott_jackson\\Documents\\Programming\\PhotoManager\\lap_100CANON_pickle.txt"
#    candidate_path = "/home/shared/Photos/2008"
#    archive_pickle_file = "C:\Users\scott_jackson\Desktop\jsonpickle.txt"
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(filename = logfile, format = LOG_FORMAT, level = logging.DEBUG, filemode = 'w')
    candidate_pickle = pickle_manager.photo_pickler(candidate_pickle_file)
    candidate = candidate_pickle.loadPickle()
    archive_pickle = pickle_manager.photo_pickler(archive_pickle_file)
    archive = archive_pickle.loadPickle()
    candidate = archive  #<-*****************Watch this!!!!!!!!!!!!!!++++++++==============
    [result, all_in_archive, none_in_archive] = PhotoFunctions.is_candidate_in_archive(candidate, candidate, "/media/rmv1/Photos/2009", "/media/rmv1/Photos/2009")
    for row in result.candidate.keys():
        print row, "|", candidate[row].size, "|", candidate[row].md5, "|", result[row].all_in_archive, "|", result[row].none_in_archive, "|", result[row].md5_match, "|", result[row].signature_match

    print "****************************"
#    print_top_level_duplicates_tree(candidate, result)
    PhotoFunctions.print_tree(candidate, result)
    
if __name__ == "__main__":
    sys.exit(main())
        