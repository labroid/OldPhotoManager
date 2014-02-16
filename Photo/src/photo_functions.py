'''
Created on Nov 23, 2011

@author: scott_jackson
'''
#pylint: disable=line-too-long

import os.path
import sys
import logging
import collections
import MD5sums
import pickle_manager


class PhotoFunctions(object):
    """Not sure if docstring goes here or under __init__ TODO
    Creates a data structure that contains information about duplicate photos in a PhotoData object, or between two PhotoData objects.
    Note:  This function has the side effect of 'filling out' the missing roll-ups of MD5 sums, size sums, and signature sums
    of the incoming PhotoData objects.  This usually isn't an issue since they come from persistent stores or are regenerated
    before calling this object
    """

    class NodeState(object):
        def __init__(self):
            self.all_in_archive = False
            self.none_in_archive = False
            self.md5_match = []
            self.signature_match = []
    
    def __init__(self, candidate, compare_method = None, archive = None,  candidate_path = None, archive_path = None):
        """Comment under init to see where it shows up"""
        logging.info('Setting up and initializing data structure...')
        
        if candidate is None:
            logging.critical("Error - candidate PhotoData instance must be identified.  Got:{}".format(candidate))
            sys.exit(-1)
        else:
            self.candidate = candidate
            
        if compare_method is None or compare_method.lower() not in ['all', 'not self', 'none']:
            logging.critical("Error - compare_method must be specified, and must be one of ['all', 'not self', 'different tree'].  Got:{}".format(compare_method))
            sys.exit(-1)
        else:
            self.compare_method = compare_method
            
        if archive is None:
            self.archive = None
            self.archive_path = None
        else:
            self.archive = archive
            if archive_path is None:
                self.archive_path = archive.path
            else:
                self.archive_path = archive_path
       
        if candidate_path is None:
            self.candidate_path = candidate.path
        else:
            self.candidate_path = candidate_path
            
        self.node = dict()  
#        self.compare_method = self.set_comparison_type()                  
        self.initialize_result_structure(self.candidate_path)
        self.populate_tree_sizes(candidate)
        self.populate_tree_md5(candidate)
        #self.populate_tree_signatures(candidate) #TODO Need to add this one!
        if (archive != candidate) and archive is not None:
            self.populate_tree_sizes(archive)
            self.populate_tree_md5(archive)
            #self.populate_tree_signatures(archive) #TODO Need to add this one!
        self.populate_duplicate_candidates()
        return ()
    
    def __getitem__(self, key):
        return self.node[key]
    
    def __setitem__(self, key, value):
        self.node[key] = value 
    
    def initialize_result_structure(self, path):
#This was originally recursive; I don't think there is a need for that since PhotoData objects have all nodes represented.  Rewritten to flat structure
#It was originally recursive in case one only wanted to descend a part of the tree, but it seems fast enough that that isn't necessary.  Also, the other functions
#in this class don't necessarily respect the path variable, but only use the whole tree (check this!)          
#         self.node[path] = self.NodeState()
#         for dirpath in self.candidate[path].dirpaths:
#             self.initialize_result_structure(dirpath)
#         for filepath in self.candidate[path].filepaths:
#             self.node[filepath] = self.NodeState()
        for path in self.candidate.node:
            self.node[path] = self.NodeState()
        return()
            
    def set_comparison_type(self):
        #if candidate and archive are different, record all duplicates
        #if candidate and archive are same, and root same, record duplicates if not self
        #if candidate and archive are same, and root different, record duplicates only if in different tree
        if self.candidate != self.archive:  #TODO should we use enumerated type here?  I don't like this but don't know if there is something more pythonic...
            return('all')
        if self.archive is None or (self.candidate_path == self.archive_path):
            return('not self')
        else:
            return('different tree')        
        logging.critical("Should never get here!")
        assert False, "Should never get here!  Check logs."
    
    def populate_tree_sizes(self, photos, top = None):
        '''Recursively descends photo tree structure and computes/populates sizes
        '''
        if top is None:  #This is initial recursive call
            logging.info("Computing cumulative sizes for file tree.") 
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
        '''Recursively descends PhotoData structure and computes aggregated md5
        Errors if a file does not have md5 populated as this is risky for deleting files
        with un-computed MD5, or resulting in incorrect directory node hashes; computes md5 for directory structure
        '''
        if top is None:  #This is initial recursive call
            logging.info("Populating cumulative tree md5 for file tree.")
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
           [This is recursive as opposed to linear because of the ability to descend an archive_path
           without having to compute the whole tree]
        '''
        if hash_dict is None:  #First iteration of recursion
            logging.info("Building md5 hash dictionary for {0}".format(self.archive_path))
            if path is None:
                path = self.archive_path
            hash_dict = collections.defaultdict(list)
            
        for dirpath in self.archive[path].dirpaths:
            self.build_hash_dict(dirpath, hash_dict)
        for filepath in [self.archive[path].filepaths, path]:
            hash_dict[self.archive[filepath].md5].append(filepath)
        return hash_dict
    
    def populate_duplicate_candidates(self, path = None):
        if path is None: #First iteration in recursion  TODO:  This isn't right - if the user passes a path this will think it isn't first iteration!  Look at node_inclustion_check - that's correct.
            logging.info("Populating duplicate candidates...")
            path = self.candidate_path
            #Clear all duplicate states from result  TODO Do we really need to do this?  Is it not a fresh structure every time unless you call this function directly?  Maybe we hide it....
            for nodepath in self.node:
                self.node[nodepath].__init__()
            archive_dict = self.build_hash_dict(self.archive_path)
            
        for dirpath in self.node[path].dirpaths:
            self.populate_duplicate_candidates(dirpath)
            
        for nodepath in [self.node[path].filepaths, path]:
            md5 = self.node[nodepath].md5
            if md5 in archive_dict:
                for candidate in archive_dict[md5]:
                    if (self.compare_method == 'all' or 
                    (self.compare_method == 'not self' and nodepath != candidate) or 
                    (self.compare_method == 'different tree' and not self.candidate_path in candidate)):
                        self.node[nodepath].md5_match.append(candidate)
        return
        
    def node_inclusion_check(self, path, top = True):
        '''Recurse through tree recording status of nodes
        '''
        if top:
            logging.info("Determining if node is duplicated.")
        all_in_archive = True  #Seed value; logic will falsify this value if any files are missing     
        none_in_archive = True #Seed value
        for dirpath in self[path].dirpaths:
            [all_in_tree, none_in_tree] = self.node_inclusion_check(dirpath, False)
            all_in_archive = all_in_archive and all_in_tree
            none_in_archive = none_in_archive and none_in_tree
        for filepath in self[path].filepaths:
            if self[filepath].md5_match or (os.path.basename(filepath) in ['.picasa.ini', 'Picasa.ini', 'picasa.ini', 'Thumbs.db']): #True if the md5_match list isn't empty OR These file types don't count. TODO Make this configurable
                self[filepath].all_in_archive = True
                self[filepath].none_in_archive = False
                #all_in_archive = all_in_archive and True   #Shown here for completeness, commented out since it results in a boolean identity
                none_in_archive = False
            else:
                self[filepath].all_in_archive = False
                self[filepath].none_in_archive = True
                all_in_archive = False
        self[path].all_in_archive = all_in_archive
        self[path].none_in_archive = none_in_archive
        
        if top:
            logging.info("Done determining if node is duplicated.")
            
        return(all_in_archive, none_in_archive)
    
    #It would be nice if there were a function here to figure out if directories are duplicated - and name the duplicated directory.  Do we have some way of determining the 'master'? (e.g. shortest path, or some regular expression of what is a master?
      
    def print_tree(self, top = None, indent_level = 0):
        '''Print Photo collection using a tree structure'''
        if top is None:
            top = self.candidate_path
        if indent_level == 0:  #Used to detect first call of recursion
            print "Photo Collection at {0}:{1}".format(self.candidate.host, self.candidate_path)    
        self._print_tree_line(top, indent_level)
        indent_level += 1
        for filepath in self[top].filepaths:
            self._print_tree_line(filepath, indent_level)
        for dirpath in self[top].dirpaths:
            self.print_tree(dirpath, indent_level)
        return
    
#     def print_top_level_duplicates_tree(self, PhotoData, result, top = None, indent_level = 0):
#         '''Print Photo collection using a tree structure only showing the top node of an 'included' subtree'''
#         if top is None:
#             top = PhotoData.path
#         if indent_level == 0:  #Used to detect first call of recursion
#             print "Photo Collection at {0}:{1} pickled at {2}".format(PhotoData.host, PhotoData.path, PhotoData.pickle)    
#         self._print_tree_line(PhotoData, result, top, indent_level)
#         if result[top].all_in_archive and PhotoData[top].isdir:
#             return  #Stop descending as soon as you find a directory that is in the archive from there down...
#         indent_level += 1
#         for filepath in PhotoData[top].filepaths:
#             self._print_tree_line(PhotoData, result, filepath, indent_level)
#         for dirpath in PhotoData[top].dirpaths:
#             self.print_top_level_duplicates_tree(PhotoData, result, dirpath, indent_level)
#         return
        
    def _print_tree_line(self, path, indent_level):
        INDENT_WIDTH = 3 #Number of spaces for each indent level
        indent = " " * INDENT_WIDTH * indent_level
        if self[path].isdir:
            print "{0}{1} {2} {3} {4} {5} {6}".format(indent, path, self[path].all_in_archive, self[path].none_in_archive, self[path].size, self[path].signature, self[path].md5_match)
        else:
            print "{0}{1} {2} {3} {4} {5} {6} {7}".format(indent, path, self[path].all_in_archive, self[path].size, self[path].signature, self[path].user_tags, self[path].md5_match, self[path].signature_match)
            
#     def create_json_tree(self, top = None):
#         '''Recursively create json representation of file tree for use by jstree'''
#         if top is None:
#             top = self.candidate_path
#         json_tree = {"data" : {"title":os.path.basename(top),"icon":self.set_icon(top, result)}, "children" : []}
#         for filepath in PhotoData[top].filepaths:
#             json_tree["children"].append({"data" : {"title" : "{0} {1} {2}".format(os.path.basename(filepath), result[filepath].md5_match, result[filepath].signature_match), "icon":self.set_icon(filepath, result)}})
#         for dirpath in PhotoData[top].dirpaths:
#             json_tree["children"].append(self.create_json_tree(PhotoData, result, dirpath))
#         return json_tree
        
def main():
#TODO:  Parse options:  -u update, -p print tree -j print json -n node
    logfile = "C:/Users/scott_jackson/Documents/Programming/PhotoManager/lap_log.txt"
    archive_pickle_file = "C:/Users/scott_jackson/Documents/Programming/PhotoManager/smitherspickle.txt"
    candidate_pickle_file = "C:\\Users\\scott_jackson\\Documents\\Programming\\PhotoManager\\lap_100CANON_pickle.txt"

    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(filename = logfile, format = LOG_FORMAT, level = logging.DEBUG, filemode = 'w')
    
    candidate = pickle_manager.PhotoPickler(candidate_pickle_file).load_pickle()
    archive = pickle_manager.PhotoPickler(archive_pickle_file).load_pickle()
    
    result = PhotoFunctions(candidate, archive, candidate_path = None, archive_path = None)
    
    print "Results for {}: Total in Candidate: {}, All in Archive: {}, None in Archive: {}".format(result.candidate_path, result[result.candidate_path].all_in_archive, result[result.candidate_path].none_in_archive)
    
    for row in result.node:
        print row, "|", candidate[row].size, "|", candidate[row].md5, "|", result[row].all_in_archive, "|", result[row].none_in_archive, "|", result[row].md5_match, "|", result[row].signature_match

    print "****************************"
#    print_top_level_duplicates_tree(candidate, result)
    result.print_tree(candidate, result)
    
if __name__ == "__main__":
    sys.exit(main())
        