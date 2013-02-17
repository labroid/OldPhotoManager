'''
Created on Oct 21, 2011

@author: scott_jackson

'''
import sys
import os
import time
import datetime
import logging
import getopt
import pyexiv2
import socket
import MD5sums
import pickle_manager

class photo_collection(object):
    def __init__(self, path):
#        self.logger = logging.getLogger()
        self.host = ''
        self.path = ''
        self.node = dict()
        self.pickle = None
        self.datasetChanged = False
        self.create_collection(path)
        
    def __getitem__(self, key):
        return self.node[key]
    
    def __setitem__(self, key, value):
        self.node[key] = value 
        
    class node_info(object):
        def __init__(self):
            self.isdir = False
            self.size = 0
            self.signature = ''
            self.dirpaths = []
            self.filepaths = []      
            self.mtime = -(sys.maxint - 1) #Set default time to very old
            self.timestamp = datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
            self.gotTags = False
            self.userTags = ''
            self.inArchive = False 
    
    def create_collection(self, path):
        logger = logging.getLogger()
        logger.debug("Creating photo collection.")
        self.host = socket.gethostname()
        self.path = os.path.normpath(path)
        self.update_collection()
        return
        
    def update_collection(self):
        logger = logging.getLogger()
        start_time = time.time()
        if self.host != socket.gethostname():
            logger.warning('Collection not on this machine; database will not be updated')
            return
        logger.debug("Updating photo collection at {0}:{1}".format(self.host, self.path))
        self.update_tree()
        self.prune_old_nodes()
        self.update_file_stats()
        self.extract_populate_tags()
        elapsed_time = time.time() - start_time
        logger.info("Total nodes: {0}, Elapsed time: {1:.2} seconds or {2} ms/file".format(len(self.node), elapsed_time, elapsed_time/len(self.node)))
        
    def update_tree(self, top = None):
        '''Descend photo tree from top and add node instances for each node and leaf of the tree.
           If leaves exist, only update them if the mtime or size have changed.
        '''
        def walkError(self, walkErr):
            print "Error", walkErr.errno, walkErr.strerror
            raise
        
        if top is None:
            top = self.path
            
        logger = logging.getLogger()
        logger.info("Updating tree for {0}".format(top))
        if os.path.isfile(top):
            self.node[top].isDir = False
            self.node[top].inArchive = True
        else:
            for dirpath, dirnames, filenames in os.walk(top, onerror = walkError):
                dirpaths = [os.path.join(dirpath, dirname) for dirname in dirnames]
                filepaths = [os.path.join(dirpath, filename) for filename in filenames]
                if not dirpath in self.node:
                    logger.info("New directory detected: {0}".format(dirpath))
                    self.node[dirpath] = self.node_info()
                self[dirpath].size = 0
                self[dirpath].isdir = True
                self[dirpath].signature = ''
                self[dirpath].dirpaths = dirpaths
                self[dirpath].filepaths = filepaths
                self[dirpath].inArchive = True
                for filepath in filepaths:
                    if filepath in self.node:
                        file_stat = self.stat_node(filepath)
                        if self[filepath].size != file_stat.st_size or self[filepath].mtime != file_stat.st_mtime:
                            self.node[filepath].gotTags = False
                    else:
                        logger.info("New File detected: {0}".format(filepath))
                        self.node[filepath] = self.node_info()
                    self[filepath].inArchive = True
        logger.info("Done extracting tree for {0}".format(top))
        
    def prune_old_nodes(self):
        '''Prune nodes from the photo collection that are no longer present.
           Also reset the inArchive flag to False so it can be used for future duplicate tracking
        '''
        logger = logging.getLogger()
        logger.info("Pruning old nodes from {0}".format(self.path))
        for filepath in self.node.keys():
            if not self[filepath].inArchive:
                logger.info("Pruning {0}".format(filepath))
                del self.node[filepath]
            else:
                self[filepath].inArchive = False

    def update_file_stats(self, path = None): 
        '''Populate file size and mtime for each photo that is a file
        '''
        logger = logging.getLogger()
        if path is None:
            path = self.path
        logger.info('Populating file stats for {0}'.format(path))
            
        for filepath in self.node.keys():  
            file_stat = self.stat_node(filepath)
            self[filepath].size = file_stat.st_size
            self[filepath].mtime = file_stat.st_mtime
        logger.info('Done populating file stats for {0}'.format(self.path))

    def stat_node(self, nodepath):
        try:
            file_stat = os.stat(nodepath)
        except:
            logger = logging.getLogger()
            logger.error("Can't stat file at {0}".format(nodepath))
            sys.exit(1)
        return(file_stat)    
        
    def extract_populate_tags(self, filelist = None):
        PHOTO_FILES = [".jpg", ".png"]  #Use lower case as extensions will be cast to lower case for comparison.  TODO: make this configurable
        PROGRESS_COUNT = 500 #How often to report progress in units of files
        logger = logging.getLogger()
        if filelist is None:
            filelist = [x for x in self.node.keys() if not self.node[x].isdir]
        total_files = len(filelist)
        logger.info("Extracting tags for {0}.  File count = {1}".format(self.path, total_files))
        start_time = time.time()
        for file_count, photo_file in enumerate(filelist, start = 1):
            if not file_count % PROGRESS_COUNT:
                elapsed_time = time.time() - start_time
                total_time_projected = float(elapsed_time) / float(file_count) * total_files
                time_remaining = float(elapsed_time) / float(file_count) * float(total_files - file_count)
                logger.info("{0} of {1} = {2:.2f}%, {3:.1f} seconds, time remaining: {4} of {5}".format(file_count, total_files, 1.0 * file_count / total_files * 100.0, elapsed_time, str(datetime.timedelta(seconds = time_remaining)),str(datetime.timedelta(seconds = total_time_projected))))
            if not self[photo_file].gotTags:
                if str.lower(os.path.splitext(photo_file)[1]) in PHOTO_FILES:
                    tags = self.getTagsFromFile(photo_file)
                    if tags is None:
                        logger.warn("Bad tags in: {0}".format(photo_file))
                    else:
                        self[photo_file].userTags = self.getUserTagsFromTags(tags)
                        self[photo_file].timestamp = self.getTimestampFromTags(tags)  
                else:
                    tags = None   
                self[photo_file].signature = self.get_file_signature(tags, photo_file)  #Get signature should now do the thumbnailMD5 and if not find another signature.
                self[photo_file].gotTags = True
                self.datasetChanged = True 
        elapsed_time = time.time() - start_time
        logger.info("Tags extracted.  Elapsed time: {0:.0g} seconds or {1:.0g} ms per file = {2:.0g} for 100k files".format(elapsed_time, elapsed_time/total_files * 1000.0, elapsed_time/total_files * 100000.0/60.0))
        return(self.datasetChanged)
        
    def get_file_signature(self, tags, filepath):
        LENGTH_LIMIT = 1048576  #Max length for a non-PHOTO_FILES, otherwise a truncated MD5 is computed
        TEXT_FILES = ['.ini', '.txt']  #file types to be compared ignoring CR/LF for OS portability.  Use lower case (extensions will be lowered before comparison
        if tags is not None and len(tags.previews) > 0:
            signature = self.thumbnailMD5sum(tags)
        else:
            if str.lower(os.path.splitext(filepath)[1]) in TEXT_FILES:
                signature = MD5sums.text_file_MD5_signature(filepath)
            else:
                if self[filepath].size < LENGTH_LIMIT:
                    signature = MD5sums.fileMD5sum(filepath)
                else:
                    signature = MD5sums.truncatedMD5sum(filepath, LENGTH_LIMIT)
        return(signature)
    
    def getTimestampFromTags(self, tags):
        if 'Exif.Photo.DateTimeOriginal' in tags.exif_keys:
            timestamp = tags['Exif.Photo.DateTimeOriginal'].value
        else:
            timestamp = datetime.datetime.strptime('1800:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
        return(timestamp)
    
    def getUserTagsFromTags(self, tags):
        if 'Xmp.dc.subject' in tags.xmp_keys:
            return(tags['Xmp.dc.subject'].value)
        else:
            return('NA')
    
    def getTagsFromFile(self, filename):
        logger = logging.getLogger()
        try:
            metadata = pyexiv2.ImageMetadata(filename)
            metadata.read()
        except IOError as err:  #The file contains photo of an unknown image type or file missing or can't be opened
            logger.warning("getTagesFromFile(): %s IOError, errno = %s, strerror = %s args = %s", filename, str(err.errno), err.strerror, err.args)
            return(None)
        except:
            logger.error("getTagesFromFile(): %s Unknown Error Trapped, errno = %s, strerror = %s args = %s", filename, str(err.errno), err.strerror, err.args)
            return(None)
        return(metadata)
    
    def thumbnailMD5sum(self, tags):
        if len(tags.previews) > 0:
            temp = MD5sums.stringMD5sum(tags.previews[0].data)
        else:
            temp = MD5sums.stringMD5sum("0")
        return(temp)
        
    def listZeroLengthFiles(self):
        zeroLengthNames = []
        for target in self.node.keys():
            if self[target].size == 0:
                zeroLengthNames.append(target)
        return(zeroLengthNames)
        
    def get_statistics(self):
        logger = logging.getLogger()
        class statistics:
            def __init__(self):
                self.dircount = 0
                self.photocount = 0
                self.unique_count = 0
                self.dup_count = 0
                self.dup_fraction = 0
        stats = statistics()
        photo_set = set()
        for archive_file in self.node.keys():
            if self[archive_file].isDir:
                stats.dircount += 1
            else:
                stats.photocount += 1
            sig = self[archive_file].signature
            if sig in photo_set:
                stats.dup_count += 1
            else:
                photo_set.add(sig)
        stats.unique_count = len(photo_set)
        stats.dup_fraction = stats.dup_count * 1.0 / (stats.photocount + stats.dup_count)
        logger.info("Collection statistics:  Directories = {0}, Files = {1}, Unique signatures = {2}, Duplicates = {3}, Duplicate Fraction = {4:.2%}".format(
            stats.dircount, stats.photocount, stats.unique_count, stats.dup_count, stats.dup_fraction))    
        return(stats)
                
    def print_statistics(self):
        result = self.get_statistics()
        print "Directories: {0}, Files: {1}, Unique photos: {2}, Duplicates: {3} ({4:.2%})".format(result.dircount, result.photocount, result.unique_count, result.dup_count, result.dup_fraction)
        return

    def print_zero_length_files(self):
        zeroFiles = self.listZeroLengthFiles()
        if len(zeroFiles) == 0:
            print "No zero-length files."
        else:
            print "Zero-length files:"
            for names in zeroFiles:
                print names
            print ""
            
    def print_tree(self, top = None, indent_level = 0, first_call = True):
        '''Print Photo collection using a tree structure'''
        if top is None:
            top = self.path
        if first_call:
            print "Photo Collection at {0}:{1} pickled at {2}".format(self.host, self.path, self.pickle)    
        self.print_tree_line(top, indent_level)
        indent_level += 1
        for filepath in self[top].filepaths:
            self.print_tree_line(filepath, indent_level)
        for dirpath in self[top].dirpaths:
            self.print_tree(dirpath, indent_level, False)
        
    def print_tree_line(self, path, indent_level):
        INDENT_WIDTH = 3 #Number of spaces for each indent level
        if self.node[path].isdir:
            print "{0}{1} {2} {3}".format(" " * INDENT_WIDTH * indent_level, path, self[path].size, self[path].signature)
        else:
            print "{0}{1} {2} {3} {4}".format(" " * INDENT_WIDTH * indent_level, path, self[path].size, self[path].signature, self[path].userTags)
            
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
    if not node_path and not pickle_path: #Both paths undefined (None or blank string)
        logger.critical("Function called with no arguments.  Aborting.")
        sys.exit(1)
    elif node_path and not pickle_path:
        logger.info("Creating photo_collection instance for {0}".format(node_path))
        return(photo_collection(node_path))
    elif not node_path and pickle_path:
        logger.info("Unpacking pickle at {0}".format(pickle_path))
        pickle = pickle_manager.photo_pickler(pickle_path)
        return(pickle.loadPickle())
    else:  #Both paths are defined
        pickle = pickle_manager.photo_pickler(pickle_path)
        if pickle.pickleExists:
            logger.info("Loading pickle at {0} for {1}".format(pickle.picklePath, node_path))
            photos = pickle.loadPickle()
            if node_update:
                photos.update_collection()
                pickle.dumpPickle(photos)
        else:
            logger.info("Scanning photos {0}; will pickle to {1}".format(node_path, pickle.picklePath))
            photos = photo_collection(node_path)
            photos.pickle = pickle_path
            pickle.dumpPickle(photos)
        return(photos) 
    
#Stole this main from Guido van van Rossum at http://www.artima.com/weblogs/viewpost.jsp?thread=4829
#class Usage(Exception):
#    def __init__(self, msg):
#        self.msg = msg
#
#def main(argv=None):
#    if argv is None:
#        argv = sys.argv
#    try:
#        try:
#            opts, args = getopt.getopt(argv[1:], "h", ["help"])
#        except getopt.error, msg:
#             raise Usage(msg)
#        # more code, unchanged
#    except Usage, err:
#        print >>sys.stderr, err.msg
#        print >>sys.stderr, "for help use --help"
#        return 2
def main():
    photo_dir = "C:\Users\scott_jackson\Pictures\Process"
    pickle_file = "C:\Users\scott_jackson\Desktop\lap_pickle.txt"
    log_file = "C:\Users\scott_jackson\Desktop\lap_log.txt"
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(filename = log_file, format = LOG_FORMAT, level = logging.DEBUG, filemode = 'w')
    logging.getLogger()
    photos = get_photo_data(photo_dir, pickle_file)
    photos.print_tree()
    print "Done!"

if __name__ == "__main__":
    sys.exit(main())

