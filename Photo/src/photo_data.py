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

logger = logging.getLogger()

class photo_collection(object):
    def __init__(self):
        self.host = ''
        self.path = ''
        self.node = dict()
        self.pickle = None
        self.datasetChanged = False
        
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
    
def create_collection(path):
    logger.debug("Creating photo collection.")
    photos = photo_collection()
    photos.host = socket.gethostname()
    photos.path = os.path.normpath(path)
    update_collection(photos)
    return photos
        
def update_collection(photos):
    start_time = time.time()
    if photos.host != socket.gethostname():
        logger.warning('Collection not on this machine; database will not be updated')
        return
    logger.debug("Updating photo collection at {0}:{1}".format(photos.host, photos.path))
    update_tree(photos)
    prune_old_nodes(photos)
    update_file_stats(photos)
    extract_populate_tags(photos)
    elapsed_time = time.time() - start_time
    logger.info("Total nodes: {0}, Elapsed time: {1:.2} seconds or {2} ms/file".format(len(photos.node), elapsed_time, elapsed_time/len(photos.node)))
    return
    
def update_tree(photos, top = None):
    '''Descend photo tree from top and add node instances for each node and leaf of the tree.
       If leaves exist, only update them if the mtime or size have changed.
    '''
    def walkError(self, walkErr):
        print "Error", walkErr.errno, walkErr.strerror
        raise
    
    if top is None:
        top = photos.path
        
    logger.info("Updating tree for {0}".format(top))
    if os.path.isfile(top):
        photos[top].isDir = False
        photos[top].inArchive = True
    else:
        for dirpath, dirnames, filenames in os.walk(top, onerror = walkError):
            dirpaths = [os.path.join(dirpath, dirname) for dirname in dirnames]
            filepaths = [os.path.join(dirpath, filename) for filename in filenames]
            if not dirpath in photos.node:
                logger.info("New directory detected: {0}".format(dirpath))
                photos.node[dirpath] = node_info()
            photos[dirpath].size = 0
            photos[dirpath].isdir = True
            photos[dirpath].signature = ''
            photos[dirpath].dirpaths = dirpaths
            photos[dirpath].filepaths = filepaths
            photos[dirpath].inArchive = True
            for filepath in filepaths:
                if filepath in photos.node:
                    file_stat = stat_node(filepath)
                    if photos[filepath].size != file_stat.st_size or photos[filepath].mtime != file_stat.st_mtime:
                        photos[filepath].gotTags = False
                else:
                    logger.info("New File detected: {0}".format(filepath))
                    photos.node[filepath] = node_info()
                photos[filepath].inArchive = True
    logger.info("Done extracting tree for {0}".format(top))
    return
        
def prune_old_nodes(photos):
    '''Prune nodes from the photo collection that are no longer present.
       Also reset the inArchive flag to False so it can be used for future duplicate tracking
    '''
    logger.info("Pruning old nodes from {0}".format(photos.path))
    for filepath in photos.node.keys():
        if not photos[filepath].inArchive:
            logger.info("Pruning {0}".format(filepath))
            del photos.node[filepath]
        else:
            photos[filepath].inArchive = False
    return

def update_file_stats(photos, path = None): 
    '''Populate file size and mtime for each photo that is a file
    '''
    if path is None:
        path = photos.path
    logger.info('Populating file stats for {0}'.format(path))
        
    for filepath in photos.node.keys():  
        file_stat = stat_node(filepath)
        photos[filepath].size = file_stat.st_size
        photos[filepath].mtime = file_stat.st_mtime
    logger.info('Done populating file stats for {0}'.format(photos.path))
    return

def stat_node(nodepath):
    try:
        file_stat = os.stat(nodepath)
    except:
        logger.error("Can't stat file at {0}".format(nodepath))
        sys.exit(1)
    return(file_stat)    
        
def extract_populate_tags(photos, filelist = None):
    PHOTO_FILES = [".jpg", ".png"]  #Use lower case as extensions will be cast to lower case for comparison.  TODO: make this configurable
    PROGRESS_COUNT = 500 #How often to report progress in units of files
    if filelist is None:
        filelist = [x for x in photos.node.keys() if not photos.node[x].isdir]
    total_files = len(filelist)
    logger.info("Extracting tags for {0}.  File count = {1}".format(photos.path, total_files))
    start_time = time.time()
    tag_extract_count = 0
    for file_count, photo_file in enumerate(filelist, start = 1):
        if not file_count % PROGRESS_COUNT:
            elapsed_time = time.time() - start_time
            total_time_projected = float(elapsed_time) / float(file_count) * total_files
            time_remaining = float(elapsed_time) / float(file_count) * float(total_files - file_count)
            logger.info("{0} of {1} = {2:.2f}%, {3:.1f} seconds, time remaining: {4} of {5}".format(file_count, total_files, 1.0 * file_count / total_files * 100.0, elapsed_time, str(datetime.timedelta(seconds = time_remaining)),str(datetime.timedelta(seconds = total_time_projected))))
        if not photos[photo_file].gotTags:
            tag_extract_count += 1
            if str.lower(os.path.splitext(photo_file)[1]) in PHOTO_FILES:
                tags = getTagsFromFile(photo_file)
                if tags is None:
                    logger.warn("Bad tags in: {0}".format(photo_file))
                else:
                    photos[photo_file].userTags = getUserTagsFromTags(tags)
                    photos[photo_file].timestamp = getTimestampFromTags(tags)  
            else:
                tags = None   
            photos[photo_file].signature = get_file_signature(photos, tags, photo_file)  #Get signature should now do the thumbnailMD5 and if not find another signature.
            photos[photo_file].gotTags = True
            photos.datasetChanged = True 
    elapsed_time = time.time() - start_time
    logger.info("Tags extracted.  {0} Updated. Elapsed time: {1:.0g} seconds or {2:.0g} ms per file = {3:.0g} for 100k files".format(tag_extract_count, elapsed_time, elapsed_time/total_files * 1000.0, elapsed_time/total_files * 100000.0/60.0))
    return(photos.datasetChanged)
        
def get_file_signature(photos, tags, filepath):
    LENGTH_LIMIT = 1048576  #Max length for a non-PHOTO_FILES, otherwise a truncated MD5 is computed
    TEXT_FILES = ['.ini', '.txt']  #file types to be compared ignoring CR/LF for OS portability.  Use lower case (extensions will be lowered before comparison
    if tags is not None and len(tags.previews) > 0:
        signature = thumbnailMD5sum(tags)
    else:
        if str.lower(os.path.splitext(filepath)[1]) in TEXT_FILES:
            signature = MD5sums.text_file_MD5_signature(filepath)
        else:
            if photos[filepath].size < LENGTH_LIMIT:
                signature = MD5sums.fileMD5sum(filepath)
            else:
                signature = MD5sums.truncatedMD5sum(filepath, LENGTH_LIMIT)
    return(signature)

def getTimestampFromTags(tags):
    if 'Exif.Photo.DateTimeOriginal' in tags.exif_keys:
        timestamp = tags['Exif.Photo.DateTimeOriginal'].value
    else:
        timestamp = datetime.datetime.strptime('1800:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
    return(timestamp)

def getUserTagsFromTags(tags):
    if 'Xmp.dc.subject' in tags.xmp_keys:
        return(tags['Xmp.dc.subject'].value)
    else:
        return('NA')

def getTagsFromFile(filename):
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
    
def thumbnailMD5sum(tags):
    if len(tags.previews) > 0:
        temp = MD5sums.stringMD5sum(tags.previews[0].data)
    else:
        temp = MD5sums.stringMD5sum("0")
    return(temp)
    
def listZeroLengthFiles(photos):
    zeroLengthNames = []
    for target in photos.node.keys():
        if photos[target].size == 0:
            zeroLengthNames.append(target)
    return(zeroLengthNames)
    
def get_statistics(photos):
    class statistics:
        def __init__(self):
            self.dircount = 0
            self.photocount = 0
            self.unique_count = 0
            self.dup_count = 0
            self.dup_fraction = 0
    stats = statistics()
    photo_set = set()
    for archive_file in photos.node.keys():
        if photos[archive_file].isdir:
            stats.dircount += 1
        else:
            stats.photocount += 1
        sig = photos[archive_file].signature
        if sig in photo_set:
            stats.dup_count += 1
        else:
            photo_set.add(sig)
    stats.unique_count = len(photo_set)
    stats.dup_fraction = stats.dup_count * 1.0 / (stats.photocount + stats.dup_count)
    logger.info("Collection statistics:  Directories = {0}, Files = {1}, Unique signatures = {2}, Duplicates = {3}, Duplicate Fraction = {4:.2%}".format(
        stats.dircount, stats.photocount, stats.unique_count, stats.dup_count, stats.dup_fraction))    
    return(stats)
            
def print_statistics(photos):
    result = get_statistics(photos)
    print "Directories: {0}, Files: {1}, Unique photos: {2}, Duplicates: {3} ({4:.2%})".format(result.dircount, result.photocount, result.unique_count, result.dup_count, result.dup_fraction)
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
    return
        
def print_tree(photos, top = None, indent_level = 0, first_call = True):
    '''Print Photo collection using a tree structure'''
    if top is None:
        top = photos.path
    if first_call:
        print "Photo Collection at {0}:{1} pickled at {2}".format(photos.host, photos.path, photos.pickle)    
    print_tree_line(photos, top, indent_level)
    indent_level += 1
    for filepath in photos[top].filepaths:
        print_tree_line(photos, filepath, indent_level)
    for dirpath in photos[top].dirpaths:
        print_tree(photos, dirpath, indent_level, False)
    return
    
def print_tree_line(photos, path, indent_level):
    INDENT_WIDTH = 3 #Number of spaces for each indent level
    if photos[path].isdir:
        print "{0}{1} {2} {3}".format(" " * INDENT_WIDTH * indent_level, path, photos[path].size, photos[path].signature)
    else:
        print "{0}{1} {2} {3} {4}".format(" " * INDENT_WIDTH * indent_level, path, photos[path].size, photos[path].signature, photos[path].userTags)
            
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
                update_collection(photos)
                pickle.dumpPickle(photos)
        else:
            logger.info("Scanning photos {0}; will pickle to {1}".format(node_path, pickle.picklePath))
            photos = create_collection(node_path)
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
    print_tree(photos)
    print "Done!"

if __name__ == "__main__":
    sys.exit(main())

