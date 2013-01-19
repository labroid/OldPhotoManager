'''
Created on Oct 21, 2011

@author: scott_jackson

'''
import sys
import os
import time
import datetime
import logging
import pyexiv2
import socket
import photo_functions
import MD5sums
from TiffImagePlugin import PHOTOSHOP_CHUNK

class photo_data:
    def __init__(self):
        self.isdir = False
        self.size = 0
        self.mtime = -(sys.maxint - 1) #Set default time to very old
        self.timestamp = datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
        self.gotTags = False
        self.signature = ''
        self.fileMD5 = ''
        self.userTags = ''
        self.inArchive = False
        self.signature_match = []
        self.signature_and_tags_match = []
        self.dirpaths = []
        self.filepaths = []      
        
class photo_collection:
    def __init__(self):
        self.host = ''
        self.path = ''
        self.photo = dict()
        self.pickle = None
        self.datasetChanged = False
        
    def __getitem__(self, key):
        return self.photo[key]
    
    def __setitem__(self, key, value):
        self.photo[key] = value
        
logger = logging.getLogger()
    
def create_collection(path):
    logger.debug("Photo collection data structure does not exist:  creating it.")
    photos = photo_collection()
    photos.host = socket.gethostname()
    photos.path = os.path.normpath(path)
    update_collection(photos)
    return(photos)
    
def update_collection(photos):
    start_time = time.time()
    if photos.host == socket.gethostname():
        logger.debug("Updating photo collection at {0}:{1}".format(photos.host, photos.path))
        update_tree(photos)
        prune_old_nodes(photos)
        update_file_stats(photos)
        extract_populate_tags(photos)
        elapsed_time = time.time() - start_time
        logger.info("Total files: {0}, Elapsed time: {1:.2} seconds or {2} ms/file".format(len(photos.photo), elapsed_time, elapsed_time/len(photos.photo)))
        photo_functions.populate_tree_sizes(photos)
        photo_functions.populate_tree_signatures(photos)
    else:
        logger.warning('Collection not on this machine; database will not be updated')

def update_tree(photos, top = None):
    '''Descend photo tree from top and add photo instances for each node and leaf of the tree.
       If leaves exist, only update them if the mtime or size have changed.
    '''
        
    def walkError(walkErr):
        print "Error", walkErr.errno, walkErr.strerror
        raise
    
    if top is None:
        top = photos.path
        
    logger.info("Updating tree for {0}".format(top))
    if os.path.isfile(top):  #Handle the case of a file as os.walk does not handle files
        photos[top] = photo_data()
    else:
        for dirpath, dirnames, filenames in os.walk(top, onerror = walkError):
            dirpaths = [os.path.join(dirpath, dirname) for dirname in dirnames]
            filepaths = [os.path.join(dirpath, filename) for filename in filenames]
            if not dirpath in photos.photo.keys():
                logger.info("New directory detected: {0}".format(dirpath))
                photos[dirpath] = photo_data()
            #Re-initialize properties of an existing node - would like to use __init()__ but not sure how
            #Another possible option is to delete an existing node and re-instantiate it
            photos[dirpath].size = 0
            photos[dirpath].mtime = -(sys.maxint - 1) #Set default time to very old
            photos[dirpath].timestamp = datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
            photos[dirpath].gotTags = False
            photos[dirpath].signature = ''
            photos[dirpath].fileMD5 = ''
            photos[dirpath].userTags = ''
            photos[dirpath].inArchive = True
            photos[dirpath].signature_match = []
            photos[dirpath].signature_and_tags_match = []
            photos[dirpath].isdir = True
            photos[dirpath].dirpaths = dirpaths
            photos[dirpath].filepaths = filepaths
            for filepath in filepaths:
                if filepath in photos.photo:
                    file_stat = stat_node(filepath)
                    if photos[filepath].size != file_stat.st_size or photos[filepath].mtime != file_stat.st_mtime:
                        photos[filepath].gotTags = False
                else:
                    logger.info("New File detected: {0}".format(filepath))
                    photos[filepath] = photo_data()    
                photos[filepath].inArchive = True
    logger.info("Done extracting tree for {0}".format(top))
    
def prune_old_nodes(photos):
    '''Prune nodes from the photo collection that are no longer present.
       Also reset the inArchive flag to False so it can be used for future duplicate tracking
    '''
    logger.info("Pruning old nodes from {0}".format(photos.path))
    for path in photos.photo.keys():
        if not photos[path].inArchive:
            logger.info("Pruning {0}".format(path))
            del photos.photo[path]
        else:
            photos[path].inArchive = False

def stat_node(nodepath):
    try:
        file_stat = os.stat(nodepath)
    except:
        logger.error("Can't stat file at {0}".format(nodepath))
        sys.exit(1)
    return(file_stat)

def update_file_stats(photos, path = None): 
    '''Populate file size and mtime for each photo that is a file
    '''
    if path is None:
        path = photos.path
    logger.info('Populating file stats for {0}'.format(path))
        
    for filepath in photos.photo.keys():  
        if os.path.isfile(filepath):
            file_stat = stat_node(filepath)
            photos[filepath].size = file_stat.st_size
            photos[filepath].mtime = file_stat.st_mtime
    logger.info('Done populating file stats for {0}'.format(photos.path))

def get_file_signature(photos, tags, filepath):
    LENGTH_LIMIT = 1048576  #Max length for a non-PHOTO_FILES, otherwise a truncated MD5 is computed
    TEXT_FILES = ['.ini', '.txt']  #file types to be compared ignoring CR/LF for OS portability.  Use lower case (extensions will be lowered before comparison
    if tags is not None and len(tags.previews) > 0:
        signature = photo_functions.thumbnailMD5sum(tags)
    else:
        if str.lower(os.path.splitext(filepath)[1]) in TEXT_FILES:
            signature = MD5sums.text_file_MD5_signature(filepath)
        else:
            if photos[filepath].size < LENGTH_LIMIT:
                signature = MD5sums.fileMD5sum(filepath)
            else:
                signature = MD5sums.truncatedMD5sum(filepath, LENGTH_LIMIT)
    return(signature)
    
def extract_populate_tags(photos, filelist = []):
    PHOTO_FILES = [".jpg", ".png"]  #Use lower case as extensions will be cast to lower case for comparison
    PROGRESS_COUNT = 500 #How often to report progress in units of files
    if not filelist:
        filelist = photos.photo.keys()
    total_files = len(filelist)
    logger.info("Extracting tags for {0}.  File count = {1}".format(photos.path, total_files))
    start_time = time.time()
    for file_count, photo_file in enumerate(filelist, start = 1):
        if not file_count % PROGRESS_COUNT:
            elapsed_time = time.time() - start_time
            total_time_projected = float(elapsed_time) / float(file_count) * total_files
            time_remaining = float(elapsed_time) / float(file_count) * float(total_files - file_count)
            logger.info("{0} of {1} = {2:.2f}%, {3:.1f} seconds, time remaining: {4} of {5}".format(file_count, total_files, 1.0 * file_count / total_files * 100.0, elapsed_time, str(datetime.timedelta(seconds = time_remaining)),str(datetime.timedelta(seconds = total_time_projected))))
        if not photos[photo_file].isdir and not photos[photo_file].gotTags:
            
            if str.lower(os.path.splitext(photo_file)[1]) in PHOTO_FILES:
                tags = getTagsFromFile(photo_file)
                if tags is None:
                    logger.warn("Bad tags in: {0}".format(photo_file))
                else:
                    photos[photo_file].userTags = photo_functions.getUserTagsFromTags(tags)
                    photos[photo_file].timestamp = photo_functions.getTimestampFromTags(tags)  
            else:
                tags = None   
            photos[photo_file].signature = get_file_signature(photos, tags, photo_file)  #Get signature should now do the thumbnailMD5 and if not find another signature.
            photos[photo_file].gotTags = True
            photos.datasetChanged = True 
    elapsed_time = time.time() - start_time
    logger.info("Tags extracted.  Elapsed time: {0:.0g} seconds or {1:.0g} ms per file = {2:.0g} for 100k files".format(elapsed_time, elapsed_time/file_count * 1000.0, elapsed_time/file_count * 100000.0/60.0))
    return(photos.datasetChanged)

def getTagsFromFile(filename):
    try:
        metadata = pyexiv2.ImageMetadata(filename)
        metadata.read()
#    except ValueError:
#        print "getTagsFromFile():", filename, "ValueError"
#        return(None)
#    except KeyError:
#        print "getTagsFromFile():", filename, "KeyError"
#        return(None)
#    except TypeError:
#        print "getTagsFromFile():", filename, "TypeError"
#        return(None)
    except IOError as err:  #The file contains photo of an unknown image type or file missing or can't be opened
        logger.warning("getTagesFromFile(): %s IOError, errno = %s, strerror = %s args = %s", filename, str(err.errno), err.strerror, err.args)
        return(None)
    except:
        logger.error("getTagesFromFile(): %s Unknown Error Trapped, errno = %s, strerror = %s args = %s", filename, str(err.errno), err.strerror, err.args)
        return(None)
    return(metadata)

