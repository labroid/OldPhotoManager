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
    start_time = time.time()
    logger.debug("Creating photo collection from {0}".format(path))
    photos = photo_collection()
    photos.host = socket.gethostname()
    photos.path = os.path.normpath(path)
    populate_tree(photos)
    populate_file_stats(photos)
    extract_populate_tags(photos)
    elapsed_time = time.time() - start_time
    logger.info("Total files: {0}, Elapsed time: {1:.2} seconds or {2} ms/file".format(len(photos.photo), elapsed_time, elapsed_time/len(photos.photo)))
    logger.info("Computing cumulative sizes for file tree")  #Logged here since function is recursive
    photo_functions.populate_tree_sizes(photos)
    logger.info("Computing cumulative signature for file tree")  #Logged here since function is recursive
    photo_functions.populate_tree_signatures(photos)
    return(photos)
    


def populate_tree(photos, top = None):
    '''Descend photo tree from top and add photo instances for each node and leaf of the tree
    '''
        
    def walkError(walkErr):
        print "Error", walkErr.errno, walkErr.strerror
        raise
    
    if top is None:
        top = photos.path
    logger.info("Populating tree for {0}".format(top))
    if os.path.isfile(top):  #Handle the case of a file as os.walk does not handle files
#        photos.photo[top] = photo_data()
        photos[top] = photo_data()
    else:
        for dirpath, dirnames, filenames in os.walk(top, onerror = walkError):
            dirpaths = [os.path.join(dirpath, dirname) for dirname in dirnames]
            filepaths = [os.path.join(dirpath, filename) for filename in filenames]
            photos[dirpath] = photo_data()
            photos[dirpath].isdir = True
            photos[dirpath].dirpaths = dirpaths
            photos[dirpath].filepaths = filepaths
            for filepath in filepaths:
                photos[filepath] = photo_data()    
    logger.info("Done extracting tree for {0}".format(top))

def stat_file(filepath):
    if os.path.isfile(filepath):
        try:
            file_stat = os.stat(filepath)
        except:
            logger.error("Can't stat file at {0}".format(filepath))
            raise
        size = file_stat.st_size
        mtime = file_stat.st_mtime
        return(size, mtime)

def populate_file_stats(photos, path = None):
    '''Populate file size and mtime for each photo that is a file
    '''
    logger.info('Populating file stats for {0}'.format(path))
    if path is None:
        path = photos.path
    for filepath in photos.photo.keys():  
        [photos[filepath].size, photos[filepath].mtime] = stat_file(filepath)
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
    
##    def refresh(self):
##        We use the InArchive flag to indicate files that are in the archive, and clear tag info to indicate that that need updating.  Checksums and 
##        sizes are summed up across directories for all directories every time.
##
##        Check if in same host and node readable
##        Clear inArchive flag for all nodes in archive
##        Traverse filesystem
##            If directory not in archive
##            if (filesize or mtime changed)
##                if is a photo file:
##                    compute thumbnail checksum and file parameters
##                    set validated flag
##                else
##                    compute file checksum and use for both thumbnail and file sums
##                    set validated flag
##        Check each database record
##            if not validated, remove from database
##        '''
###        for element in self.photo.keys():
###            self.photo[element].inArchive = False
###        self.traverse(self.path, refresher)
###        
#def refresh_collection(photos):  #TODO Working on this one!  Still incomplete
#    logger.info("Refreshing tree for {0}".format(photos.path))
#    
#    if photos.host != socket.gethostname() or not os.path.exists(photos.path):
#        err_str = "Error:  Can only refresh archive on host machine"
#        print err_str
#        logger.error(err_str)
#        sys.exit(1)  #TODO might be nice to raise an error instead...think about it.
#    
#    #Clear inArchive - used as flag for existing nodes    
#    for node in photos.photo.keys():
#        photos[node].inArchive = False
#        
#    update_tree(photos)
#        
#        
#def update_tree(photos):
#    def walkError(walkErr):
#        err_str = "Error {0} {1}".format(walkErr.errno, walkErr.strerror)
#        print err_str
#        logger.error(err_str)
#        sys.exit(1)  #TODO might be nice to raise an error instead...think about it.
#    
#    if os.path.isfile(photos.path):  #Handle the case of a file as os.walk does not handle files
#        err_str = "Error: Stubbed function - photo album is a file"
#        print err_str
#        logger.error(err_str)
#    else:
#        for dirpath, dirnames, filenames in os.walk(photos.path, onerror = walkError):
#            dirpaths = [os.path.join(dirpath, dirname) for dirname in dirnames]
#            filepaths = [os.path.join(dirpath, filename) for filename in filenames]
#            if not dirpath in photos.photo:
#                logger.info("New: {0}".format(dirpath))
#                photos[dirpath] = photo_data()
#                photos[dirpath].isdir = True
#                photos[dirpath].dirpaths = dirpaths
#                photos[dirpath].filepaths = filepaths
#                photos[dirpath].inArchive = True
#                for filepath in filepaths:
#                    logger.info("New: {0}".format(filepath))
#                    photos[filepath] = photo_data()    #Need to extract tag data for all of these!clear gotTags
#            else:
#                photos[dirpath].isdir = True
#                photos[dirpath].dirpaths = dirpaths
#                photos[dirpath].filepaths = filepaths
#                photos[dirpath].inArchive = True
#                for filepath in filepaths:
#                    [size, mtime] = stat_file(filepath)
#                    if size != photos[filepath].size or mtime != photos[filepath].mtime:
#                        logger.info("Changed: {0} - Old/New size: {1}/{2} - Old/New mtime: {3}/{4}".format(filepath, photos[filepath].size, size, photos[filepath].mtime, mtime))
#                        photos[filepath].size = size
#                        photos[filepath].mtime = mtime
#                        photos[filepath].inArchive = True
#        self.isdir = False
#        self.size = 0
#        self.mtime = -(sys.maxint - 1) #Set default time to very old
#        self.timestamp = datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
#        self.gotTags = False
#        self.signature = ''
#        self.fileMD5 = ''
#        self.userTags = ''
#        self.inArchive = False
#        self.signature_match = []
#        self.signature_and_tags_match = []
#        self.dirpaths = []
#        self.filepaths = []  
#                    photos[filepath] = photo_data() 
#                
#    logger.info("Done refreshing tree for {0}".format(photos.path))
#     
    

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
            photos[photo_file].signature = get_file_signature(photos, tags, photo_file)  #Get signature should now do the thumbnailMD5 and if not find another signature.:w
            photos.datasetChanged = True 
            photos[photo_file].gotTags = True
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

