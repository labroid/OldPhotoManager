'''
Created on Oct 21, 2011

@author: scott_jackson

TODO:  Finish converting this to make the archive pickle only contain photo_collection
The pickle that holds photo data should only contain the photo data and not processing 
methods except to the extent necessary to create the data object.  If methods are needed,
then they should only call the standard libraries, such that no dependencies exist
for methods outside of this object.

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
        self.candidates = []
        self.dirpaths = []
        self.filepaths = []      
        
class photo_collection:  #This class should be data only
    
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
#    photo_functions.populate_tree_sizes(self)
#    logger.info("Computing cumulative signature for file tree")  #Logged here since function is recursive
#    photo_functions.populate_tree_signatures(self)
    return(photos)
    


def populate_tree(photos, top = None):
    '''Descend photo tree from top and add photo instances for each node and leaf of the tree
    '''
        
    def walkError(walkErr):
        print "Error", walkErr.errno, walkErr.strerror
        raise
    
    logger.info("Populating tree for {0}".format(top))
    if top is None:
        top = photos.path
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



def populate_file_stats(photos, path = None):
    '''Populate file size and mtime for each photo that is a file
    '''
    logger.info('Populating file stats for {0}'.format(path))
    if path is None:
        path = photos.path
    for filepath in photos.photo.keys():  
        if os.path.isfile(filepath):          
            try:
                file_stat = os.stat(filepath)
            except:
                photos[filepath].size = -1
                photos[filepath].mtime = -1
                logger.error("Can't stat file at {0}".format(filepath))
                raise
            photos[filepath].size = file_stat.st_size
            photos[filepath].mtime = file_stat.st_mtime
    logger.info('Done populating file stats for {0}'.format(photos.path))

def get_file_signature(photos, tags, filepath):
    LENGTH_LIMIT = 1048576  #Max length for a non-PHOTO_FILES, otherwise a truncated MD5 is computed
    if tags is not None and len(tags.previews) > 0:
            signature = photo_functions.thumbnailMD5sum(tags)
    else:
        if photos[filepath].size < LENGTH_LIMIT:
            signature = MD5sums.fileMD5sum(filepath)
        else:
            signature = MD5sums.truncatedMD5sum(filepath, LENGTH_LIMIT)
    return(signature)
    

#    def refresh(self):
#        ''' TODO Rescan photo photo and update pickle but only if hostname is same and root node exists
#            Update node database
#        Check if in same host
#        Clear validated flag
#        Traverse filesystem (computing node values depth first as done normally, setting validated flags)
#            if (filesize or mtime changed)
#                if is a photo file:
#                    compute thumbnail checksum and file parameters
#                    set validated flag
#                else
#                    compute file checksum and use for both thumbnail and file sums
#                    set validated flag
#        Check each database record
#            if not validated, remove from database
#        '''
##        for element in self.photo.keys():
##            self.photo[element].inArchive = False
##        self.traverse(self.path, refresher)
##        
##    def refresher(self, root, dirs, files):
#        pass            

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
            logger.info("{0} of {1} = {2:.2f}%, {3:.1f} seconds, time remaining: {4}".format(file_count, total_files, 1.0 * file_count / total_files * 100.0, elapsed_time, str(datetime.timedelta(elapsed_time / file_count * float(total_files - file_count)))))
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

