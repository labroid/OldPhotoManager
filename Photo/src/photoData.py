'''
Created on Oct 21, 2011

@author: scott_jackson

'''
import sys
import os
import stopwatch
import time
import datetime
import logging
from pickle_manager import photo_pickler
from fileMD5sum import fileMD5sum, stringMD5sum
from photo_functions import getTagsFromFile, getTimestampFromTags,\
    thumbnailMD5sum, getTagsFromFile, getUserTagsFromTags
from photo_utils import print_now
from stopwatch import stopWatch

class photoUnitData():
    def __init__(self):
        self.size=0
        self.mtime = -(sys.maxint - 1) #Set default time to very old
        self.timestamp = datetime.datetime.strptime('1700:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
        self.gotTags = False  #Get rid of this; thumbnailMD5 is sufficient
        self.thumbnailMD5 = ''
        self.dirflag = False
        self.fileMD5 = ''
        self.userTags = ''
        self.inArchive = False
        self.candidates = []
        self.degenerateParent = False
            
class photoData:  #Create a function to update pickle, and an option to auto update on changes (maybe in get tags?)
    def __init__(self, path, pickle):
        self.data = dict()
        self.path = path
        self.pickle = pickle
        self.datasetChanged = False  #Make this an internal variable?
        
        logger = logging.getLogger(__name__)
        logger.info("Traversing {0}".format(self.path))
        timer = stopwatch.stopWatch()
        timer.start()
        self.traverse(self.path, self.sumFileDirSize)
        logger.info("Done. Total files: {0}, Elapsed time: {1:.2} seconds or {2} ms/file".format(len(self.data), timer.read(), timer.read()/len(self.data)))
            
    def _walkError(self,walkErr):
        global _walkErrorFlag
        print "Error",walkErr.errno, walkErr.strerror
        raise
        return()
                
    def traverse(self, root_in, sumFunction):
        if os.path.isfile(root_in):  #For the case when the root_in is just a file and not a directory
            sumFunction("", [], [root_in])
        else:
            for root, dirs, files in os.walk(root_in, topdown=False, onerror = self._walkError):
                sumFunction(root, dirs, files)
            
    def sumFileDirSize(self, root, dirs, files):
        logger = logging.getLogger(__name__)
        if root not in self.data:
            total_size = 0
            for filebase in files:
                filename = os.path.join(root, filebase)
                self.data[filename] = photoUnitData()
                self.data[filename].dirflag = False
                try:
                    self.data[filename].size = os.path.getsize(filename)
                except:
                    self.data[filename].size = -1
                    logger.error("Can't determine size of {0}".format(filename))
                    break
                total_size += self.data[filename].size
                try:
                    self.data[filename].mtime = os.path.getmtime(filename)
                except:
                    self.data[filename].mtime = -1
                    logger.error("Can't determine mtime of {}".format(filename))
                    break
        for directory in dirs:
            dirname = os.path.join(root, directory)
            total_size += self.data[dirname].size
        if root != '':  #When root is a directory and not just a simple root
            self.data[root] = photoUnitData()
            self.data[root].size = total_size
            self.data[root].dirflag = True
            if len(dirs) == 1 and len(files) == 0:
                self.data[root].degenerateParent = True
            else:
                self.data[root].degenerateParent = False
            
    def getFileMD5(self, filename):
        if self.data[filename].fileMD5 == '':
            self.data[filename].fileMD5 = fileMD5sum(filename)
            self.datasetChanged = True
        return(self.data[filename].fileMD5)
        
    def listZeroLengthFiles(self):
        zeroLengthNames = []
        for target in self.data.keys():
            if self.data[target].size == 0:
                zeroLengthNames.append(target)
        return(zeroLengthNames)
    
    def refresh(self):
        ''' TODO Rescan photo data and update pickle but only if hostname is same and root node exists
        '''
        pass
    
    def dump_pickle(self):
        '''TODO refresh pickle file, usually called if changes were made in photodata.
        '''
        pass
            
    def extract_tags(self, filelist = []):
        PHOTO_FILES = [".jpg", ".png"]  #Use lower case as comparisons are all cast to lower case
        timer = stopwatch.stopWatch()
        timer.start()
        logger = logging.getLogger(__name__)
        if len(filelist) == 0:
            filelist = self.data.keys()
        total_files = len(filelist)
        logger.info("Extracting tags.  File count = {0}".format(total_files))
        file_count = 0
        for photo_file in filelist:
            file_count += 1
            if file_count % 500 == 0:
                logger.info("{0} of {1} = {2:.2}%, {3:.1} seconds, {4} remaining seconds".format(
                            file_count, total_files, file_count/total_files * 100.0, timer.read(), 
                            timer.read() * (1.0 - file_count/total_files)))
            if not self.data[photo_file].dirflag and not self.data[photo_file].gotTags:
                if str.lower(os.path.splitext(photo_file)[1]) in PHOTO_FILES:
                    tags = getTagsFromFile(photo_file)
                    if (tags == None):
                        self.data[photo_file].gotTags = True
                        logger.warn("Bad tags in: {0}".format(photo_file))
                    else:
                        self.datasetChanged = True
                        tags = getTagsFromFile(photo_file)
                        self.data[photo_file].thumbnailMD5 = thumbnailMD5sum(tags)
                        self.data[photo_file].userTags = getUserTagsFromTags(tags)
                        self.data[photo_file].timestamp = getTimestampFromTags(tags)
                        self.data[photo_file].gotTags = True
                else:  #Consider using length instead of MD5 on .mov files?
                    self.data[photo_file].thumbnailMD5 = self.getFileMD5(photo_file)
                    self.data[photo_file].gotTags = True
                    self.datasetChanged = True
        elapsed_time = timer.read()
        logger.info("Tags extracted.  Elapsed time: {0:.0} seconds or {1:.0} ms per file = {2:.0} for 100k files".format(elapsed_time, elapsed_time/file_count * 1000, elapsed_time/file_count * 100000/60))
        if self.pickle != None:
            self.pickle.dumpPickle(self)
        return(self.datasetChanged)
    
    def node_statistics(self):
        logger = logging.getLogger(__name__)
        timer = stopwatch.stopWatch()
        logger.info("Counting node types in " + self.path)
        dircount = 0
        filecount = 0
        timer.start()
        extensions = set()
        for archive_file in self.data.keys():
            extensions.add(str.lower(os.path.splitext(archive_file)[1]))
            if self.data[archive_file].dirflag:
                dircount += 1
            else:
                filecount += 1
        
        logger.info("Elapsed time: " + str(timer.read()) + " or " + str(timer.read() / (filecount + dircount) * 1000000.0) + " us/file")
        logger.info("Dircount: " + str(dircount) + " Filecount " + str(filecount) + " Total " + str(dircount + filecount))
        logger.info("File extensions in archive:" + str(extensions))
        return filecount, dircount
    
def get_photo_data(node_path, pickle_path, node_update = True):
    ''' Create instance of photo data given one of three cases:
    1.  Supply only node_path:  Create photo data instance
    2.  Supply only pickle_path:  load pickle.  Abort if pickle empty.
    3.  Supply both node_path and pickle_path:  Try to load pickle. 
                                                If exists:
                                                    load pickle
                                                    update pickle unless asked not to
                                                else:
                                                    create photo data instance and create pickle
    
    all other cases are errors
    '''
    logger = logging.getLogger(__name__)
    if node_path is not None and pickle_path is None:
        logger.info("Creating photoUnitData instance")
        node = photoUnitData(node_path)
    elif node_path is None and pickle_path is not None:
        logger.info("Unpacking pickle")
        pickle = photo_pickler(pickle_path)
        node = pickle.loadPickle()
    elif node_path is not None and pickle_path is not None:
        pickle= photo_pickler(pickle_path)
        if pickle.pickleExists:
            logger.info("Loading pickle")
            node = pickle.loadPickle()
            if node_update:
                logger.info("Refreshing photo data in pickle")
                node.refresh()          
        else:
            logger.info("Scanning node")
            node = photoData(node_path)
            node.pickle = pickle
            node.dump_pickle()
    else:
        logger.critical("function called with arguments:\"{0}\" and \"{1}\"".format(node_path, pickle_path))
        sys.exit(1)
        return(node)
        
