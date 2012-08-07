'''
Created on Oct 21, 2011

@author: scott_jackson

'''
import sys
import os
import stopwatch
import datetime
import logging
import pyexiv2
from fileMD5sum import fileMD5sum, truncatedMD5sum
import photo_functions

class photoUnitData:  #Put this class def inside photoData?
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
        
class photoData:  #Create a function to update pickle, and an option to auto update on changes (maybe in get tags?)
    def __init__(self, path):
        self.host = ''
        self.path = path
        self.data = dict()
        self.pickle = None
        self.datasetChanged = False
        logger = logging.getLogger()
        
        timer = stopwatch.stopWatch()
        timer.start()
        
        self.extract_tree(self.path)
        self.populate_file_stats()
        self.extract_populate_tags()
        logger.info("Total files: {0}, Elapsed time: {1:.2} seconds or {2} ms/file".format(len(self.data), timer.read(), timer.read()/len(self.data)))
        logger.info("Computing cumulative sizes for file tree")
        photo_functions.populate_tree_sizes(self)
        logger.info("Computing cumulative signature for file tree")
        photo_functions.populate_tree_signatures(self)
        
    def extract_tree(self, top):
        logger = logging.getLogger()
        logger.info("Extracting tree for {0}".format(top))
        if os.path.isfile(top):  #Handle the case of a file as os.walk does not handle files
            self.data[top] = photoUnitData()
        else:
            for dirpath, dirnames, filenames in os.walk(top, onerror = self._walkError):
                dirpaths = [os.path.join(dirpath, dirname) for dirname in dirnames]
                filepaths = [os.path.join(dirpath, filename) for filename in filenames]
                self.data[dirpath] = photoUnitData()
                self.data[dirpath].isdir = True
                self.data[dirpath].dirpaths = dirpaths
                self.data[dirpath].filepaths = filepaths
                for filepath in filepaths:
                    self.data[filepath] = photoUnitData()    
        logger.info("Done extracting tree for {0}".format(top))

    def _walkError(self, walkErr):
        global _walkErrorFlag
        print "Error", walkErr.errno, walkErr.strerror
        raise
                                                
    def populate_file_stats(self):
        logger = logging.getLogger()
        logger.info('Populating file stats for {0}'.format(self.path))
        for filepath in self.data.keys():  
            if os.path.isfile(filepath):          
                try:
                    file_stat = os.stat(filepath)
                except:
                    self.data[filepath].size = -1
                    self.data[filepath].mtime = -1
                    logger.error("Can't stat file at {0}".format(filepath))
                    raise
                self.data[filepath].size = file_stat.st_size
                self.data[filepath].mtime = file_stat.st_mtime
        logger.info('Done populating file stats for {0}'.format(self.path))

    def get_file_signature(self, filename):
        LENGTH_LIMIT = 1048576  #Max length for a non-PHOTO_FILES, otherwise a truncated MD5 is computed
        if self.data[filename].fileMD5 == '':
            if self.data[filename].size < LENGTH_LIMIT:
                signature = fileMD5sum(filename)
            else:
                signature = truncatedMD5sum(filename, LENGTH_LIMIT)
        return(signature)
        

#    def refresh(self):
#        ''' TODO Rescan photo data and update pickle but only if hostname is same and root node exists
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
##        for element in self.data.keys():
##            self.data[element].inArchive = False
##        self.traverse(self.path, refresher)
##        
##    def refresher(self, root, dirs, files):
#        pass            

    def extract_populate_tags(self, filelist = []):
        logger = logging.getLogger()
        PHOTO_FILES = [".jpg", ".png"]  #Use lower case as extensions will be cast to lower case for comparison
        timer = stopwatch.stopWatch() #Also starts watch
        if len(filelist) == 0:
            filelist = self.data.keys()
        total_files = len(filelist)
        logger.info("Extracting tags for {0}.  File count = {1}".format(self.path, total_files))
        file_count = 0
        for photo_file in filelist:
            file_count += 1
            if file_count % 500 == 0:
                logger.info("{0} of {1} = {2:.2}%, {3:.1f} seconds, {4:.2f} remaining seconds".format(
                            file_count, total_files, 1.0 * file_count / total_files * 100.0, timer.read(), 
                            timer.read() / file_count * (total_files - file_count)))
            if not self.data[photo_file].isdir and not self.data[photo_file].gotTags:
                if str.lower(os.path.splitext(photo_file)[1]) in PHOTO_FILES:
                    tags = self.getTagsFromFile(photo_file)
                    if (tags == None):
                        self.data[photo_file].gotTags = True
                        logger.warn("Bad tags in: {0}".format(photo_file))
                    else:
                        self.data[photo_file].signature = photo_functions.thumbnailMD5sum(tags)
                        self.data[photo_file].userTags = photo_functions.getUserTagsFromTags(tags)
                        self.data[photo_file].timestamp = photo_functions.getTimestampFromTags(tags)
                        self.data[photo_file].gotTags = True                        
                        self.datasetChanged = True
                else:
                    self.data[photo_file].signature = self.get_file_signature(photo_file)
                    self.data[photo_file].gotTags = True
                    self.datasetChanged = True
        elapsed_time = timer.read()
        logger.info("Tags extracted.  Elapsed time: {0:.0} seconds or {1:.0} ms per file = {2:.0} for 100k files".format(elapsed_time, elapsed_time/file_count * 1000, elapsed_time/file_count * 100000/60))
        return(self.datasetChanged)
    
    def getTagsFromFile(self,filename):
        logger = logging.getLogger()
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
        except IOError as err:  #The file contains data of an unknown image type or file missing or can't be opened
            logger.warning("getTagesFromFile(): %s IOError, errno = %s, strerror = %s args = %s", filename, str(err.errno), err.strerror, err.args)
            return(None)
        except:
            logger.error("getTagesFromFile(): %s Unknown Error Trapped, errno = %s, strerror = %s args = %s", filename, str(err.errno), err.strerror, err.args)
            return(None)
        return(metadata)
    
    def dump_pickle(self):
        self.pickle.dumpPickle(self)