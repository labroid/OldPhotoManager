'''
This class and set of functions extract data from a collection of PhotoData.  
It also contains helper functions to create pickles, provide high-level
statistics, and print extracted data. The test main at the bottom can print
 the content of the pickle as a diagnostic
 
 Call with unicode path to get unicode traversal of the filesystem 

Created on Oct 21, 2011

@author: scott_jackson

'''
#pylint: disable=line-too-long

import sys
import os
import time
import datetime
import logging
import pyexiv2
import socket
import MD5sums
import pickle_manager

class NodeInfo(object):
    def __init__(self):
        self.isdir = False
        self.size = 0
        self.md5 = ''
        self.signature = ''
        self.dirpaths = []
        self.filepaths = []      
        self.mtime = -(sys.maxint - 1) #Set default time to very old
        self.timestamp = datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
        self.got_tags = False
        self.user_tags = ''
        self.in_archive = False 

class PhotoData(object):
    def __init__(self, path):
        logging.info("Creating photo data instance...")
        self.host = socket.gethostname()
        if type(path) != 'unicode':
            logging.warning('Path passed to photodata is not unicode; may lead to inconsistent operation')
        self.path = os.path.normpath(path)
        self.node = dict()
        self.pickle = None
        self.dataset_changed = False
        self.update_collection()
        return
        
    def __getitem__(self, key):
        return self.node[key]
    
    def __setitem__(self, key, value):
        self.node[key] = value 
        
    def update_collection(self):
        start_time = time.time()
        if self.host != socket.gethostname():
            logging.warning('Collection not on this machine; data set will not be updated')
            return
        logging.info("Updating photo collection at {0}:{1}".format(self.host, repr(self.path)))
        #Clear in_archive flag.  Flag will be set while crawling tree if file still there
        for path in self.node.iterkeys():
            self.node[path].in_archive = False
        self._update_tree()
        self._prune_old_nodes()
        self._populate_md5sums()
        self._extract_populate_tags()
        elapsed_time = time.time() - start_time
        logging.info("Total nodes: {0}, Elapsed time: {1:.2} seconds or {2} ms/file".format(len(self.node), elapsed_time, elapsed_time/len(self.node)))
        return
    
    def _clear_archive_tree(self, path):
        '''
        Clear the in_archive flag for the tree rooted at path.  _update_tree will set flag for files that are still in that tree
        '''
        if path in self.node:
            for dirpath in self.node[path].dirpaths:
                self._clear_archive(dirpath)
                self.node[dirpath].in_archive = False
            for filepath in self.node[path].filepaths:
                self.node[filepath].in_archive = False
            self.node[path].in_archive = False
        
    def _update_tree(self):
        '''Descend photo tree and updates/adds/deletes node instances for each node and leaf of the tree.
           If leaves exist, only update them if the mtime or size have changed.
        '''
        def _walk_error(walk_err):
            print "Error {}:{}".format(walk_err.errno, walk_err.strerror)  #TODO Maybe some better error trapping here...and do we need to encode strerror if it might contain unicode??
            raise
            
        logging.info("Updating tree...")
        if os.path.isfile(self.path):
            self[self.path].isDir = False
            self[self.path].in_archive = True  #TODO Check that this is all that needs to be done...
        else:
            self._clear_archive_tree(self.path)
            for dirpath, dirnames, filenames in os.walk(self.path, onerror = _walk_error):
                dirpaths = [os.path.normpath(os.path.join(dirpath, dirname)) for dirname in dirnames]
                filepaths = [os.path.normpath(os.path.join(dirpath, filename)) for filename in filenames]
#                filepaths = [filename.decode(sys.getfilesystemencoding()) for filepath in filepaths]
                if not dirpath in self.node:
                    logging.debug("New directory detected: {0}".format(repr(dirpath)))
                    self.node[dirpath] = NodeInfo()
                    if "142_0908" in dirpath:
                        pass
                self[dirpath].size = -1
                self[dirpath].isdir = True
                self[dirpath].signature = ''
                self[dirpath].dirpaths = dirpaths
                self[dirpath].filepaths = filepaths
                self[dirpath].in_archive = True
                
                for filepath in filepaths:
                    file_stat = self._stat_node(filepath)
                    if filepath not in self.node:
                        #print repr(filepath)
                        logging.debug("New File detected: {0}".format(repr(filepath)))
                        self.node[filepath] = NodeInfo()
                    if self[filepath].size != file_stat.st_size or self[filepath].mtime != file_stat.st_mtime:
                        self[filepath].size = file_stat.st_size
                        self[filepath].mtime = file_stat.st_mtime
                        self[filepath].got_tags = False
                    self[filepath].in_archive = True
        logging.info("Done extracting tree.")
        return
    
    def _stat_node(self, nodepath):
        try:
            file_stat = os.stat(nodepath)
        except:
            logging.error("Can't stat file at {0}".format(repr(nodepath)))
            sys.exit(1)
        return(file_stat)    
                
    def _prune_old_nodes(self):  #TODO:  Should be a better way to handle if file is present - right now the data structure carries datasetchanged, gottags, inarchive
        '''Prune nodes from the photo collection that are no longer present.
           Also reset the in_archive flag to False
        '''
        logging.info("Pruning old nodes from {0}".format(self.path))
        for filepath in self.node:
            if "142_0908" in filepath:
                pass
            if not self[filepath].in_archive:
                logging.debug("Pruning {0}".format(repr(filepath)))
                del self.node[filepath]
        return

    def monitor_progress(self, total_files, start_time, file_count):
        PROGRESS_COUNT = 500 #How often to report progress in units of files
        if not file_count % PROGRESS_COUNT:
            elapsed_time = time.time() - start_time
            total_time_projected = float(elapsed_time) / float(file_count) * total_files
            time_remaining = float(elapsed_time) / float(file_count) * float(total_files - file_count)
            logging.info("{0} of {1} = {2:.2f}%, {3:.1f} seconds, time remaining: {4} of {5}".format(file_count, total_files, 1.0 * file_count / total_files * 100.0, elapsed_time, str(datetime.timedelta(seconds=time_remaining)), str(datetime.timedelta(seconds=total_time_projected))))
        return 
    
    def _populate_md5sums(self):
        logging.info("Computing MD5 sums")
        total_files = str(len(self.node))
        for count, path in enumerate(self.node.iterkeys(), start = 1):
            if not self.node[path].isdir:
                self.node[path].md5 = MD5sums.fileMD5sum(path)
                if not count % 100:
                    logging.info("MD5 Sums:  {} of {} complete".format(count, total_files))

    def _extract_populate_tags(self):
        PHOTO_FILES = [".jpg", ".png"]  #Use lower case as extensions will be cast to lower case for comparison.  TODO: make this configurable
        filelist = [x for x in self.node.keys() if not self.node[x].isdir and not self.node[x].got_tags]  #TODO do we need keys() here?  I think it would be happy with default generator instance.  Check once testing is fixed.
        total_files = len(filelist)
        logging.info("Extracting tags for {0}.  File count = {1}".format(repr(self.path), total_files))
        start_time = time.time()
        tag_extract_count = 0
        for file_count, photo_file in enumerate(filelist, start = 1):
            self.monitor_progress(total_files, start_time, file_count)
            tag_extract_count += 1
            ext = os.path.splitext(photo_file)[1].encode().lower()
            if ext in PHOTO_FILES:
                tags = self._get_tags_from_file(photo_file)
                if tags is None:
                    logging.warn("Bad tags in: {0}".format(repr(photo_file)))
                else:
                    self[photo_file].user_tags = self._get_user_tags_from_tags(tags)
                    self[photo_file].timestamp = self._get_timestamp_from_tags(tags)  
            else:
                tags = None   
            self[photo_file].signature = self._get_file_signature(tags, photo_file)  #Get signature should now do the thumbnailMD5 and if not find another signature.
            self[photo_file].got_tags = True
            self.dataset_changed = True 
        elapsed_time = time.time() - start_time
        logging.info("Tags extracted.  {0} Updated. Elapsed time: {1:.0g} seconds or {2:.0g} ms per file = {3:.0g} for 100k files".format(tag_extract_count, elapsed_time, elapsed_time/total_files * 1000.0, elapsed_time/total_files * 100000.0/60.0))
        return(self.dataset_changed)
        
    def _get_file_signature(self, tags, filepath):
    #    LENGTH_LIMIT = 1048576  #Max length for a non-PHOTO_FILES, otherwise a truncated MD5 is computed
        TEXT_FILES = ['.ini', '.txt']  #file types to be compared ignoring CR/LF for OS portability.  Use lower case (extensions will be lowered before comparison)
        if tags is not None and len(tags.previews) > 0:
            signature = self._thumbnail_MD5_sum(tags)
        else:
            if str.lower(os.path.splitext(filepath)[1].encode()) in TEXT_FILES:
                signature = MD5sums.text_file_MD5_signature(filepath)
            else:
    #            if PhotoData[filepath].size < LENGTH_LIMIT:
    #                signature = MD5sums.fileMD5sum(filepath)
    #            else:
    #                signature = MD5sums.truncatedMD5sum(filepath, LENGTH_LIMIT)
                signature = self[filepath].md5
        return(signature)

    def _get_timestamp_from_tags(self,tags):
        if 'Exif.Photo.DateTimeOriginal' in tags.exif_keys:
            timestamp = tags['Exif.Photo.DateTimeOriginal'].value
        else:
            timestamp = datetime.datetime.strptime('1800:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
        return(timestamp)

    def _get_user_tags_from_tags(self, tags):
        if 'Xmp.dc.subject' in tags.xmp_keys:
            return(tags['Xmp.dc.subject'].value)
        else:
            return(None)

    def _get_tags_from_file(self, filename):
        try:
            filepath = filename.decode(sys.getfilesystemencoding())
            metadata = pyexiv2.ImageMetadata(filepath)
            metadata.read()
        except IOError as err:  #The file contains photo of an unknown image type or file missing or can't be opened
            logging.warning("%s IOError, errno = %s, strerror = %s args = %s", repr(filename), str(err.errno), err.strerror, err.args)
            return(None)
        except:
            logging.error("%s Unknown Error Trapped", repr(filename))
            return(None)
        return(metadata)
    
    def _thumbnail_MD5_sum(self, tags):
        if len(tags.previews) > 0:
            temp = MD5sums.stringMD5sum(tags.previews[0].data)
        else:
            temp = MD5sums.stringMD5sum("0")
        return(temp)
    
    def list_zero_length_files(self):
        zero_length_names = []
        for target in self.node:  #Got rid of keys() in case this breaks....  TODO need a test here
            if self[target].size == 0:
                zero_length_names.append(target)
        return(zero_length_names)
    
    def get_statistics(self):
        class statistics:
            def __init__(self):
                self.dircount = 0
                self.filecount = 0
                self.unique_count = 0
                self.dup_count = 0
                self.dup_fraction = 0
        stats = statistics()
        
        photo_set = set()
        for archive_file in self.node:  #Got rid of keys() in case this breaks...  TODO need a test here
            if self[archive_file].isdir:
                stats.dircount += 1
            else:
                stats.filecount += 1
            sig = self[archive_file].signature
            if sig in photo_set:
                stats.dup_count += 1
            else:
                photo_set.add(sig)
                
        stats.unique_count = len(photo_set)
        stats.dup_fraction = stats.dup_count * 1.0 / (stats.filecount + stats.dup_count)
        logging.info("Collection statistics:  Directories = {0}, Files = {1}, Unique signatures = {2}, Duplicates = {3}, Duplicate Fraction = {4:.2%}".format(
            stats.dircount, stats.filecount, stats.unique_count, stats.dup_count, stats.dup_fraction))    
        return(stats)
            
    def print_statistics(self):
        result = self.get_statistics()
        print "Directories: {0}, Files: {1}, Unique PhotoData: {2}, Duplicates: {3} ({4:.2%})".format(result.dircount, result.filecount, result.unique_count, result.dup_count, result.dup_fraction)
        return

    def print_zero_length_files(self):
        zero_files = self.list_zero_length_files()
        if len(zero_files) == 0:
            print "No zero-length files."
        else:
            print "Zero-length files:"
            for names in zero_files:
                print repr(names)
        return
    
    def print_flat(self, top = None):
        for path in self.node.iterkeys():
            s = self.node[path]
            print repr(path), s.isdir, s.size, s.md5, s.signature, s.dirpaths, s.filepaths, s.mtime, s.timestamp, s.got_tags, s.user_tags, s.in_archive
            print s.__dict__
#            json.dumps(s.__dict__)

#            print "{},{}".format(repr(path), s.md5, s.signature, s.size, s.user_tags )
        
    def print_tree(self, top = None, indent_level = 0, first_call = True):
        '''Print Photo collection using a tree structure'''
        if top is None:
            top = self.path
        if first_call:
            print "Photo Collection at {0}:{1} pickled at {2}".format(self.host, repr(self.path), self.pickle)    
        self._print_tree_line(top, indent_level)
        indent_level += 1
        for filepath in self[top].filepaths:
            self._print_tree_line(filepath, indent_level)
        for dirpath in self[top].dirpaths:
            self.print_tree(dirpath, indent_level, False)
        return
    
    def _print_tree_line(self, path, indent_level):
        INDENT_WIDTH = 3 #Number of spaces for each indent level
        if self[path].isdir:
            print "{0}{1} {2} {3} {4}".format(" " * INDENT_WIDTH * indent_level, repr(path), self[path].size, self[path].md5, self[path].signature)
        else:
            print "{0}{1} {2} {3} {4} {5} {6}".format(" " * INDENT_WIDTH * indent_level, repr(path), self[path].size, self[path].md5, self[path].signature, self[path].user_tags, self[path].timestamp)
            
    def emit_records(self):
        #emit collection information here, maybe under a path called "root" or something similar so it can be found
        for photo in self.node.iterkeys():
            record = self.node[photo].__dict__
            record.update({"node":photo})
            yield record
                
def get_photo_data(node_path, pickle_path, node_update = True):
    ''' Create instance of photo photo given one of three cases:
    1.  Supply only node_path:  Create photos instance
    2.  Supply only pickle_path:  load pickle.  Abort if pickle empty or other pickle problems.
    3.  Supply both node_path and pickle_path:  Try to load pickle. 
            If exists:
                load pickle
                update pickle unless asked not to
            else:
                create photos instance, update, and create pickle
    all other cases are errors
    '''
    if not node_path and not pickle_path: #Both paths undefined (None or empty string)
        logging.critical("Function called with no arguments.  Aborting.")
        sys.exit(1)
    elif node_path and not pickle_path: #Only node path is given
        logging.info("Creating PhotoData instance for {0}".format(repr(node_path)))
        return(PhotoData(node_path))
    elif not node_path and pickle_path:  #Only pickle is given
        logging.info("Unpacking pickle at {0}".format(repr(pickle_path)))
        pickle = pickle_manager.PhotoPickler(pickle_path)
        return(pickle.load_pickle())
    else:  #Both paths are defined
        pickle = pickle_manager.PhotoPickler(pickle_path)
        if pickle.pickle_exists:
            logging.info("Loading pickle at {0} for {1}".format(repr(pickle.pickle_path), repr(node_path)))
            photos = pickle.load_pickle()
            if node_update:
                photos.update_collection()
                pickle.dump_pickle(photos)
        else:
            logging.info("Scanning photos {0}; will pickle to {1}".format(repr(node_path), repr(pickle.pickle_path)))
            photos = PhotoData(node_path)
            photos.pickle = pickle_path
            pickle.dump_pickle(photos)
        return(photos) 
    
#Stole this main from Guido van van Rossum at http://www.artima.com/weblogs/viewpost.jsp?thread=4829
#class Usage(Exception):
#    def __init__(self, msg):
#        self.msg = '''Usage:  get_photo_data [-p pickle_path] [-l log_file] photos_path'''
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
    import pymongo
#    photo_dir = "C:/Users/scott_jackson/git/PhotoManager/Photo/tests/test_photos"
#    pickle_file = "C:/Users/scott_jackson/git/PhotoManager/Photo/tests/test_photos_pickle"
    photo_dir = "C:/Users/scott_jackson/Pictures/Uploads"
    log_file = "C:\Users\scott_jackson\Documents\Personal\Programming\lap_log.txt"
    
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(filename = log_file, format = LOG_FORMAT, level = logging.DEBUG, filemode = 'w')
    
    photos = get_photo_data(unicode(photo_dir), None)
#    photos.print_tree()
#    photos.print_flat()
#    print "Putting it in Mongodb"
    collection = pymongo.MongoClient().phototest.photo_archive
    for rec in photos.emit_records():
        print collection.insert(rec)

    print "Done!"

if __name__ == "__main__":
    sys.exit(main())

