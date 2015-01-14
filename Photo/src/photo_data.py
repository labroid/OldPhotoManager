'''
Populates database 'db' with data extracted from a tree at 'root'
If database doesn't exist setting 'create_new' to True will create new database
Call with unicode root to get unicode traversal of the filesystem 

Created on Oct 21, 2011

@author: scott_jackson

data model for node in database (not all are present in a given node):

    'path' : '',
    'isdir' : False,
    'size' : -1, 
    'md5' : '',
    'signature' : '',
    'dirpaths' : [],
    'filepaths' : [],
    'mtime' : -(sys.maxint - 1), #Set default time to very old
    'timestamp' : datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S'),
    'got_tags' : False,
    'user_tags' : [],
    'in_archive' : False, #TODO probably not needed if refresh_time is used
    'refresh_time' : -(sys.maxint - 1), #Set default time to very old
'''
#TODO: Add logging
#TODO: Is there a need to have 'root' defined when instantiating?  Can we instantiate and then only know 'top' when 'db_sync' is called?
#pylint: disable=line-too-long

import sys
import os
import time
import datetime
import re
import logging
import pymongo
import pyexiv2
import socket
import MD5sums

COLLECTION_CONFIG = 'config'
COLLECTION_PHOTOS = 'photos'
DB_STATE_TAG = 'database_state'
FS_TRAVERSE_TIME_TAG = 'fs_traverse_time'
HOST = 'host'
DB_DIRTY = 'dirty'
DB_CLEAN = 'clean'
CONFIG_TAG = 'config'
TRAVERSE_PATH_TAG = 'traverse_path'

def main():
    host = '4DAA1001519'
    photo_dir = u"C:/Users/scott_jackson/Pictures/Uploads"
    log_file = "C:\Users\scott_jackson\Documents\Personal\Programming\lap_log.txt"

        
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(filename = log_file, format = LOG_FORMAT, level = logging.DEBUG, filemode = 'w')
    
#    db = pymongo.MongoClient().phototest2
        
    start = time.time()
    photos = PhotoDb(host, photo_dir, create_new = False)
    photos.sync_db()
    finished = time.time()-start
    print "Elapsed time:",finished
#    photos = get_photo_data(unicode(photo_dir), None)
#    photos.print_tree()
#    photos.print_flat()
#    collection = pymongo.MongoClient().phototest.photo_archive
#    collection.drop()
#    collection.insert(photos.emit_records())
#    pattern = re.compile('.*20140106 Istanbul.*')
#    dog = db.find({'path': pattern})
#    for line in dog:
#       print line
    print "Done!"
    
def set_up_db(host, create_new):
        if host is None:
            print "Error - must define host (machine name)"
            sys.exit(1)
        try:
            client = pymongo.MongoClient()
            db = client[host]
        except pymongo.errors.ConnectionFailure:
            print "***ERROR*** Database connection failed.  Make sure mongod is running."
            sys.exit(1)        
        except:
            print "Unknown problem connecting to mongodb"
            sys.exit(1)
        if COLLECTION_CONFIG in db.collection_names():
            config = db[COLLECTION_CONFIG]       
        elif create_new:
            config = db[COLLECTION_CONFIG]
            config.update({CONFIG_TAG : HOST}, {'$set' : {HOST : socket.gethostname()}}, upsert = True)
        else:
            print "Error - collection '{}' does not exist.  Maybe you meant to instantiate class with the create_new flag set?".format(COLLECTION_CONFIG)
            sys.exit(1)
        if create_new or COLLECTION_PHOTOS in db.collection_names():
            photos = db[COLLECTION_PHOTOS]
        else:
            print "Error - collection '{}' does not exist.  Maybe you meant to instantiate class with the create_new flag set?".format(COLLECTION_CONFIG)
            sys.exit(1)                 
        photos.ensure_index('path', unique = True)
        return(db, config, photos)
        
def check_host(config):
    host = socket.gethostname()
    db_host_record = config.find_one({CONFIG_TAG : HOST})
    if HOST not in db_host_record:
        print "Error - no host listed for this database.  Exiting to prevent damage."
        sys.exit(1)
    db_host = db_host_record[HOST]
    if host != db_host:
        print "Error - Host is {}, while database was built on {}.  Update do database not allowed on this host."
        sys.exit(1)    
       
class PhotoDb(object):
    def __init__(self, host = None, root = None, create_new = False):    
        if root is None:
            print "Error - must define root node"
        self.root = os.path.normpath(root)
        self.start_time = 0  #Persistent variable for method in this class because I don't know a better way
        db, self.config, self.photos = set_up_db(host, create_new)
        check_host(self.config)
        if not create_new:
            self._check_db_clean()
        
    def _check_db_clean(self):
        states = self.config.find({DB_STATE_TAG : {'$exists' : True}}).sort("_id", pymongo.DESCENDING).limit(10)
        if states.count() < 1:
            print "Error - database status not available; don't know if clean"
            #TODO: Figure out how to recover - probably regenerate whole thing
            sys.exit(1)
        state = states[0]
        if state[DB_STATE_TAG] == DB_DIRTY:
            #TODO: figure out what to do to recover - probably regenerate whole thing
            print "Error - DB is dirty.  No recovery options implemented."
            sys.exit(1)
        elif state[DB_STATE_TAG] == DB_CLEAN:
            return
        else:
            print "Error - unknown if DB clean.  Check returned state of '{}'".format(state)
            #TODO:  figure out how to recover; probably regenerate whole thing
            sys.exit(1)
            
    def sync_db(self, top = None):
        '''
        Sync contents of database with filesystem starting at 'top'
        '''        
        if top is None:
            top = self.root
        self._mark_db_status('dirty')
        self._traverse_fs(top)
        self._update_md5(top)
        self._update_tags(top)
        self._prune_db(top)
        self._mark_db_status('clean')

    def _prune_db(self, top):
        logging.info("Pruning from database nodes no longer in filesystem...")
        if top is None:
            top = self.root
        time.sleep(1) #Wait a second to make double-dog sure mongodb is caught up 
        fresh_times = self.config.find().sort(FS_TRAVERSE_TIME_TAG, pymongo.DESCENDING).limit(10)
        if fresh_times.count() < 1:
            #TODO: check latest time is associated with 'dirty' state and top paths are the same
            logging.error('ERROR: no file system fresh start time available.  Too dangerous to continue.')
            sys.exit(1)
        fresh_time = fresh_times[0]['fs_traverse_time']
        regex = self._make_tree_regex(top)
        records = self.photos.find({'path' : regex, 'refresh_time' : {'$lt' : fresh_time}})
        num_removed = records.count()
        for record in records:
            logging.info("Removed records for node: {}".format(record['path']))
        rm_stat = self.photos.remove({'path' : regex, 'refresh_time' : {'$lt' : fresh_time}})
        if num_removed != int(rm_stat['n']):
            logging.error("Error - number expected to be removed from database does not match expected number.  Database return message: {}".format(rm_stat))
        logging.info("Done pruning.  Pruned {} nodes".format(rm_stat['n']))
            
         
    def _mark_db_status(self, status):
        if status == 'dirty':
            self.config.insert({DB_STATE_TAG : 'dirty', FS_TRAVERSE_TIME_TAG : time_now(), TRAVERSE_PATH_TAG : self.root})
        elif status == 'clean':
            self.config.insert({DB_STATE_TAG : 'clean', FS_TRAVERSE_TIME_TAG : time_now(), TRAVERSE_PATH_TAG : self.root})
        else:
            logging.error("Error: bad database status received.  Got: '{}'".format(status))
    
    def _walk_error(self, walk_err):
        print "Error {}:{}".format(walk_err.errno, walk_err.strerror)  #TODO: Maybe some better error trapping here...and do we need to encode strerror if it might contain unicode??
        raise
     
    def _update_file_record(self, filepath):
        file_stat = self._stat_node(filepath)
        db_file = self.photos.find_one({'path':filepath}, {'_id':0})
        if db_file is None:
            logging.debug("New File detected: {0}".format(repr(filepath)))
            self.photos.insert(
                           {
                            'path':filepath, 
                            'isdir':False, 
                            'size':file_stat.st_size, 
                            'mtime':file_stat.st_mtime, 
                            'in_archive':True, 
                            'refresh_time':time_now()
                            }
                           )
        elif db_file['size'] != file_stat.st_size or db_file['mtime'] != file_stat.st_mtime:
            self.photos.update(
                           {'path':db_file['path']}, 
                           {
                            '$set':{
                                    'isdir':False, 
                                    'size':file_stat.st_size, 
                                    'md5':'', 
                                    'signature':'', 
                                    'mtime':file_stat.st_mtime, 
                                    'timestamp':datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S'), 
                                    'got_tags':False,
                                    'user_tags':[], 
                                    'in_archive':True, 
                                    'refresh_time':time_now()
                                    }
                            }
                           )
        else:
            self.photos.update(
                           { 'path' : db_file[ 'path' ] }, 
                           { '$set' : { 'refresh_time' : time_now() }}
                           )

    def _update_dir_record(self, dirpath, dirpaths, filepaths):
        db_dir = self.photos.find_one({'path':dirpath}, {'path':True, '_id':False})
        if db_dir is None:
            logging.debug("New directory detected: {0}".format(repr(dirpath)))
            db_dir = dirpath
        self.photos.update(
                       {'path':dirpath}, 
                       {
                        'path':dirpath, 
                        'isdir':True, 
                        'dirpaths':dirpaths, 
                        'filepaths':filepaths, 
                        'in_archive':True, 
                        'refresh_time':time_now()
                        }, 
                       upsert=True) #Replace existing record - TODO: wipe out previous sum data, if the record exists

    def _traverse_fs(self, top = None):
        if top is None:
            top = self.root
        logging.info("Traversing filesystem tree starting at {}...".format(top))
        if os.path.isfile(top):
            self._update_file_record(top)
        else:
            for dirpath, dirnames, filenames in os.walk(top, onerror = self._walk_error):
                dirpath = os.path.normpath(dirpath)
                dirpaths = [os.path.normpath(os.path.join(dirpath, dirname)) for dirname in dirnames]
                filepaths = [os.path.normpath(os.path.join(dirpath, filename)) for filename in filenames]
                self._update_dir_record(dirpath, dirpaths, filepaths)
                for filepath in filepaths:
                    self._update_file_record(filepath)
        logging.info("Done traversing filesystem tree.")
        return
            


    def _stat_node(self, nodepath):
        try:
            file_stat = os.stat(nodepath)
        except:
            logging.error("Can't stat file at {0}".format(repr(nodepath)))
            sys.exit(1)
        return(file_stat)

    def _make_tree_regex(self, top):
        '''
        Return regex that will extract tree starting from top, independent of OS
        '''
        if top.count('/') > 0: #Linux
            top_tree = '^' + top + '$|^' + top + '/.*'
            top_regex = re.compile(top_tree)
        elif top.count('\\') > 0: #Windows        
            path = re.sub(r'\\', r'\\\\', top)
            basepath = '^' + path
            children = '^' +  path + '\\\\.*' 
            pattern = unicode(basepath + '|' + children)
            top_regex = re.compile(pattern)
        else:
            print("Error:  Path to top of tree contains no path separators, can't determine OS type.  Tree top received: {}".format(top))
        return top_regex
        
    def _update_md5(self, top = None):
        logging.info("Computing missing md5 sums...")
        if top is None:
            top = self.root
        regex = self._make_tree_regex(top)
        files = self.photos.find(
                             {
                             '$and' : [
                                        {
                                         'path' : regex
                                         }, 
                                         {
                                          'isdir' : False
                                          },
                                          {
                                            '$or' : [
                                                     {
                                                      'md5' : {'$exists' : False}
                                                     },
                                                     {
                                                      'md5' : ''
                                                      }
                                                     ]
                                          }, 

                                        ]
                              },
                             {
                              '_id': False,
                              'path':True
                              },
                              timeout = False
                              )    
        logging.info("Number of files for MD5 update: {}".format(files.count()))    
        for n, path in enumerate(files, start = 1):
            logging.info('Computing MD5 {} for: {}'.format(n, repr(path['path'])))
            md5sum = MD5sums.fileMD5sum(path['path'])
            self.photos.update({'path' : path['path']}, {'$set':{'md5' : md5sum}})
        logging.info("Done computing missing md5 sums.")
                    
    def _update_tags(self, top = None):
        logging.info('Updating file tags...')
        if top is None:
            top = self.root
        tree_regex = self._make_tree_regex(top)
        files = self.photos.find(
                              {
                               'path' : tree_regex,
                               'isdir' : False, 
                               '$or' : [
                                        {'got_tags' : {'$exists' : False}},
                                        {'got_tags' : False}
                                        ]
                               },
                               {
                                '_id': False, 'path':True
                                }, 
                             timeout = False)
        total_files = files.count()
        for file_count, photo_record in enumerate(files, start = 1):
            self._monitor_tag_progress(total_files, file_count)
            photopath = photo_record['path']
            tags = self._get_tags_from_file(photopath)
            if tags is None:
                #logging.warn("Bad tags in: {0}".format(repr(photopath)))
                user_tags = ''
                timestamp = datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
            else:
                user_tags = self._get_user_tags_from_tags(tags)
                timestamp = self._get_timestamp_from_tags(tags)  
            signature = self._get_file_signature(tags, photopath)  #Get signature should now do the thumbnailMD5 and if not find another signature.
            self.photos.update(
                               {'path' : photopath}, 
                                {'$set' : 
                                 {
                                  'user_tags' : user_tags, 
                                  'timestamp' : timestamp, 
                                  'signature' : signature, 
                                  'got_tags' : True
                                 }
                                }
                               )
        logging.info('Done updating file tags.')
        return()
    
    def _monitor_tag_progress(self, total_files, file_count):
        PROGRESS_COUNT = 500 #How often to report progress in units of files
        if file_count == 1: #First call; initialize
            self.start_time = time_now()  
            logging.info("Extracting tags for {0}.  File count = {1}".format(repr(self.root), total_files))
        if not file_count % PROGRESS_COUNT:
            elapsed_time = time_now() - self.start_time
            total_time_projected = float(elapsed_time) / float(file_count) * total_files
            time_remaining = float(elapsed_time) / float(file_count) * float(total_files - file_count)
            logging.info("{0} of {1} = {2:.2f}%, {3:.1f} seconds, time remaining: {4} of {5}".format(file_count, total_files, 1.0 * file_count / total_files * 100.0, elapsed_time, str(datetime.timedelta(seconds=time_remaining)), str(datetime.timedelta(seconds=total_time_projected))))
        if file_count == total_files:
            elapsed_time = time_now() - self.start_time
            logging.info("Tags extracted: {0}.  Elapsed time: {1:.0g} seconds or {2:.0g} ms per file = {3:.0g} for 100k files".format(file_count, elapsed_time, elapsed_time/total_files * 1000.0, elapsed_time/total_files * 100000.0/60.0))
        return
        
    def _get_tags_from_file(self, filename):
        KNOWN_NO_TAG_FILES = [".picasa.ini", "thumbs.db"]
        KNOWN_NO_TAG_EXTS = [".mov"]  #Use lower case
        if os.path.basename(filename) in KNOWN_NO_TAG_FILES:
            return(None)
        if os.path.splitext(filename)[1].lower() in KNOWN_NO_TAG_EXTS:
            return(None)
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
        
    def _get_file_signature(self, tags, filepath):
        TEXT_FILES = ['.ini', '.txt']  #file types to be compared ignoring CR/LF for OS portability.  Use lower case (extensions will be lowered before comparison)
        if tags is not None and len(tags.previews) > 0:
            signature = MD5sums.stringMD5sum(tags.previews[0].data)
        else:
            if str.lower(os.path.splitext(filepath)[1].encode()) in TEXT_FILES:
                signature = MD5sums.text_file_MD5_signature(filepath)
            else:
                record = self.photos.find_one({'path' : filepath})
                if 'md5' in record:
                    signature = record['md5']
                else:
                    logging.info("MD5 was missing on this record.  Strange...")
                    signature = MD5sums.fileMD5sum(filepath)
        return(signature)
    
def time_now():
    '''
    Returns current time in seconds with microsecond resolution
    '''
    t = datetime.datetime.now()
    return(time.mktime(t.timetuple()) + t.microsecond / 1E6)

#--------------------------------------------      
    
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



if __name__ == "__main__":
    sys.exit(main())