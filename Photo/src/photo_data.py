'''
Populates database 'db' with data extracted from a tree at 'root'
If database doesn't exist setting 'create_new' to True will create new database
Call with unicode root to get unicode traversal of the filesystem

Created on Oct 21, 2011

@author: scott_jackson

data model for node in database (not all are present in a given node):

    'path': '',
    'isdir': False,
    'size': -1,
    'md5': '',
    'signature': '',
    'dirpaths': [],
    'filepaths': [],
    'mtime': -(sys.maxint - 1), #Set default time to very old
    'timestamp': datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S'),
    'got_tags': False,
    'user_tags': [],
    'in_archive': False, #TODO probably not needed if refresh_time is used
    'refresh_time': -(sys.maxint - 1), #Set default time to very old
'''
# TODO: Add logging
# TODO: Is there a need to have 'root' defined when instantiating?  Can we instantiate and then only know 'top' when 'db_sync' is called?
# pylint: disable=line-too-long

import sys
import os
import time
import datetime
import re
import logging
import posixpath
import pymongo
import pyexiv2
import socket
import MD5sums
import ntpath

_CONFIG = 'config'
_PHOTOS = 'photos'
_DB_STATE = 'database_state'
_FS_TRAVERSE_TIME = 'fs_traverse_time'
_HOST = 'host'
_DIRTY = 'dirty'
_CLEAN = 'clean'
_TRAVERSE_PATH = 'traverse_path'
_LINUX = 'linux'
_WINDOWS = 'windows'
_PATH = 'path'
_SIGNATURE = 'signature'
_MD5 = 'md5'
_ISDIR = 'isdir'
_MD5_MATCH = 'md5_match'
_SIG_MATCH = 'sig_match'
_COLLECTION = 'collection'


def main():
    t_host = 'localhost'
    t_collection = '4DAA1001519'
    a_host = 'mongodb://192.168.1.8'
    a_collection = 'barney'
#    t_photo_dir = u"C:\\Users\\scott_jackson\\Pictures\\Uploads"
    t_photo_dir = u"C:\\Users\\scott_jackson\\Pictures"
#    t_photo_dir = u"C:\\Users\\scott_jackson\\git\\PhotoManager\\Photo\\tests\\test_photos\\target"
#    a_photo_dir = u"C:\\Users\\scott_jackson\\git\\PhotoManager\\Photo\\tests\\test_photos\\archive"
#    a_host = 'mongodb://localhost/barney'
#    a_photo_dir = u"/media/526b46db-af0b-4453-a835-de8854d51c2b/Photos"
    a_photo_dir = '/media/526b46db-af0b-4453-a835-de8854d51c2b/Photos'
#    host = t_host
#    collection = t_collection
#    photo_dir = t_photo_dir

    log_file = "C:\Users\scott_jackson\Documents\Personal\Programming\lap_log.txt"
    LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(
                        filename=log_file,
                        format=LOG_FORMAT,
                        level=logging.DEBUG,
                        filemode='w'
                        )

    start = time.time()
#    PhotoDb(t_host, t_photo_dir, create_new=False).sync_db()
#    db = set_up_db(a_host, a_collection)
#    print_empty_files(db, a_photo_dir)
#    print_empty_dirs(db, a_photo_dir)
#    print_unexpected_files(db, a_photo_dir)
#    print_hybrid_dirs(db, a_photo_dir)
#    stats = TreeStats(db, a_photo_dir).print_tree_stats()
#    print_tree(db, a_photo_dir)
    db_archive = set_up_db(a_host, a_collection)
    db_target = set_up_db(t_host, t_collection)
    print_duplicates_tree(db_archive, db_target, a_photo_dir, t_photo_dir)
    print "Done"
    sys.exit(0)

    print_duplicates_tree(a_host, t_host, a_photo_dir, t_photo_dir)
    finished = time.time() - start
    print "------------------"
    print "Done! - elapsed time {} seconds".format(finished)
    sys.exit(1)


def clean_user_tags(db):  # Used only to repair database from bug.
                        # Delete when dbs are clean and code tested
    print "Cleaning user tags..."
    records = db.photos.find({'isdir': False})
    for record in records:
        if type(record['user_tags']) != type([]):
            print record['user_tags']
            db.photos.update(
                             {'path': record['path']},
                             {'$set': {'user_tags': []}}
                             )
    print "Done"


def time_now():
    '''
    Returns current time in seconds with microsecond resolution
    '''
    t = datetime.datetime.now()
    return(time.mktime(t.timetuple()) + t.microsecond / 1E6)


def set_up_db(host, collection, create_new=False):
    '''
    Connects to database and confirms existence of config and photos
    collections.
    Will create new database if create_new is True.
    'host' follows forms allowed for Mongodb connections (server name,
    IP address, URL, etc.)
    'collection' is the name used for the photo collection, as opposed
    to the Mongodb collection (sorry for the confusing names)
    '''
    if host is None:
        print "Error - must define host (machine name, IP address, URL, ports if necessary, etc.)"
        sys.exit(1)
    try:
        client = pymongo.MongoClient(host)
    except pymongo.errors.ConnectionFailure:
        print "***ERROR*** Database connection failed to {}.  Make sure mongod is running.".format(host)
        sys.exit(1)
    except:
        print "Unknown problem connecting to mongodb"
        sys.exit(1)
    try:
        db = client[collection]
    except:
        print"***ERROR*** Problem connecting to database {} on host {}.".format(collection, host)
        sys.exit(1)
    if create_new:  # Collection creation on mongodb is implicit, so referencing them creates them if they don't exist.  With this approach we don't damage existing collections if they exist.
        config = db[_CONFIG]
        config.update({_CONFIG: _HOST}, {'$set': {_HOST: socket.gethostname(), _COLLECTION: collection}}, upsert=True)
        photos = db[_PHOTOS]
    else:
        collections = db.collection_names()
        if _CONFIG not in collections or _PHOTOS not in collections:
            print "Error - Collections {} or {} do not exist in db {} on {}.  Maybe you meant to instantiate class with the create_new flag set?".format(_CONFIG, _PHOTOS, collection, host)
            sys.exit(1)
        photos = db[_PHOTOS]
    photos.ensure_index('path', unique=True)
    photos.ensure_index('signature')
    return(db)


def check_host(config):  # TODO:  This might be wrong after host refactoring
    host = socket.gethostname()
    db_host_record = config.find_one({_CONFIG: _HOST})
    if _HOST not in db_host_record:
        print "Error - no host listed for this database.  Exiting to prevent damage."
        sys.exit(1)
    db_host = db_host_record[_HOST]
    if host != db_host:
        print "Error - Host is {}, while database was built on {}.  Update do database not allowed on this host."
        sys.exit(1)


def check_db_clean(config):  # TODO: Check this is correct after host refactoring
    states = config.find({_DB_STATE: {'$exists': True}}).sort("_id", pymongo.DESCENDING).limit(10)
    if states.count() < 1:
        print "Error - database status not available; don't know if clean"
        # TODO: Figure out how to recover - probably regenerate whole thing
        sys.exit(1)
    state = states[0]
    if state[_DB_STATE] == _DIRTY:
        # TODO: figure out what to do to recover - probably regenerate whole thing
        print "Error - DB is dirty.  No recovery options implemented."
        sys.exit(1)
    elif state[_DB_STATE] == _CLEAN:
        return
    else:
        print "Error - unknown if DB clean.  Check returned state of '{}'".format(state)
        # TODO:  figure out how to recover; probably regenerate whole thing
        sys.exit(1)


def stat_node(nodepath):
    '''stat node and return file stats as os.stat object'''
    try:
        file_stat = os.stat(nodepath)
    except:
        logging.error("Can't stat file at {0}".format(repr(nodepath)))
        sys.exit(1)
    return(file_stat)


def find_os_from_path_string(path):
    '''Identify file system based on path separators'''
    if path.count('/') > 0:
        return(_LINUX)
    elif path.count('\\') > 0:
        return(_WINDOWS)
    elif path is None:
        return(None)
    else:
        print("Error:  Path to top of tree contains no path separators, can't determine OS type.  Tree top received: {}".format(path))
        sys.exit(1)


def make_tree_regex(top):
    '''
    Return regex that when queried will extract tree starting from top, independent of OS
    '''
    os_type = find_os_from_path_string(top)
    if os_type is None:
        top_tree = '.*'
    elif os_type == _LINUX:
        top_tree = '^' + top + '$|^' + top + '/.*'
    elif os_type == _WINDOWS:
        path = re.sub(r'\\', r'\\\\', top)
        basepath = '^' + path
        children = '^' + path + '\\\\.*'
        top_tree = unicode(basepath + '|' + children)
    return re.compile(top_tree)


def get_metadata_from_file(filename):
    '''Return metadata dictionary from photo file'''
    KNOWN_NO_TAG_FILES = [".picasa.ini", "thumbs.db"]
    KNOWN_NO_TAG_EXTS = [".mov"]  # Use lower case
    if os.path.basename(filename) in KNOWN_NO_TAG_FILES:
        return(None)
    if os.path.splitext(filename)[1].lower() in KNOWN_NO_TAG_EXTS:
        return(None)
    try:
        filepath = filename.decode(sys.getfilesystemencoding())
        metadata = pyexiv2.ImageMetadata(filepath)
        metadata.read()
    except IOError as err:  # The file contains photo of an unknown image type or file missing or can't be opened
        logging.warning("%s IOError, errno=%s, strerror=%s args=%s",
                        repr(filename),
                        str(err.errno),
                        err.strerror,
                        err.args)
        return(None)
    except:
        logging.error("%s Unknown Error Trapped", repr(filename))
        return(None)
    return(metadata)


def get_timestamp_from_metadata(metadata):
    '''Return timestamp from image metadata, setting defaults for missing values'''
    if 'Exif.Photo.DateTimeOriginal' in metadata.exif_keys:
        timestamp = metadata['Exif.Photo.DateTimeOriginal'].value
    else:
        timestamp = datetime.datetime.strptime('1800:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
    return(timestamp)


def get_user_tags_from_metadata(metadata):
    '''Return user tags from photo metadata'''
    if 'Xmp.dc.subject' in metadata.xmp_keys:
        return(metadata['Xmp.dc.subject'].value)
    else:
        return([])


def find_empty_files(db, top=None):  # TODO:  Should we check database is clean??
    '''
    Return iterator of dictionaries for empty files in database
    '''
    emptylist = db.photos.find({'isdir': False, 'path': make_tree_regex(top), 'size': long(0)})
    return(emptylist)


def print_empty_files(db, top=None):
    '''Print empty files in database 'db' in form suitable to remove them'''
    records = find_empty_files(db, top)
    print "#Number of empty files: {}".format(records.count())
    for empty in records:
        print "#rm {} # Size {}".format(empty['path'], empty['size'])
    print "#--Done finding empty files---"


def find_empty_dirs(db, top=None):
    '''
    Return iterator of dictionaries for empty directories
    '''
    emptylist = db.photos.find({'isdir': True, 'filepaths': [], 'dirpaths': [], 'path': make_tree_regex(top)},{'path': 1, 'filepaths': 1, 'dirpaths': 1})
    return(emptylist)


def print_empty_dirs(db, top=None):
    '''Print empty directories in database db suitable to delete them'''
    emptylist = find_empty_dirs(db, top)
    print "Number of empty dirs: {}".format(emptylist.count())
    for empty in emptylist:
        print "#rmdir {} # File list {}, Dir list {}".format(empty['path'], empty['filepaths'], empty['dirpaths'])
    print "#--Done finding empty dirs---"


def find_unexpected_files(db, top=None):
    '''
    Find unexpected file types and return iterator of dictionaries of them
    '''
    EXPECTED = [
                '\.jpg$',
                '\.png$',
                'picasa.ini$',
                'thumbs.db$',
                '\.mov$',
                '\.avi$',
                '\.thm$',
                '\.bmp$',
                '\.gif$'
                ]
    all_targets = ''
    for target in EXPECTED:
        all_targets = all_targets + target + "|"
    all_targets = all_targets[:-1]
    target_regex = re.compile(all_targets, re.IGNORECASE)
    records = db.photos.aggregate([
                                 {'$match': {'path': make_tree_regex(top)}},
                                 {'$match': {'isdir': False}},
                                 {'$match': {'path': {'$not': target_regex}}},
                                 {'$project': {'_id': False, 'size': True, 'path': True}}
                                ])
    return(records['result'])


def print_unexpected_files(db, top=None):
    '''
    Print list of unexpected file types suitable for use in shell script to move them  # TODO:  Should call function that does move independent of OS, creating directory tree along the way
    '''
    records = find_unexpected_files(db, top)
    print "# Number of unexpected type files: {}".format(len(records))
    for record in records:
        print("#rm {} # Size: {}".format(record['path'], record['size']))
    print "#--Done finding unexpected files--"


def find_duplicates(db_archive, db_target, top_archive=None, top_target=None):
    print "Finding duplicates..."
    t_regex = make_tree_regex(top_target)
    a_regex = make_tree_regex(top_archive)
    db_target.photos.find({}, {'$unset': {_SIG_MATCH: '', _MD5_MATCH: ''}})  # Unset any previous match search  TODO:  Not sure this is correct.  Might need to be update with 'multi'
    records = db_target.photos.find({_PATH: t_regex, _ISDIR: False})
    for record in records:
        match = db_archive.photos.find_one({_PATH: a_regex, _SIGNATURE: record[_SIGNATURE]})
        if match is None:
#            t_photos.update({_PATH: record[_PATH]}, {'$set': {'unique': True}})
            pass  # No need to tag unique files
        else:
            if record[_PATH] == match[_PATH]:
                pass  # skip self if comparing within same host and tree  TODO: check host too
            else:
                if record[_MD5] == match[_MD5]:
                    db_target.photos.update({_PATH: record[_PATH]}, {'$set': {_MD5_MATCH: match[_PATH]}})
                else:
                    db_target.photos.update({_PATH: record[_PATH]}, {'$set': {_SIG_MATCH: match[_PATH]}})
    print "Done finding duplicates."


def print_duplicates(db_archive, db_target, top_archive=None, top_target=None):
    find_duplicates(db_archive, db_target, top_archive, top_target)
    regex = make_tree_regex(top_target)
#    unique_records = photos.find({_PATH: regex, '$exists': {_MD5_MATCH: False}, '$exists': {_SIG_MATCH: False}})
    unique_records = db_target.photos.find(
                                           {
                                            _PATH: regex, _MD5_MATCH: {'$exists': False},
                                            _SIG_MATCH: {'$exists': False}
                                            }
                                           )
    md5_records = db_target.photos.find(
                                        {
                                         _PATH: regex, _MD5_MATCH: {'$exists': True}
                                         }
                                        )
    sig_records = db_target.photos.find(
                                        {
                                         _PATH: regex, _SIG_MATCH: {'$exists': True}
                                         }
                                        )
    print("Match status for {}".format(top_target))
    for u in unique_records:
        print "Unique: {}".format(u[_PATH])
    for m in md5_records:
        print "MD5 match: {} matches {}".format(m[_PATH], m[_MD5_MATCH])
    for s in sig_records:
        print "Signature match: {} matches {}".format(s[_PATH], s[_SIG_MATCH])


def print_duplicates_tree(db_archive, db_target, top_archive=None, top_target=None):
    '''Print tree from 'top' indenting each level and showing duplicate status'''
    find_duplicates(db_archive, db_target, top_archive, top_target)
    offset = tree_depth(top_target)
    os_type = find_os_from_path_string(top_target)
    if os_type == _LINUX:
        basename = posixpath.basename
    if os_type == _WINDOWS:
        basename = ntpath.basename
    indent = 4  # Number of spaces to indent printout
    photo_count = db_target.photos.find({_PATH: make_tree_regex(top_target)}).count()
    if photo_count is None:
        print "No files found starting at {}".format(top_target)
        return
    else:
        print "Walking tree from: {}, {} nodes found.".format(top_target, photo_count)
        for dirpath, _unused_dirs, files in walk_db_tree(db_target.photos, top_target):
            print '{}[{}]'.format(' ' * indent * (tree_depth(dirpath) - offset), basename(dirpath))
            for f in files:
                record = db_target.photos.find_one({_PATH: f})
                if _MD5_MATCH not in record and _SIG_MATCH not in record:
                    print '{}Unique: {}'.format(' ' * indent * (tree_depth(f) - offset), basename(f))
                else:
                    if _MD5_MATCH in record:
                        print '{}MD5 match: {} matches {}'.format(' ' * indent * (tree_depth(f) - offset), basename(f), record[_MD5_MATCH])
                    elif _SIG_MATCH in record:
                        print '{}Sig match: {} matches {}'.format(' ' * indent * (tree_depth(f) - offset), basename(f), record[_SIG_MATCH])
                        
                        
def walk_db_tree(collection, top, topdown=True):
    record = collection.find_one({'path': top}, {'_id': False, 'dirpaths': True, 'filepaths': True})
    if record is None:
        raise IndexError('No files found for db tree walk at {}'.format(top))
    if topdown:
        yield top, record['dirpaths'], record['filepaths']
    for dirs in record['dirpaths']:
        for t in walk_db_tree(collection, dirs, topdown):
            yield t
    if not topdown:
        yield top, record['dirpaths'], record['filepaths']


def tree_depth(path):
    os_type = find_os_from_path_string(path)
    if os_type == _LINUX:
        return(path.count('/'))
    if os_type == _WINDOWS:
        return(path.count('\\'))
    if os_type is None:
        return(0)


def print_tree(db, top):
    '''Print tree from 'top' indenting each level'''
    offset = tree_depth(top)
    os_type = find_os_from_path_string(top)
    if os_type == _LINUX:
        basename = posixpath.basename
    if os_type == _WINDOWS:
        basename = ntpath.basename
    indent = 4  # Number of spaces to indent printout
    photo_count = db.photos.find({_PATH: make_tree_regex(top)}).count()
    if photo_count is None:
        print "No files found starting at {}".format(top)
        return
    else:
        print "Walking tree from: {}, {} nodes found.".format(top, photo_count)
        for dirpath, _unused_dirs, files in walk_db_tree(db, top):
            print '{}{}'.format(' ' * indent * (tree_depth(dirpath) - offset), basename(dirpath))
            for f in files:
                print '{}{}'.format(' ' * indent * (tree_depth(f) - offset), basename(f))


class TreeStats():
    def __init__(self, db, top):
        self.db = db
        self.top = top
        self.top_regex = make_tree_regex(top)
        self.total_nodes = 0
        self.total_dirs = 0
        self.total_files = 0
        self.tagged_records = 0
        self.unique_signatures = 0
        self.unique_md5s = 0
        self.compute_tree_stats()
        self.signatures_iter = None
        self.md5_iter = None

    def compute_tree_stats(self):
        self.total_nodes = self.db.photos.find({'path': self.top_regex}).count()
        self.total_dirs = self.db.photos.find({'path': self.top_regex, 'isdir': True}).count()
        self.total_files = self.db.photos.find({'path': self.top_regex, 'isdir': False}).count()
        self.tagged_records = self.db.photos.find(
                                                  {
                                                   'path': self.top_regex,
                                                   'isdir': False,
                                                   'user_tags': {'$ne': []}
                                                   }
                                                  ).count()
        self.signatures_iter = self.db.photos.aggregate([
                                               {"$match": {
                                                            "path": self.top_regex,
                                                            "isdir": False,
                                                            "signature": {"$ne": ""}
                                                            }
                                               },
                                               {"$group": {
                                                           "_id": "$signature"
                                                           }
                                                }
                                              ])
        if self.signatures_iter['ok'] != 1:
            raise RuntimeError('Mongodb return code not=1.  Got: {}'.format(self.signatures_iter['ok']))
        self.unique_signatures = len(self.signatures_iter['result'])
        self.md5_iter = self.db.photos.aggregate([
                                           {"$match": {
                                                        "path": self.top_regex,
                                                        "isdir": False,
                                                        "md5": {"$ne": ""}
                                                        }
                                           },
                                           {"$group": {
                                                       "_id": "$md5"
                                                       }
                                            }
                                          ])
        if self.md5_iter['ok'] != 1:
            raise RuntimeError('Mongodb return code not=1.  Got: {}'.format(self.signatures_iter['ok']))
        self.unique_md5s = len(self.md5_iter['result'])

    def print_tree_stats(self):
        self.compute_tree_stats()
        print "Stats for tree rooted at {}".format(self.top)
        if self.total_nodes == 0:
            print "No nodes (files or directories) found.  Aborting stats."
            return
        print("Total nodes: {}".format(self.total_nodes))
        print("Total dirs: {}".format(self.total_dirs))
        print("Total files: {}".format(self.total_files))
        print("Total tagged: {}, {:.1%} of files".format(self.tagged_records, float(self.tagged_records)/self.total_files))
        print("Unique signatures={}, {} duplicates ({:.1%})".format(self.unique_signatures, self.total_files - self.unique_signatures, 1.0 - float(self.unique_signatures)/self.total_files))
        print("Unique MD5s={}, {} duplicates ({:.1%})".format(self.unique_md5s, self.total_files - self.unique_md5s, 1.0 - float(self.unique_md5s)/self.total_files))

# def move_files(safe_top, path):
#     #Test for OS here
#     if _LINUX:
#         #if dir does not exist:
#         #    create dir
#         #move file to safe_top\path
#     elif WIN:
#         #if dir does not exist
#         #    create dir
#         #move file to safe_top\path
#     else:
#         print "***FAIL*** Could not determine OS."

# def find_md5_match_dirs(host, top):
#     db, config, photos=set_up_db(host)
#     tree_regex=make_tree_regex(top)
#     dir_records=photos.find({'isdir': True, 'dirpaths': empty, 'filepaths': not empty}, {'_id': False, 'path': True})
#     for dir_record in dir_records:
#         photos.find({'isdir': False, 'path': dir_record['path']})


def find_hybrid_dirs(db, top):  # Broken
    # Look for dirs that have files and dirs in them (this shouldn't happen if the photo directory is clean)
#    hybridlist=db.find({'$and': [{'$not': {'filepaths': '[]'}}, {'$not': {'dirpaths': '[]'}}]}, {'path': 1, 'filepaths': 1, 'dirpaths': 1})
    tree_regex = make_tree_regex(top)
    hybridlist = db.photos.find(
                                {
                                 'isdir': True,
                                 'path': tree_regex,
                                 'filepaths': {'$ne': []},
                                 'dirpaths': {'$ne': []}
                                 },
                                 {
                                  '_id': 0,
                                  'path': 1,
                                  'filepaths': 1,
                                  'dirpaths': 1
                                  }
                                )
    return(hybridlist)


def print_hybrid_dirs(db, top):
    hybridlist = find_hybrid_dirs(db, top)
    print "Finding dirs that contain both files and directories.  Length of hybrid list:", hybridlist.count()
    for hybrids in hybridlist:
        print "#{} # File list {}, Dir list {}".format(hybrids['path'], hybrids['filepaths'], hybrids['dirpaths'])
    print "#--Done finding hybrid directories"


class PhotoDb(object):
    def __init__(self, collection, top, host='localhost', create_new=False):
        self.root = os.path.normpath(top)
        self.start_time = 0  # Persistent variable for method in this class because I don't know a better way
        _unused_db, self.config, self.photos = set_up_db(host, collection, create_new)
        check_host(self.config)
        if not create_new:
            check_db_clean(self.config)

    def sync_db(self, top=None):
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
        time.sleep(1)  # Wait a second to make double-dog sure mongodb is caught up
        fresh_times = self.config.find().sort(_FS_TRAVERSE_TIME, pymongo.DESCENDING).limit(10)
        if fresh_times.count() < 1:
            # TODO: check latest time is associated with 'dirty' state and top paths are the same
            logging.error('ERROR: no file system fresh start time available.  Too dangerous to continue.')
            sys.exit(1)
        fresh_time = fresh_times[0]['fs_traverse_time']
        regex = make_tree_regex(top)
        records = self.photos.find({'path': regex, 'refresh_time': {'$lt': fresh_time}})
        num_removed = records.count()
        for record in records:
            logging.info("Removed records for node: {}".format(record['path']))
        rm_stat = self.photos.remove({'path': regex, 'refresh_time': {'$lt': fresh_time}})
        if num_removed != int(rm_stat['n']):
            logging.error("Error - number expected to be removed from database does not match expected number.  Database return message: {}".format(rm_stat))
        logging.info("Done pruning.  Pruned {} nodes".format(rm_stat['n']))

    def _mark_db_status(self, status):
        if status == 'dirty':
            self.config.insert({_DB_STATE: 'dirty', _FS_TRAVERSE_TIME: time_now(), _TRAVERSE_PATH: self.root})
        elif status == 'clean':
            self.config.insert({_DB_STATE: 'clean', _FS_TRAVERSE_TIME: time_now(), _TRAVERSE_PATH: self.root})
        else:
            logging.error("Error: bad database status received.  Got: '{}'".format(status))

    def _walk_error(self, walk_err):
        print "Error {}:{}".format(walk_err.errno, walk_err.strerror)  # TODO: Maybe some better error trapping here...and do we need to encode strerror if it might contain unicode??
        raise

    def _update_file_record(self, filepath):
        file_stat = stat_node(filepath)
        db_file = self.photos.find_one({'path': filepath}, {'_id': 0})
        if db_file is None:
            logging.debug("New File detected: {0}".format(repr(filepath)))
            self.photos.insert(
                           {
                            'path': filepath,
                            'isdir': False,
                            'size': file_stat.st_size,
                            'mtime': file_stat.st_mtime,
                            'in_archive': True,
                            'refresh_time': time_now()
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
                           {'path': db_file['path']},
                           {'$set': {'refresh_time': time_now()}}
                           )

    def _update_dir_record(self, dirpath, dirpaths, filepaths):
        db_dir = self.photos.find_one({'path': dirpath}, {'path': True, '_id': False})
        if db_dir is None:
            logging.debug("New directory detected: {0}".format(repr(dirpath)))
            db_dir = dirpath
        self.photos.update(
                       {'path': dirpath},
                       {
                        'path': dirpath,
                        'isdir': True,
                        'dirpaths': dirpaths,
                        'filepaths': filepaths,
                        'in_archive': True,
                        'refresh_time': time_now()
                        },
                       upsert=True)  # Replace existing record - TODO: wipe out previous sum data, if the record exists

    def _traverse_fs(self, top=None):
        if top is None:
            top = self.root
        logging.info("Traversing filesystem tree starting at {}...".format(top))
        if os.path.isfile(top):
            self._update_file_record(top)
        else:
            for dirpath, dirnames, filenames in os.walk(top, onerror=self._walk_error):
                dirpath = os.path.normpath(dirpath)
                dirpaths = [os.path.normpath(os.path.join(dirpath, dirname)) for dirname in dirnames]
                filepaths = [os.path.normpath(os.path.join(dirpath, filename)) for filename in filenames]
                self._update_dir_record(dirpath, dirpaths, filepaths)
                for filepath in filepaths:
                    self._update_file_record(filepath)
        logging.info("Done traversing filesystem tree.")
        return

    def _update_md5(self, top=None):
        logging.info("Computing missing md5 sums...")
        if top is None:
            top = self.root
        regex = make_tree_regex(top)
        files = self.photos.find(
                             {
                             '$and': [
                                        {
                                         'path': regex
                                         },
                                         {
                                          'isdir': False
                                          },
                                          {
                                            '$or': [
                                                     {
                                                      'md5': {'$exists': False}
                                                     },
                                                     {
                                                      'md5': ''
                                                      }
                                                     ]
                                          },

                                        ]
                              },
                             {
                              '_id': False,
                              'path': True
                              },
                              timeout=False
                              )
        logging.info("Number of files for MD5 update: {}".format(files.count()))
        for n, path in enumerate(files, start=1):
            logging.info('Computing MD5 {} for: {}'.format(n, repr(path['path'])))
            md5sum = MD5sums.fileMD5sum(path['path'])
            self.photos.update({'path': path['path']}, {'$set': {'md5': md5sum}})
        logging.info("Done computing missing md5 sums.")

    def _update_tags(self, top=None):
        logging.info('Updating file tags...')
        if top is None:
            top = self.root
        tree_regex = make_tree_regex(top)
        files = self.photos.find(
                              {
                               'path': tree_regex,
                               'isdir': False,
                               '$or': [
                                        {'got_tags': {'$exists': False}},
                                        {'got_tags': False}
                                        ]
                               },
                               {
                                '_id': False, 'path': True
                                },
                             timeout=False)
        total_files = files.count()
        for file_count, photo_record in enumerate(files, start=1):
            self._monitor_tag_progress(total_files, file_count)
            photopath = photo_record['path']
            tags = get_metadata_from_file(photopath)
            if tags is None:
                # logging.warn("Bad tags in: {0}".format(repr(photopath)))
                user_tags = []
                timestamp = datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
            else:
                user_tags = get_user_tags_from_metadata(tags)
                timestamp = get_timestamp_from_metadata(tags)
            signature = self._get_file_signature(tags, photopath)  # Get signature should now do the thumbnailMD5 and if not find another signature.
            self.photos.update(
                               {'path': photopath},
                                {'$set':
                                 {
                                  'user_tags': user_tags,
                                  'timestamp': timestamp,
                                  'signature': signature,
                                  'got_tags': True
                                 }
                                }
                               )
        logging.info('Done updating file tags.')
        return()

    def _monitor_tag_progress(self, total_files, file_count):
        PROGRESS_COUNT = 500  # How often to report progress in units of files
        if file_count == 1:  # First call; initialize
            self.start_time = time_now()
            logging.info("Extracting tags for {0}.  File count={1}".format(repr(self.root), total_files))
        if not file_count % PROGRESS_COUNT:
            elapsed_time = time_now() - self.start_time
            total_time_projected = float(elapsed_time) / float(file_count) * total_files
            time_remaining = float(elapsed_time) / float(file_count) * float(total_files - file_count)
            logging.info("{0} of {1}={2:.2f}%, {3:.1f} seconds, time remaining: {4} of {5}".format(file_count, total_files, 1.0 * file_count / total_files * 100.0, elapsed_time, str(datetime.timedelta(seconds=time_remaining)), str(datetime.timedelta(seconds=total_time_projected))))
        if file_count == total_files:
            elapsed_time = time_now() - self.start_time
            logging.info("Tags extracted: {0}.  Elapsed time: {1:.0g} seconds or {2:.0g} ms per file={3:.0g} for 100k files".format(file_count, elapsed_time, elapsed_time/total_files * 1000.0, elapsed_time/total_files * 100000.0/60.0))
        return

    def _get_file_signature(self, metadata, filepath):
        TEXT_FILES = ['.ini', '.txt']  # file types to be compared ignoring CR/LF for OS portability.  Use lower case (extensions will be lowered before comparison)
        if metadata is not None and len(metadata.previews) > 0:
            signature = MD5sums.stringMD5sum(metadata.previews[0].data)
        else:
            if str.lower(os.path.splitext(filepath)[1].encode()) in TEXT_FILES:
                signature = MD5sums.text_file_MD5_signature(filepath)
            else:
                record = self.photos.find_one({'path': filepath})
                if 'md5' in record:
                    signature = record['md5']
                else:
                    logging.info("MD5 was missing on this record.  Strange...")
                    signature = MD5sums.fileMD5sum(filepath)
        return(signature)

# --------------------------------------------

#Stole this main from Guido van van Rossum at http://www.artima.com/weblogs/viewpost.jsp?thread=4829
#class Usage(Exception):
#    def __init__(self, msg):
#        self.msg='''Usage:  get_photo_data [-p pickle_path] [-l log_file] photos_path'''
#
#def main(argv=None):
#    if argv is None:
#        argv=sys.argv
#    try:
#        try:
#            opts, args=getopt.getopt(argv[1:], "h", ["help"])
#        except getopt.error, msg:
#             raise Usage(msg)
#        # more code, unchanged
#    except Usage, err:
#        print >>sys.stderr, err.msg
#        print >>sys.stderr, "for help use --help"
#        re

if __name__ == "__main__":
    sys.exit(main())
