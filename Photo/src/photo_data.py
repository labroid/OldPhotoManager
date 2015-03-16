"""
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
"""
# TODO: Add logging
# TODO: Is there a need to have 'root' defined when instantiating?
# Can we instantiate and then only know 'top' when 'db_sync' is called?
# pylint: disable=line-too-long

import sys
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
import itertools

CONFIG = 'config'
PHOTOS = 'photos'
DB_STATE = 'database_state'
FS_TRAVERSE_TIME = 'fs_traverse_time'
REFRESH_TIME = 'refresh_time'
HOST = 'host'
DIRTY = 'dirty'
CLEAN = 'clean'
TRAVERSE_PATH = 'traverse_path'
LINUX = 'linux'
WINDOWS = 'windows'
PATH = 'path'
SIGNATURE = 'signature'
MD5 = 'md5'
ISDIR = 'isdir'
MD5_MATCH = 'md5_match'
SIG_MATCH = 'sig_match'
COLLECTION = 'collection'
FILEPATHS = 'filepaths'
DIRPATHS = 'dirpaths'
SIZE = 'size'
USER_TAGS = 'user_tags'
MTIME = 'mtime'


def main():
    t_host = 'localhost'
    t_repository = '4DAA1001519'
    # a_host = 'mongodb://192.168.1.8'
    a_host = t_host
    a_repository = 'barney'
    t_photo_dir = u"C:\\Users\\scott_jackson\\Pictures\\Uploads"
    # t_photo_dir = u"C:\\Users\\scott_jackson\\Pictures"
    # t_photo_dir = u"C:\\Users\\scott_jackson\\git\\PhotoManager\\Photo\\tests\\test_photos\\target"
    # a_photo_dir = u"C:\\Users\\scott_jackson\\git\\PhotoManager\\Photo\\tests\\test_photos\\archive"
    # a_host = 'mongodb://localhost/barney'
    # a_photo_dir = u"/media/526b46db-af0b-4453-a835-de8854d51c2b/Photos"
    a_photo_dir = '/media/526b46db-af0b-4453-a835-de8854d51c2b/Photos'
    # host = t_host
    # collection = t_collection
    # photo_dir = t_photo_dir

    log_file = "C:\Users\scott_jackson\Documents\Personal\Programming\lap_log.txt"
    log_format = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
    logging.basicConfig(
        filename=log_file,
        format=log_format,
        level=logging.DEBUG,
        filemode='w'
    )

    start = time.time()
    database = set_up_db(a_repository, a_host)
    no_tags, tagged = dirs_by_no_tags(database, a_photo_dir)
    print "***NO TAGS***"
    for path in no_tags.keys():
        print path, no_tags[path]
    print "***TAGS***"
    for path in tagged.keys():
        print path, tagged[path]
    # extract_picture_frame_set(database, a_photo_dir, "SJJ Frame", "/home/scott/SJJ_Frame")
    # PhotoDb(t_host, t_repository, t_photo_dir, create_new=False).sync_db()  # TODO: Still need to test this
    # db = set_up_db(a_collection, a_host)
    # print_empty_files(db, a_photo_dir)
    # print_empty_dirs(db, a_photo_dir)
    #    print_unexpected_files(db, a_photo_dir)
    #    print_hybrid_dirs(db, a_photo_dir)
    #    stats = TreeStats(db, a_photo_dir).print_tree_stats()
    #    print_tree(db, a_photo_dir)
    #    db_archive = set_up_db(a_collection, a_host)
    #    db_target = set_up_db(t_collection, t_host)
    #    print_duplicates_tree(db_archive, db_target, a_photo_dir, t_photo_dir)
    print "Done"
    sys.exit(0)

    # print_duplicates_tree(a_host, t_host, a_photo_dir, t_photo_dir)
    # finished = time.time() - start
    # print "------------------"
    # print "Done! - elapsed time {} seconds".format(finished)
    # sys.exit(1)


def clean_user_tags(database):  # Used only to repair database from bug.
    # Delete when dbs are clean and code tested
    print "Cleaning user tags..."
    records = database.photos.find({ISDIR: False})
    for record in records:
        if isinstance(record[USER_TAGS], list):
            print record[USER_TAGS]
            database.photos.update(
                {PATH: record[PATH]},
                {'$set': {USER_TAGS: []}}
            )
    print "Done"


def time_now():
    """
    Returns current time in seconds with microsecond resolution
    """
    coarse_time = datetime.datetime.now()
    return time.mktime(coarse_time.timetuple()) + coarse_time.microsecond / 1E6


def set_up_db(repository, host='localhost', create_new=False):
    """
    Connects to database on 'host' named 'repository' and confirms existence of config and photos
    collections.
    Will create new database if create_new is True.
    'host' follows forms allowed for Mongodb connections (server name,
    IP address, URL, etc.)
    'repository' is the name used for the photo repository, as opposed
    to the Mongodb repository (sorry for the confusing names)
    """
    if host is None:
        error_message = "Error - must define host (machine name, IP address, URL, ports if necessary, etc.)"
        logging.error(error_message)
        raise (ValueError(error_message))
    try:
        client = pymongo.MongoClient(host)
    except pymongo.errors.ConnectionFailure:
        error_message = "***ERROR*** Database connection failed to {}. " \
                        "Make sure mongod is running.".format(host)
        logging.error(error_message)
        raise (ValueError(error_message))
    except:
        error_message = "Unknown problem connecting to mongodb on {}.".format(host)
        logging.error(error_message)
        raise (ValueError(error_message))
    try:
        database = client[repository]
    except:
        error_message = "***ERROR*** Problem connecting to database {} on host {}.".format(repository, host)
        logging.error(error_message)
        raise (ValueError(error_message))
    if create_new:  # Collection creation on mongodb is implicit, so referencing them creates them
        # if they don't exist.  With this approach we don't damage existing collections
        # if they exist.
        config = database[CONFIG]
        config.update(
            {CONFIG: HOST},
            {'$set': {HOST: socket.gethostname(), COLLECTION: repository}},
            upsert=True)
        photos = database[PHOTOS]
    else:
        collections = database.collection_names()
        if CONFIG not in collections or PHOTOS not in collections:
            error_message = "Error - Collections {} or {} do not exist in database {} on {}.\
              Maybe you meant to instantiate class with the create_new flag set?". \
                format(CONFIG, PHOTOS, repository, host)
            logging.error(error_message)
            raise (ValueError(error_message))
        photos = database[PHOTOS]
    photos.ensure_index(PATH, unique=True)
    photos.ensure_index(SIGNATURE)
    return database


def check_host(config):
    repository_host_record = config.find_one({CONFIG: HOST})
    if repository_host_record is None or HOST not in repository_host_record:
        error_message = "Error - no host listed for this database.  Exiting to prevent damage."
        logging.error(error_message)
        raise (ValueError(error_message))
    current_host = socket.gethostname()
    repository_host = repository_host_record[HOST]
    if current_host != repository_host:
        error_message = "Error - Host is {}, while database was built on {}.  " \
                        "Update to database not allowed on this host.".format(current_host, repository_host)
        logging.error(error_message)
        raise (ValueError(error_message))


def check_db_clean(config):  # TODO: Check this is correct after host refactoring
    states = config.find({DB_STATE: {'$exists': True}}).sort("_id", pymongo.DESCENDING).limit(10)
    if states.count() < 1:
        print "Error - database status not available; don't know if clean.  " \
              "Consider updating repository with create_new flag set"
        # TODO: Figure out how to recover - probably regenerate whole thing
        sys.exit(1)
    state = states[0]
    if state[DB_STATE] == DIRTY:
        # TODO: figure out what to do to recover - probably regenerate whole thing
        print "Error - DB is dirty.  No recovery options implemented."
        sys.exit(1)
    elif state[DB_STATE] == CLEAN:
        return
    else:
        print "Error - unknown if DB clean.  Check returned state of '{}'".format(state)
        # TODO:  figure out how to recover; probably regenerate whole thing
        sys.exit(1)


def stat_node(nodepath):
    """stat node and return file stats as os.stat object"""
    try:
        file_stat = os.stat(nodepath)
    except:
        error_message = "Can't stat file at {0}".format(repr(nodepath))
        logging.error(error_message)
        raise (ValueError, error_message)
    return file_stat


def dirs_by_no_tags(database, top):
    photo_directories = database.photos.find({PATH: make_tree_regex(top), ISDIR: True, DIRPATHS: []})
    no_tag_dict = {}
    tagged_dict = {}
    for directory in photo_directories:
        user_tag_set = [
            x['user_tags'] for x in database.photos.find(
                {
                    PATH: make_tree_regex(directory[PATH]),
                    ISDIR: False
                }
            )
        ]
        cumulative_tags = set(list(itertools.chain(*user_tag_set)))
        if cumulative_tags == set([]):
            no_tag_dict[directory[PATH]] = cumulative_tags
        else:
            tagged_dict[directory[PATH]] = cumulative_tags
    return no_tag_dict, tagged_dict


def dirs_with_all_match(database, top):  # TODO: This is a framework and is totally wrong
    photo_directories = database.photos.find({PATH: make_tree_regex(top), ISDIR: True, DIRPATHS: []})
    no_tag_list = []
    for directory in photo_directories:
        user_tag_set = [
            x['user_tags'] for x in database.photos.find(
                {
                    PATH: make_tree_regex(directory[PATH]),
                    ISDIR: False
                }
            )
        ]
        cumulative_tags = set(list(itertools.chain(*user_tag_set)))
        if cumulative_tags == set([]):
            pass
        else:
            no_tag_list.append(directory)
    return no_tag_list


def extract_picture_frame_set(database, top, tag, output_dir):  # TODO: Write test for this
    """
    Scan database and produces shell script suitable to select and convert photos
    """
    records = database.photos.aggregate(
        [
            {'$match': {
                USER_TAGS: {'$in': [tag]},
                PATH: make_tree_regex(top)
                }
             },
            {'$sort': {SIGNATURE: 1}},
            {'$group': {
                '_id': "$signature",
                'firstPath': {'$first': '$path'}
                }
             },
            {'$project': {'firstPath': 1}}
        ])
    print "#Number of records:", len(records['result'])
    for count, entry in enumerate(records['result'], start=1355):
        path = entry['firstPath']
        print 'convert "{}" -resize x1080 "{}/deres{}"'.format(path, output_dir, count)


def find_os_from_path_string(path):
    """Identify file system based on path separators"""
    if path.count('/') > 0:
        return LINUX
    elif path.count('\\') > 0:
        return WINDOWS
    elif path is None:
        return None
    else:
        error_message = "Error:  Path to top of tree contains no path separators, \
            can't determine OS type.  Tree top received: {}".format(path)
        logging.error(error_message)
        raise (ValueError, error_message)


def make_tree_regex(top):
    """
    Return regex that when queried will extract tree starting from top, independent of OS
    """
    os_type = find_os_from_path_string(top)
    if os_type is None:
        top_tree = '.*'
    elif os_type == LINUX:
        top_tree = '^' + top + '$|^' + top + '/.*'
    elif os_type == WINDOWS:
        path = re.sub(r'\\', r'\\\\', top)
        basepath = '^' + path
        children = '^' + path + '\\\\.*'
        top_tree = unicode(basepath + '|' + children)
    else:
        top_tree = '.*'
    return re.compile(top_tree)


def get_metadata_from_file(filename):
    """Return metadata dictionary from photo file"""
    known_no_tag_files = [".picasa.ini", "thumbs.db"]
    known_no_tag_exts = [".mov"]  # Use lower case
    if os.path.basename(filename) in known_no_tag_files:
        return None
    if os.path.splitext(filename)[1].lower() in known_no_tag_exts:
        return None
    try:
        filepath = filename.decode(sys.getfilesystemencoding())
        metadata = pyexiv2.ImageMetadata(filepath)
        metadata.read()
    except IOError as err:  # The file contains photo of an unknown image type or \
        # file missing or can't be opened
        logging.warning("%s IOError, errno=%s, strerror=%s args=%s",
                        repr(filename),
                        str(err.errno),
                        err.strerror,
                        err.args)
        return None
    except:
        logging.error("%s Unknown Error Trapped", repr(filename))
        return None
    return metadata


def get_timestamp_from_metadata(metadata):
    """Return timestamp from image metadata, setting defaults for missing values"""
    if 'Exif.Photo.DateTimeOriginal' in metadata.exif_keys:
        timestamp = metadata['Exif.Photo.DateTimeOriginal'].value
    else:
        timestamp = datetime.datetime.strptime('1800:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
    return timestamp


def get_user_tags_from_metadata(metadata):
    """Return user tags from photo metadata"""
    if 'Xmp.dc.subject' in metadata.xmp_keys:
        return metadata['Xmp.dc.subject'].value
    else:
        return []


def check_top_within_last_update(database, top):
    updated_path = database.config.find_last("blah")
    if top == updated_path:
        return
    if updated_path in top:
        return
    print '#***WARNING*** Action being taken on {} which is not with latest database update path of {}'. \
        format(top, updated_path)
    pass


def find_empty_files(database, top=None):  # TODO:  Should we check database is clean??
    """
    Return iterator of dictionaries for empty files in database
    """
    emptylist = database.photos.find(
        {
            ISDIR: False,
            PATH: make_tree_regex(top),
            SIZE: long(0)
        }
    )
    return emptylist


def print_empty_files(database, top=None):
    """Print empty files in database 'database' in form suitable to remove them"""
    records = find_empty_files(database, top)
    print "#Number of empty files: {}".format(records.count())
    for empty in records:
        print "#rm {} # Size {}".format(empty[PATH], empty[SIZE])
    print "#--Done finding empty files---"


def find_empty_dirs(database, top=None):
    """
    Return iterator of dictionaries for empty directories
    """
    emptylist = database.photos.find(
        {
            ISDIR: True,
            FILEPATHS: [],
            DIRPATHS: [],
            PATH: make_tree_regex(top)
        },
        {
            PATH: 1,
            FILEPATHS: 1,
            DIRPATHS: 1
        }
    )
    return emptylist


def print_empty_dirs(database, top=None):
    """Print empty directories in database database suitable to delete them"""
    emptylist = find_empty_dirs(database, top)
    print "Number of empty dirs: {}".format(emptylist.count())
    for empty in emptylist:
        print "#rmdir {} # File list {}, Dir list {}".format(
            empty[PATH], empty[FILEPATHS], empty[DIRPATHS])
    print "#--Done finding empty dirs---"


def find_unexpected_files(database, top=None):
    """
    Find unexpected file types and return iterator of dictionaries of them
    """
    expected = [
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
    for target in expected:
        all_targets = all_targets + target + "|"
    all_targets = all_targets[:-1]
    target_regex = re.compile(all_targets, re.IGNORECASE)
    records = database.photos.aggregate([
        {'$match': {PATH: make_tree_regex(top)}},
        {'$match': {ISDIR: False}},
        {'$match': {PATH: {'$not': target_regex}}},
        {'$project': {'_id': False, SIZE: True, PATH: True}}
    ])
    return records['result']


def print_unexpected_files(database, top=None):
    """
    Print list of unexpected file types suitable for use in shell script to move them
    TODO:  Should call function that does move independent of OS,
    creating directory tree along the way
    """
    records = find_unexpected_files(database, top)
    print "# Number of unexpected type files: {}".format(len(records))
    for record in records:
        print("#rm {} # Size: {}".format(record[PATH], record[SIZE]))
    print "#--Done finding unexpected files--"


def find_duplicates(db_archive, db_target, top_archive=None, top_target=None):
    print "Finding duplicates..."
    t_regex = make_tree_regex(top_target)
    a_regex = make_tree_regex(top_archive)
    result = db_target.photos.update(
        {},
        {'$unset': {SIG_MATCH: '', MD5_MATCH: ''}},
        upsert=False,
        multi=True
    )  # Unset any previous match search
    print "clear result: {}".format(result)
    records = db_target.photos.find({PATH: t_regex, ISDIR: False})
    for record in records:
        print "Target:{}".format(record)
        match = db_archive.photos.find_one({PATH: a_regex, SIGNATURE: record[SIGNATURE]})
        if match is None:
            # t_photos.update({_PATH: record[_PATH]}, {'$set': {'unique': True}})
            pass  # No need to tag unique files
        else:
            if record[PATH] == match[PATH]:
                pass  # skip self if comparing within same host and tree  TODO: check host too
            else:
                if record[MD5] == match[MD5]:
                    db_target.photos.update(
                        {PATH: record[PATH]},
                        {'$set': {MD5_MATCH: match[PATH]}}
                    )
                else:
                    db_target.photos.update(
                        {PATH: record[PATH]},
                        {'$set': {SIG_MATCH: match[PATH]}}
                    )
    print "Done finding duplicates."


def print_duplicates(db_archive, db_target, top_archive=None, top_target=None):
    find_duplicates(db_archive, db_target, top_archive, top_target)
    regex = make_tree_regex(top_target)
    unique_records = db_target.photos.find(
        {
            PATH: regex,
            MD5_MATCH: {'$exists': False},
            SIG_MATCH: {'$exists': False}
        }
    )
    md5_records = db_target.photos.find(
        {
            PATH: regex, MD5_MATCH: {'$exists': True}
        }
    )
    signature_records = db_target.photos.find(
        {
            PATH: regex, SIG_MATCH: {'$exists': True}
        }
    )
    print("Match status for {}".format(top_target))
    for unique_record in unique_records:
        print "Unique: {}".format(unique_record[PATH])
    for md5_record in md5_records:
        print "MD5 match: {} matches {}".format(md5_record[PATH], md5_record[MD5_MATCH])
    for signature_record in signature_records:
        print "Signature match: {} matches {}".format(
            signature_record[PATH], signature_record[SIG_MATCH])


def print_duplicates_tree(db_archive, db_target, top_archive=None, top_target=None):
    """Print tree from 'top' indenting each level and showing duplicate status"""
    find_duplicates(db_archive, db_target, top_archive, top_target)
    offset = tree_depth(top_target)
    os_type = find_os_from_path_string(top_target)
    if os_type == LINUX:
        basename = posixpath.basename
    if os_type == WINDOWS:
        basename = ntpath.basename
    indent = 4  # Number of spaces to indent printout
    photo_count = db_target.photos.find({PATH: make_tree_regex(top_target)}).count()
    if photo_count is None:
        print "No files found starting at {}".format(top_target)
        return
    else:
        print "Walking tree from: {}, {} nodes found.".format(top_target, photo_count)
        for dirpath, _unused_dirs, files in walk_db_tree(db_target.photos, top_target):
            print '{}[{}]'.format(' ' * indent * (tree_depth(dirpath) - offset), basename(dirpath))
            for filepath in files:
                record = db_target.photos.find_one({PATH: filepath})
                if MD5_MATCH not in record and SIG_MATCH not in record:
                    print '{}Unique: {}'.format(
                        ' ' * indent * (tree_depth(filepath) - offset), basename(filepath))
                else:
                    if MD5_MATCH in record:
                        print '{}MD5 match: {} matches {}'.format(
                            ' ' * indent * (tree_depth(filepath) - offset),
                            basename(filepath), record[MD5_MATCH])
                    elif SIG_MATCH in record:
                        print '{}Sig match: {} matches {}'.format(
                            ' ' * indent * (tree_depth(filepath) - offset),
                            basename(filepath), record[SIG_MATCH])


def walk_db_tree(collection, top, topdown=True):
    record = collection.find_one(
        {PATH: top},
        {
            '_id': False,
            DIRPATHS: True,
            FILEPATHS: True
        }
    )
    if record is None:
        raise IndexError('No files found for db tree walk at {}'.format(top))
    if topdown:
        yield top, record[DIRPATHS], record[FILEPATHS]
    for dirs in record[DIRPATHS]:
        for walk_iterator in walk_db_tree(collection, dirs, topdown):
            yield walk_iterator
    if not topdown:
        yield top, record[DIRPATHS], record[FILEPATHS]


def tree_depth(path):
    os_type = find_os_from_path_string(path)
    if os_type == LINUX:
        return path.count('/')
    if os_type == WINDOWS:
        return path.count('\\')
    if os_type is None:
        return 0


def print_tree(database, top):
    """Print tree from 'top' indenting each level"""
    offset = tree_depth(top)
    os_type = find_os_from_path_string(top)
    if os_type == LINUX:
        basename = posixpath.basename
    if os_type == WINDOWS:
        basename = ntpath.basename
    indent = 4  # Number of spaces to indent printout
    photo_count = database.photos.find({PATH: make_tree_regex(top)}).count()
    if photo_count is None:
        print "No files found starting at {}".format(top)
        return
    else:
        print "Walking tree from: {}, {} nodes found.".format(top, photo_count)
        for dirpath, _unused_dirs, files in walk_db_tree(database, top):
            print '{}{}'.format(' ' * indent * (tree_depth(dirpath) - offset), basename(dirpath))
            for f in files:
                print '{}{}'.format(' ' * indent * (tree_depth(f) - offset), basename(f))


class TreeStats():
    def __init__(self, database, top):
        """
        Compute key statistics of collection
        :param database:
        :param top:
        """
        self.database = database
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
        self.total_nodes = self.database.photos.find({PATH: self.top_regex}).count()
        self.total_dirs = self.database.photos.find({PATH: self.top_regex, ISDIR: True}).count()
        self.total_files = self.database.photos.find({PATH: self.top_regex, ISDIR: False}).count()
        self.tagged_records = self.database.photos.find(
            {
                PATH: self.top_regex,
                ISDIR: False,
                USER_TAGS: {'$ne': []}
            }
        ).count()
        self.signatures_iter = self.database.photos.aggregate([
            {"$match": {
                PATH: self.top_regex,
                ISDIR: False,
                SIGNATURE: {"$ne": ""}
                }
             },
            {"$group": {"_id": "$signature"}}])
        if self.signatures_iter['ok'] != 1:
            raise RuntimeError('Mongodb return code not=1.  Got: {}'.format(
                self.signatures_iter['ok']))
        self.unique_signatures = len(self.signatures_iter['result'])
        self.md5_iter = self.database.photos.aggregate([
            {
                "$match": {
                    PATH: self.top_regex,
                    ISDIR: False,
                    MD5: {"$ne": ""}
                }
            },
            {
                "$group": {
                    "_id": "$md5"
                }
            }
        ])
        if self.md5_iter['ok'] != 1:
            raise RuntimeError('Mongodb return code not=1.  Got: {}'.format(
                self.signatures_iter['ok']))
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
        print(
            "Total tagged: {}, {:.1%} of files".format(self.tagged_records,
                                                       float(self.tagged_records) / self.total_files))
        print("Unique signatures={}, {} duplicates ({:.1%})".format(self.unique_signatures,
                                                                    self.total_files - self.unique_signatures,
                                                                    1.0 - float(
                                                                        self.unique_signatures) / self.total_files))
        print("Unique MD5s={}, {} duplicates ({:.1%})".format(self.unique_md5s, self.total_files - self.unique_md5s,
                                                              1.0 - float(self.unique_md5s) / self.total_files))


# def move_files(safe_top, path):
# #Test for OS here
# if _LINUX:
# #if dir does not exist:
# #    create dir
# #move file to safe_top\path
# elif WIN:
# #if dir does not exist
# #    create dir
# #move file to safe_top\path
# else:
# print "***FAIL*** Could not determine OS."

# def find_md5_match_dirs(host, top):
# db, config, photos=set_up_db(host)
# tree_regex=make_tree_regex(top)
# dir_records=photos.find({_ISDIR: True, _DIRPATHS: empty, _FILEPATHS: not empty}, {'_id': False, _PATH: True})
#     for dir_record in dir_records:
#         photos.find({_ISDIR: False, _PATH: dir_record[_PATH]})


def find_hybrid_dirs(database, top):  # Broken
    # Look for dirs that have files and dirs in them (this shouldn't happen if the photo directory is clean)
    #    hybridlist=database.find({'$and': [{'$not': {_FILEPATHS: '[]'}},
    #       {'$not': {_DIRPATHS: '[]'}}]}, {_PATH: 1, _FILEPATHS: 1, _DIRPATHS: 1})
    tree_regex = make_tree_regex(top)
    hybridlist = database.photos.find(
        {
            ISDIR: True,
            PATH: tree_regex,
            FILEPATHS: {'$ne': []},
            DIRPATHS: {'$ne': []}
        },
        {
            '_id': 0,
            PATH: 1,
            FILEPATHS: 1,
            DIRPATHS: 1
        }
    )
    return hybridlist


def print_hybrid_dirs(db, top):
    hybridlist = find_hybrid_dirs(db, top)
    print "Finding dirs that contain both files and directories.  Length of hybrid list:", hybridlist.count()
    for hybrids in hybridlist:
        print "#{} # File list {}, Dir list {}".format(hybrids[PATH], hybrids[FILEPATHS], hybrids[DIRPATHS])
    print "#--Done finding hybrid directories"


class PhotoDb(object):
    def __init__(self, repository, top, host='localhost', create_new=False):
        self.root = top
        self.start_time = 0  # Persistent variable for method in this class because I don't know a better way
        self.database = set_up_db(repository, host, create_new)
        self.photos = self.database.photos
        self.config = self.database.config
        check_host(self.config)
        if not create_new:
            check_db_clean(self.config)
        self.sync_db()

    def sync_db(self, top=None):
        """
        Sync contents of database with filesystem starting at 'top'
        """
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
        top = self.root if top is None else top
        time.sleep(1)  # Wait a second to make double-dog sure mongodb is caught up
        most_recent_records = self.config.find().sort(FS_TRAVERSE_TIME, pymongo.DESCENDING).limit(10)
        if most_recent_records.count() < 1:
            error_message = 'ERROR: no log records of filesystem scan available.\
              Too dangerous to continue.'
            logging.error(error_message)
            raise (ValueError, error_message)
        if most_recent_records[0][TRAVERSE_PATH] != top:
            error_message = 'ERROR: path being pruned "{}" does not match most recent database filesystem sync with {}' \
                .format(top, most_recent_records[0][TRAVERSE_PATH])
            logging.error(error_message)
            raise (ValueError, error_message)
        fresh_time = most_recent_records[0][FS_TRAVERSE_TIME]
        regex = make_tree_regex(top)
        records = self.photos.find({PATH: regex, REFRESH_TIME: {'$lt': fresh_time}})
        for record in records:
            remove_path = record[PATH]
            logging.info("Removing records for node: {}".format(remove_path))
            rm_stat = self.photos.remove({PATH: remove_path})
            if rm_stat['ok'] != 1:
                error_message = "Error - expected to remove record {} from database but database returned " \
                                "following: {}".format(remove_path, rm_stat)
                print(error_message)
                logging.error(error_message)

    def _mark_db_status(self, status):
        if status == 'dirty':
            self.config.insert({
                DB_STATE: 'dirty',
                FS_TRAVERSE_TIME: time_now(),
                TRAVERSE_PATH: self.root
            })
        elif status == 'clean':
            self.config.insert({
                DB_STATE: 'clean',
                FS_TRAVERSE_TIME: time_now(),
                TRAVERSE_PATH: self.root
            })
        else:
            error_message = "Error: bad database status received.  Got: '{}'".format(status)
            print error_message
            logging.error(error_message)
            sys.exit(1)

    @staticmethod
    def _walk_error(walk_err):
        print "Error {}:{}".format(walk_err.errno,
                                   walk_err.strerror)  # TODO: Maybe some better error trapping here...and do we
        #  need to encode strerror if it might contain unicode??
        raise

    def _update_file_record(self, filepath):
        file_stat = stat_node(filepath)
        db_file = self.photos.find_one({PATH: filepath}, {'_id': 0})
        if db_file is None:
            logging.debug("New File detected: {0}".format(repr(filepath)))
            self.photos.insert(
                {
                    PATH: filepath,
                    ISDIR: False,
                    SIZE: file_stat.st_size,
                    MTIME: file_stat.st_mtime,
                    'in_archive': True,
                    'refresh_time': time_now()
                }
            )
        elif db_file[SIZE] != file_stat.st_size or db_file[MTIME] != file_stat.st_mtime:
            logging.debug("File change detected: {}".format(repr(filepath)))
            self.photos.update(
                {PATH: db_file[PATH]},
                {
                    '$set': {
                        ISDIR: False,
                        SIZE: file_stat.st_size,
                        MD5: '',
                        SIGNATURE: '',
                        MTIME: file_stat.st_mtime,
                        'timestamp': datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S'),
                        'got_tags': False,
                        USER_TAGS: [],
                        'in_archive': True,
                        'refresh_time': time_now()
                    }
                }
            )
        else:
            self.photos.update(
                {PATH: db_file[PATH]},
                {'$set': {'refresh_time': time_now()}}
            )

    def _update_dir_record(self, dirpath, dirpaths, filepaths):
        db_dir = self.photos.find_one({PATH: dirpath}, {PATH: True, '_id': False})
        if db_dir is None:
            logging.debug("New directory detected: {0}".format(repr(dirpath)))
            db_dir = dirpath
        self.photos.update(
            {PATH: dirpath},
            {
                PATH: dirpath,
                ISDIR: True,
                DIRPATHS: dirpaths,
                FILEPATHS: filepaths,
                'in_archive': True,
                'refresh_time': time_now()
            },
            upsert=True)  # Replace existing record - TODO: wipe out previous sum data, if the record exists

    def _traverse_fs(self, top=None):
        top = self.root if top is None else top
        logging.info("Traversing filesystem tree starting at {}...".format(top))
        if os.path.isfile(top):
            self._update_file_record(top)
        else:
            for dirpath, dirnames, filenames in os.walk(top, onerror=self._walk_error):
                dirpath = os.path.normpath(dirpath)
                dirpaths = \
                    [os.path.normpath(os.path.join(dirpath, dirname)) for dirname in dirnames]
                filepaths = \
                    [os.path.normpath(os.path.join(dirpath, filename)) for filename in filenames]
                self._update_dir_record(dirpath, dirpaths, filepaths)
                for filepath in filepaths:
                    self._update_file_record(filepath)
        logging.info("Done traversing filesystem tree.")
        return

    def _update_md5(self, top=None):
        logging.info("Computing missing md5 sums...")
        top = self.root if top is None else top
        regex = make_tree_regex(top)
        files = self.photos.find(
            {
                '$and': [
                    {
                        PATH: regex
                    },
                    {
                        ISDIR: False
                    },
                    {
                        '$or': [
                            {
                                MD5: {'$exists': False}
                            },
                            {
                                MD5: ''
                            }
                        ]
                    },

                ]
            },
            {
                '_id': False,
                PATH: True
            },
            timeout=False
        )
        logging.info("Number of files for MD5 update: {}".format(files.count()))
        for n, path in enumerate(files, start=1):
            logging.info('Computing MD5 {} for: {}'.format(n, repr(path[PATH])))
            md5sum = MD5sums.fileMD5sum(path[PATH])
            self.photos.update({PATH: path[PATH]}, {'$set': {MD5: md5sum}})
        logging.info("Done computing missing md5 sums.")

    def _update_tags(self, top=None):
        logging.info('Updating file tags...')
        if top is None:
            top = self.root
        tree_regex = make_tree_regex(top)
        files = self.photos.find(
            {
                PATH: tree_regex,
                ISDIR: False,
                '$or': [
                    {'got_tags': {'$exists': False}},
                    {'got_tags': False}
                ]
            },
            {
                '_id': False, PATH: True
            },
            timeout=False)
        total_files = files.count()
        for file_count, photo_record in enumerate(files, start=1):
            self._monitor_tag_progress(total_files, file_count)
            photopath = photo_record[PATH]
            tags = get_metadata_from_file(photopath)
            if tags is None:
                # logging.warn("Bad tags in: {0}".format(repr(photopath)))
                user_tags = []
                timestamp = datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S')
            else:
                user_tags = get_user_tags_from_metadata(tags)
                timestamp = get_timestamp_from_metadata(tags)
            signature = self._get_file_signature(tags, photopath)  # Get signature should now do the thumbnailMD5
            # and if not find another signature.
            self.photos.update(
                {PATH: photopath},
                {'$set': {
                    USER_TAGS: user_tags,
                    'timestamp': timestamp,
                    SIGNATURE: signature,
                    'got_tags': True
                    }
                 }
            )
        logging.info('Done updating file tags.')
        return ()

    def _monitor_tag_progress(self, total_files, file_count):
        progress_count = 500  # How often to report progress in units of files
        if file_count == 1:  # First call; initialize
            self.start_time = time_now()
            logging.info("Extracting tags for {0}.  File count={1}".format(repr(self.root), total_files))
        if not file_count % progress_count:
            elapsed_time = time_now() - self.start_time
            total_time_projected = float(elapsed_time) / float(file_count) * total_files
            time_remaining = float(elapsed_time) / float(file_count) * float(total_files - file_count)
            logging.info("{0} of {1}={2:.2f}%, {3:.1f} seconds, time remaining: {4} of {5}".
                         format(file_count, total_files, 1.0 * file_count / total_files * 100.0, elapsed_time,
                                str(datetime.timedelta(seconds=time_remaining)),
                                str(datetime.timedelta(seconds=total_time_projected))))
        if file_count == total_files:
            elapsed_time = time_now() - self.start_time
            logging.info(
                "Tags extracted: {0}.  Elapsed time: {1:.0g} seconds or {2:.0g} ms per file={3:.0g} "
                "for 100k files".format(file_count, elapsed_time, elapsed_time / total_files * 1000.0,
                                        elapsed_time / total_files * 100000.0 / 60.0))
        return

    def _get_file_signature(self, metadata, filepath):
        text_files = ['.ini', '.txt']  # file types to be compared ignoring CR/LF
        # for OS portability.  Use lower case (extensions
        # will be lowered before comparison)
        if metadata is not None and len(metadata.previews) > 0:
            signature = MD5sums.stringMD5sum(metadata.previews[0].data)
        else:
            if str.lower(os.path.splitext(filepath)[1].encode()) in text_files:
                signature = MD5sums.text_file_MD5_signature(filepath)
            else:
                record = self.photos.find_one({PATH: filepath})
                if MD5 in record:
                    signature = record[MD5]
                else:
                    logging.warning("MD5 was missing on this record.  Strange...")
                    signature = MD5sums.fileMD5sum(filepath)
        return signature

# --------------------------------------------

# Stole this main from Guido van van Rossum at http://www.artima.com/weblogs/viewpost.jsp?thread=4829
# class Usage(Exception):
#    def __init__(self, msg):
#        self.msg='''Usage:  get_photo_data [-p pickle_path] [-l log_file] photos_path'''
#
# def main(argv=None):
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
