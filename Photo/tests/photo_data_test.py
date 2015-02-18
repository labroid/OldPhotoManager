# pylint: disable=line-too-long

import re
import logging
import datetime
import pymongo
import photo_data

log_file = "C:\Users\scott_jackson\Documents\Personal\Programming\lap_log.txt"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(message)s"
logging.basicConfig(
                    filename=log_file,
                    format=LOG_FORMAT,
                    level=logging.DEBUG,
                    filemode='w'
                    )

host = 'localhost'
repository = 'test_photos'
#test_photos_base = os.path.join(os.getcwd(), 'test_photos')
test_photos_base = "C:\\Users\\scott_jackson\\git\\PhotoManager\\Photo\\tests\\test_photos"
database = ''


def get_photo(name):
    photo = database.photos.find_one({'path': re.compile(name)})
    return(photo)


def test_db_setup():
    global database
    database = photo_data.set_up_db(repository, host, create_new=True)
    assert isinstance(database, pymongo.database.Database)
    #assert type(database) == type(pymongo.MongoClient()['test'])
#  TODO put these back in the real test suite; right now they take too long
#    with pytest.raises(ValueError):
#        photo_data.set_up_db(None, None, None)
#    with pytest.raises(ValueError):
#        photo_data.set_up_db("missing","incorrect", create_new=False)


def test_photo_data_instantiation():
    photodata = photo_data.PhotoDb(repository, test_photos_base, host, create_new=True)
    assert isinstance(photodata, photo_data.PhotoDb)
    #Add tests for database failures (e.g. test that bad host or database names fail appropriately)


def test_collections_present():
    collections = database.collection_names()
    assert 'photos' in collections
    assert 'config' in collections


def test_tree_stats():
    stats = photo_data.TreeStats(database, test_photos_base)
    stats.print_tree_stats()
    assert(stats.total_nodes == 38)
    assert(stats.total_dirs == 13)
    assert(stats.total_files == 25)
    assert(stats.tagged_records == 14)
    assert(stats.unique_signatures == 17)
    assert(stats.unique_md5s == 18)


def test_tags_extracted():
    user_tags = get_photo('unique_1_tags_date')['user_tags']
    assert 'test tag 1' in user_tags, "Missed test tag 1"
    assert 'test tag 2' in user_tags, "Missed test tag 2"
    no_tags = get_photo('unique_2_no_tags_date')['user_tags']
    assert no_tags == [], "Improperly extracted user_tags.  Got {}".format(no_tags)


def test_dates_extracted():
    sample_date = get_photo('unique_1_tags_date.JPG')['timestamp']
    assert sample_date == datetime.datetime(2012, 11, 23, 17, 21, 30)
    no_date = get_photo('unique_3_no_tags_no_date.jpg')['timestamp']
    assert no_date == datetime.datetime(1800, 1, 1, 0, 0)


def test_sizes_extracted():
    sample_size = get_photo('unique_1_tags_date.JPG')['size']
    assert sample_size == 153493
    zero_size = get_photo('empty_photo.jpg')['size']
    assert zero_size == 0


def test_photo_md5s():
    sample_md5 = get_photo('unique_1_tags_date.JPG')['md5']
    assert sample_md5 == 'c77645d3ec1a43abc0f9bac4f9140ee4'


def test_find_empty_files():
    empty_files = photo_data.find_empty_files(database, test_photos_base)
    assert(empty_files.count() == 1)
    assert('empty_photo.jpg' in empty_files[0]['path'])


def test_find_empty_dirs():
    empty_dirs = photo_data.find_empty_dirs(database, test_photos_base)
    assert(empty_dirs.count() == 1)
    assert('empty_dir' in empty_dirs[0]['path'])


def test_extract_frame_set(capsys):
    photo_data.extract_picture_frame_set(database, test_photos_base, "SJJ Frame", 'test')
    out, err = capsys.readouterr()
    assert err == ''
    assert 'unique_1_tags_date.JPG' in out


def test_find_unexpected_files():
    records = photo_data.find_unexpected_files(database, test_photos_base)
    print type(records)
    assert len(records) == 1
    assert 'Strange file.dog' in records[0]['path']


def test_find_duplicates():
    photo_data.find_duplicates(database, database, test_photos_base + "\\archive", test_photos_base + "\\target")
    md5records = database.photos.find({'md5_match': {'$exists': True}})
    print "Done.  Printing {} md5records".format(md5records.count())
    for record in md5records:
        print "MD5 match: {}\n{}".format(record['path'], record['md5_match'])
    sig_records = database.photos.find({'sig_match': {'$exists': True}})
    for record in sig_records:
        print "sig match: {}\n{}".format(record['path'], record['sig_match'])
    assert False


def test_find_hybrid_dirs():
    records = photo_data.find_hybrid_dirs(database, test_photos_base)
    print type(records)
    for record in records:
        assert 'duplicates' in record['path']
        #Imperfect test as 'duplicates' could be in path, but adding to path is complcated with OS differences




# class TestPhotoData(object):
#
#     def setup_class(self):
#         self.test_photos = None
#         self.basepath = os.path.join(os.getcwd(), 'test_photos')
# #        #Make handy references for photo files
# #         self.unique_1_tags_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_1_tags_date.JPG'))
# #         self.unique_2_no_tags_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_2_no_tags_date.JPG'))
# #         self.unique_3_no_tags_no_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_3_no_tags_no_date.jpg'))
# #         self.file_no_thumbnail_no_tags = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','file_no_thumbnail_no_tags'))
# 
#     def test_tree(self):
#         #Make sure directory tree was fully descended
#         files = []
#         for (dirpath, dirnames, filenames) in os.walk(self.photo_path):
#             for file in filenames:
#                 files.append(os.path.join(dirpath, file))
#         #File names not necessarily represented the same way in different OS, so you can't just check if key is in photo.node.keys().  So create your own lists.
#         scanned_files = [os.path.normpath(x) for x in self.photos.node.keys()]
#     
#         for file in files:
#             assert os.path.normpath(file) in scanned_files
#             

#         
#     def test_dates_extracted(self):
#         #Check dates extracted correctly
#     #    assert photos[unique_3_no_tags_no_date].timestamp == datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S'), "Missing timestamp extraction incorrect"
#         assert self.photos[self.unique_1_tags_date].timestamp == datetime.datetime.strptime('2012:11:23 17:21:30', '%Y:%m:%d %H:%M:%S'), "Incorrect timestamp extraction"
#         
#     def test_thumb_file_checksums(self):        
#         #Check thumb and file checksums
#         assert self.photos[self.unique_1_tags_date].signature == '61cc239581f693059b496202e5e50b73'
#         assert self.photos[self.unique_1_tags_date].md5 == 'ef1f34077aa82b009d7f58b6a84677fa'
#         
#     def test_no_tags(self):
#         #Check thumb checksum if no thumbnail
#         assert self.photos[self.file_no_thumbnail_no_tags].signature == 'd88d35b8106931ba26c631c7bfe3ce48'  #Thumb signature same as file signature - something is wrong....
#     
#     def test_pickler(self):
#         photo_dir = "C:/Users/scott_jackson/git/PhotoManager/Photo/tests/test_photos"
#         pickle_file = "C:/Users/scott_jackson/git/PhotoManager/Photo/tests/test_photos_pickle"
#         log_file = "C:\\Users\\scott_jackson\\Documents\\Programming\\PhotoManager\\lap_log.txt"
#         pickle = pickle_manager.PhotoPickler(pickle_file)
#         pickle.dump_pickle(self.photos)
#         
# #Could also test other PhotoData methods, such as list zero files, print functions...but save those since PhotoData will change with the file storage method eventually used in the cloud
# 
# #Tests of PhotoFunctions
# 
#     def test_photo_function_instance(self):
#         results = photo_functions.PhotoFunctions(self.photos, compare_method = 'not self')
#         assert isinstance(self.results, photo_functions.PhotoFunctions)        
# 
#             