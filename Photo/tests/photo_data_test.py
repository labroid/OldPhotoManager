# pylint: disable=line-too-long

import re
import logging
import datetime

import pymongo

from photodb import photo_data


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
#test_photos_base = os.path.join(os.getcwd(), 'test_photos')  # TODO: Make this portable!
test_photos_base = "C:\\Users\\scott_jackson\\git\\PhotoManager\\Photo\\tests\\test_photos"
database = None


def get_photo(name):
    photo = database.photos.find_one({'path': re.compile(name)})
    return(photo)


def test_db_setup():
    global database
    database = photo_data.set_up_db(repository, host, create_new=True)
    assert isinstance(database, pymongo.database.Database)
#  TODO put these back in the real test suite; right now they take too long
#    with pytest.raises(ValueError):
#        photo_data.set_up_db(None, None, None)
#    with pytest.raises(ValueError):
#        photo_data.set_up_db("missing","incorrect", create_new=False)


def test_photo_data_instantiation():
    photodata = photo_data.PhotoDb(repository, test_photos_base, host, create_new=True)
    assert isinstance(photodata, photo_data.PhotoDb)


def test_collections_present():
    collections = database.collection_names()
    assert 'photos' in collections
    assert 'config' in collections


def test_tree_stats():
    stats = photo_data.TreeStats(database, test_photos_base)
    stats.print_tree_stats()
    assert(stats.total_nodes == 37)
    assert(stats.total_dirs == 13)
    assert(stats.total_files == 24)
    assert(stats.tagged_records == 14)
    assert(stats.unique_signatures == 16)
    assert(stats.unique_md5s == 17)


def test_tags_extracted():
    user_tags = get_photo('unique_1_tags_date')['user_tags']
    assert set(['test tag 1', 'test tag 2', 'SJJ Frame']) == set(user_tags)
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


def test_signature():
    sample_signature = get_photo('unique_1_tags_date.JPG')['signature']
    assert sample_signature == '61cc239581f693059b496202e5e50b73'


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
    photo_data.find_and_set_duplicates(database, database,
                               test_photos_base + "\\archive", test_photos_base + "\\target")
    md5records = database.photos.find({'md5_match': {'$exists': True}})
    assert md5records.count() == 4
    md5files = [re.search('.*[\\\\/](.*$)', x['path']).group(1) for x in md5records]
    assert set(
               [".picasa.ini", "dup_dir_pic_1.jpg", "dup_dir_pic_2.jpg", "target_duplicate_2.JPG"]
               ) == set(md5files)
    sig_records = database.photos.find({'sig_match': {'$exists': True}})
    sigfiles = [re.search('.*[\\\\/](.*$)', x['path']).group(1) for x in sig_records]
    assert set(["target_same_as_unique_1_but_tags_different.JPG"]) == set(sigfiles)


def test_find_hybrid_dirs():
    records = photo_data.find_hybrid_dirs(database, test_photos_base)
    print type(records)
    for record in records:
        assert 'duplicates' in record['path']
        #Imperfect test as 'duplicates' could be in other paths, but adding to path is complicated with OS differences


def test_dirs_with_no_tags():
    no_tags = photo_data.dirs_by_no_tags(database, test_photos_base)
    assert len(no_tags) == 2
