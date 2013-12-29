import photo_data
import os
import os.path
import datetime.datetime

def test_instantiation():
    x = photo_data.photo_collection()
    assert isinstance(x, photo_data.photo_collection)
        
def test_scans_photos():
    basepath = os.getcwd()
    photo_path = os.path.join(basepath, 'test_photos')
    photos = photo_data.photo_data(node_path=photo_path)

    #Make sure directory tree was fully descended
    files = []
    for (dirpath, dirnames, filenames) in os.walk(photos):
        for file in filenames:
            files.append(os.path.join(dirpath, file))
    for file in files:
        assert file in photos

    #Make handy references for photo files
    unique_1_tags_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_1_tags_date'))
    
    #Check tags extracted (and not if no tags)
    assert 'test tag 1' in photos[unique_1_tags_date].userTags, "Missed test tag 1"
    assert 'test tag 2' in photos[unique_1_tags_date].userTags, "Missed test tag 2"
    assert len(photos[unique_2_no_tags_date]) == 0, "Improperly extracted tags"
    
    #Check dates extracted correctly
    assert photos[unique_3_no_tags_no_date].timestamp == datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S'), "Missing timestamp extraction incorrect"
    assert photos[unique_1_tags_date].timestamp == datetime.datetime.strptime('2012:11:23 17:21:30', '%Y:%m:%d %H:%M:%S'), "Incorrect timestamp extraction"
    
    #Check thumb and file checksums
    assert photos[unique_1_tags_date].signature == '61cc239581f693059b496202e5e50b73'
    assert photos[unique_1_tags_date].md5 == 'ef1f34077aa82b009d7f58b6a84677fa'
    
    #Check thumb checksum if no thumbnail
    assert photos[file_no_thumbnail_no_tags].signature == 'd88d35b8106931ba26c631c7bfe3ce48'  #Thumb signature same as file signature - something is wrong....
        