import photo_data
import os
import os.path
import datetime

photos = None #Holder for eventual PhotoData instance, made global for access by test modules after instantiation
basepath = os.getcwd()
photo_path = os.path.join(basepath, 'test_photos')

def test_instantiation():
    global photos
    photos = photo_data.PhotoData(photo_path)
    assert isinstance(photos, photo_data.PhotoData)
        
def test_scans_photos():
    global photos
    #Make sure directory tree was fully descended
    files = []
    for (dirpath, dirnames, filenames) in os.walk(photo_path):
        for file in filenames:
            files.append(os.path.join(dirpath, file))
    #File names not necessarily represented the same way in different OS, so you can't just check if key is in photo.node.keys().  So create your own lists.
    scanned_files = [os.path.normpath(x) for x in photos.node.keys()]
    
    for file in files:
        assert os.path.normpath(file) in scanned_files

    #Make handy references for photo files
    unique_1_tags_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_1_tags_date.JPG'))
    unique_2_no_tags_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_2_no_tags_date.JPG'))
    unique_3_no_tags_no_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_3_no_tags_no_date.jpg'))
    file_no_thumbnail_no_tags = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','file_no_thumbnail_no_tags'))

    #Check tags extracted (and not if no tags)
    assert 'test tag 1' in photos[unique_1_tags_date].user_tags, "Missed test tag 1"
    assert 'test tag 2' in photos[unique_1_tags_date].user_tags, "Missed test tag 2"
    assert photos[unique_2_no_tags_date].user_tags is None, "Improperly extracted tags"
    
    #Check dates extracted correctly
#    assert photos[unique_3_no_tags_no_date].timestamp == datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S'), "Missing timestamp extraction incorrect"
    assert photos[unique_1_tags_date].timestamp == datetime.datetime.strptime('2012:11:23 17:21:30', '%Y:%m:%d %H:%M:%S'), "Incorrect timestamp extraction"
    
    #Check thumb and file checksums
    assert photos[unique_1_tags_date].signature == '61cc239581f693059b496202e5e50b73'
    assert photos[unique_1_tags_date].md5 == 'ef1f34077aa82b009d7f58b6a84677fa'
    
    #Check thumb checksum if no thumbnail
    assert photos[file_no_thumbnail_no_tags].signature == 'd88d35b8106931ba26c631c7bfe3ce48'  #Thumb signature same as file signature - something is wrong....
        