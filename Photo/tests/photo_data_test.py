import photo_data
import os
import os.path
import datetime



class test_scans_photos:

    def __init__(self):
        self.photos = None
        basepath = os.getcwd()
        self.photo_path = os.path.join(basepath, 'test_photos')
        
        #Make handy references for photo files
        unique_1_tags_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_1_tags_date.JPG'))
        unique_2_no_tags_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_2_no_tags_date.JPG'))
        unique_3_no_tags_no_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_3_no_tags_no_date.jpg'))
        file_no_thumbnail_no_tags = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','file_no_thumbnail_no_tags'))
        
        self.test_instantiation()

    def test_instantiation(self):
        self.photos = photo_data.PhotoData(photo_path)
        assert isinstance(self.photos, photo_data.PhotoData)
        
    def test_tree(self):
        #Make sure directory tree was fully descended
        files = []
        for (dirpath, dirnames, filenames) in os.walk(photo_path):
            for file in filenames:
                files.append(os.path.join(dirpath, file))
        #File names not necessarily represented the same way in different OS, so you can't just check if key is in photo.node.keys().  So create your own lists.
        scanned_files = [os.path.normpath(x) for x in self.photos.node.keys()]
    
        for file in files:
            assert os.path.normpath(file) in scanned_files
            
    def test_tags_extracted(self):    
        #Check tags extracted (and not if no tags)
        assert 'test tag 1' in self.photos[unique_1_tags_date].user_tags, "Missed test tag 1"
        assert 'test tag 2' in self.photos[unique_1_tags_date].user_tags, "Missed test tag 2"
        assert self.photos[unique_2_no_tags_date].user_tags is None, "Improperly extracted tags"
        
    def test_dates_extracted(self):
        #Check dates extracted correctly
    #    assert photos[unique_3_no_tags_no_date].timestamp == datetime.datetime.strptime('1700:1:1 00:00:00', '%Y:%m:%d %H:%M:%S'), "Missing timestamp extraction incorrect"
        assert self.photos[unique_1_tags_date].timestamp == datetime.datetime.strptime('2012:11:23 17:21:30', '%Y:%m:%d %H:%M:%S'), "Incorrect timestamp extraction"
        
    def test_thumb_file_checksums(self):        
        #Check thumb and file checksums
        assert self.photos[unique_1_tags_date].signature == '61cc239581f693059b496202e5e50b73'
        assert self.photos[unique_1_tags_date].md5 == 'ef1f34077aa82b009d7f58b6a84677fa'
        
    def test_no_tags(self):
        #Check thumb checksum if no thumbnail
        assert self.photos[file_no_thumbnail_no_tags].signature == 'd88d35b8106931ba26c631c7bfe3ce48'  #Thumb signature same as file signature - something is wrong....
            