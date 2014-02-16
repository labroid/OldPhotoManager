'''
Created on Feb 15, 2014

@author: scott_jackson
'''

import photo_data
import pickle_manager
import os
import os.path
import datetime
import pytest
import photo_functions
import pymongo

def main():
    
        photo_path = os.path.join("c:/Users/scott_jackson/git/PhotoManager/Photo/tests/test_photos")
        print("basepath = {}".format(photo_path))
        photos = photo_data.PhotoData(photo_path)
        
        #Make handy references for photo files
        unique_1_tags_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_1_tags_date.JPG'))
        unique_2_no_tags_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_2_no_tags_date.JPG'))
        unique_3_no_tags_no_date = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','unique_3_no_tags_no_date.jpg'))
        file_no_thumbnail_no_tags = os.path.normpath(os.path.join(os.getcwd(),'test_photos','archive','uniques','file_no_thumbnail_no_tags'))
        
        for trash in photos.json_records():
            print trash
        
#         db = pymongo.MongoClient().photos
#         db.


if __name__ == '__main__':
    main()