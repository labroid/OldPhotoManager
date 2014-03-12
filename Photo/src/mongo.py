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
        
#        for trash in photos.emit_records():
#            print trash
        
        client = pymongo.MongoClient()
        db = client.photo_database
        collection = db.photo_archive
        
#Populate the database
#        collection.insert(photos.emit_records())

        print "Quick test to find non-null signatures"
        cursor = collection.find( {"signature": {"$ne":""}} )
        for line in cursor:
            print line
        
        print "Do an aggregation pipeline"
        cursor = collection.aggregate([
                                              { "$project" : {
                                                              "signature" : 1,
                                                              "node" : 1
                                                              }
                                               },
                                               { "$match" : {
                                                            "signature" : { "$ne" : "" } 
                                                            }
                                               },
                                               {"$group" : {
                                                           "_id" : "$signature",
                                                            "nodeSet" : { "$push" : "$node"},
                                                           "nodeCount" : {"$sum" : 1}
                                                           }
                                                },
                                                 {"$match" : {
                                                              "nodeCount" : { "$gt" : 1}
                                                              }
                                                  }
                                              ])

        print type(cursor)
        print type(cursor['result'])
        for line in cursor['result']:
            print line
            
        print "Now for mapreduce..."
        
        from bson.code import Code
        mapper = Code("""
                       function () {
                           emit(this.signature, this.path);
                       }
                       """)
        reducer = Code("""
                function (key, values) {
                    var answer = [];
                    answer = values[0]
                    return {"hello":answer};
                    }
                """)

        result = db.photo_archive.map_reduce(mapper, reducer, "results", full_response=True)
        print result
        for doc in result:
            print doc, result[doc]
        print type(result)
        print type(result['result'])
        print "Done"
        


if __name__ == '__main__':
    main()