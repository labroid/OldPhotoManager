import re
import time
import pymongo
from py._path.local import PosixPath
import posixpath



def find_empty_files():
    #Look for empty files
    emptylist = db.find({'isdir' : False, 'size' : long(0)})
    print "Empty count", emptylist.count()
    for empty in emptylist:
        print "#rm {} # Size {}".format(empty['path'], empty['size'])
    print "#--Done finding empty files---"

def find_empty_dirs():    
    #Look for empty dirs
    emptylist = db.find({'isdir' : True, 'filepaths' : [], 'dirpaths' : []},{'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    for empty in emptylist:
        print "#rmdir {} # File list {}".format(empty['path'], empty['filepaths'])
    print "#--Done finding empty dirs---"
        
def find_hybrid_dirs():
    #Look for dirs that have files and dirs in them (this shouldn't happen if the photo directory is clean)
#    hybridlist = db.find({'$and': [{'$not' : {'filepaths' : '[]'}}, {'$not' : {'dirpaths' : '[]'}}]}, {'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    hybridlist = db.find({'filepaths' : {'$ne' : '[]'}}, {'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    print "Length of hybrid list:", hybridlist.count()
    for hybrids in hybridlist:
        print "#rmdir {} # File list {}, Dir list {}".format(hybrids['path'], hybrids['filepaths'], hybrids['dirpaths'])
    print "#--Done finding hybrid dirs---"
    
def photos_with_no_tags():  #Possibly useful function to create
    pass

def create_md5_dict():
    print "Do an aggregation pipeline"
    cursor = db.aggregate([
                                          { "$project" : {
                                                          "md5" : 1,
                                                          "path" : 1
                                                          }
                                           },
                                           { "$match" : {
                                                        "md5" : { "$ne" : "" } 
                                                        }
                                           },
                                           {"$group" : {
                                                       "_id" : "$md5",
                                                        "nodeSet" : { "$push" : "$path"},
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
    for record in cursor['result']:
        print "nodeCount:{} nodeSet{}".format(record['nodeCount'], repr(record['nodeSet']))

    print "#--Done creating MD5 dict---"
    
def extract_picture_frame_sets():
    outdirs = ['/home/scott/Desktop/SJJframe', '/home/scott/Desktop/NGHframe', '/home/scott/Desktop/JAJframe']
    searchtags = ['SJJ Frame', 'NGH Frame', 'JAJ Frame']
    for tag, outdir in zip(searchtags, outdirs):
        records = db.aggregate(
                               [
                                {'$match' : {'user_tags' : {'$in' : [tag] }}},
                                { '$sort' : { 'signature' : 1}},
                                { '$group' : 
                                    {
                                     '_id' : "$signature",
                                     'firstPath' : { '$first' : '$path'}
                                     }
                                 },
                                { '$project' : {'firstPath' : 1}}
                                ])
        print "#Number of records:", len(records['result'])
        for count, entry in enumerate(records['result'], start = 1):
            path = entry['firstPath']
            print 'convert "{}" -resize x768 "{}/deres{}"'.format(path, outdir, count)
    

def compute_dir_match():
    #Walk the tree
    dirpathlist = ["/media/526b46db-af0b-4453-a835-de8854d51c2b/Photos/2000"]
    for dirpath in dirpathlist:
        regex = re.compile('^' + dirpath + '$')
        print "Finding {}".format('^' + dirpath)
        lists = db.find({'path' : regex, 'isdir' : True}, {'path' : 1, 'dirpaths' : 1, 'filepaths' : 1, "_id" : 0})
        print lists.count()
        for pathlist in lists:
            #print pathlist.keys(), pathlist
            print pathlist['path']
            print pathlist['dirpaths']
            print pathlist['filepaths']
    print "#--Done walking tree---"

db = pymongo.MongoClient().phototest.photo_archive  #Set up filepaths to be unique keys?
db.ensure_index('path', unique = True)

#find_empty_files()
#find_empty_dirs()
#find_hybrid_dirs()
#create_md5_dict()
extract_picture_frame_sets()
#compute_dir_match()