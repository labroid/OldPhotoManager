# pylint: disable=line-too-long

#TODO IMPORTANT:  If operations are attempted against a tree where the root node is not a child of 
#the latest "CLEAN" traverse path, then operation should be rejected!

import re
import sys
import os.path
import pymongo
import pymongo.errors
from py._path.local import PosixPath
import posixpath

def main():
    try:
        db_target = pymongo.MongoClient().phototest2
        db_archive = pymongo.MongoClient().phototest
        #config = db_target.config
#        photos = db_target.photo_archive2
        #photos = db_target.photo_archive
        photos.ensure_index('path', unique = True)
    except pymongo.errors.ConnectionFailure:
        print "***ERROR*** Database connection failed.  Make sure mongod is running."
        sys.exit(1)
    except pymongo.errors.CollectionInvalid:
        print "Mongodb Collection invalid - check collection name"
        sys.exit(1)
    except:
        print "Problem connecting to mongodb"
    
    top = u'/media/526b46db-af0b-4453-a835-de8854d51c2b/Photos'
    #top = u"C:\\Users\\scott_jackson\\Pictures\\Uploads"
    find_empty_files(photos, top)
    find_empty_dirs(photos, top)
    #find_hybrid_dirs(top)
    #check_user_tags(photos, top, fix = True)
    #for dirname in range(2000, 2015):
    print "Listings for {}".format(top)
    #print "DB status: {}, Last Scanned: {}".format(#put the right stuff here)
    record = photos.find_one({'path' : top})
    dirpaths = record['dirpaths']
    dirpaths.sort()
    for dirpath in dirpaths:
        stats = tree_stats(photos, dirpath)
        if stats.total_files > 0:
            print "{:>50s}: {:6d} Files, {:5.1%} md5 duplicates, {:5.1%} signature duplicates, {:5.1%} tagged".format(os.path.basename(dirpath), stats.total_files, 1.0 - float(stats.unique_md5s)/stats.total_files, 1.0 - float(stats.unique_signatures)/stats.total_files, float(stats.tagged_records)/stats.total_files)
        else:
            print "{:>50s}: {:6d} Files, ----- md5 duplicates, ----- signature duplicates, ------ tagged".format(os.path.basename(dirpath), stats.total_files)
    
    
    
    #create_md5_dict(top)
    #extract_picture_frame_set(' JAJ Frame', '/home/scott/Desktop/JAJframe')
    #compute_dir_match(top)
    #compute_tag_distribution(top)
    #walk_db_tree(top)
    #print_tree(top)
    #find_unexpected_files(top)
    
        
   
def find_hybrid_dirs(photos, top):  #Broken
    #Look for dirs that have files and dirs in them (this shouldn't happen if the photo directory is clean)
#    hybridlist = db.find({'$and': [{'$not' : {'filepaths' : '[]'}}, {'$not' : {'dirpaths' : '[]'}}]}, {'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    hybridlist = photos.find({'$and' : [{'filepaths' : {'$ne' : '[]'}}, {'dirpaths' : {'$ne' : '[]'}}]}, {'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    print "Length of hybrid list:", hybridlist.count()
 #   for hybrids in hybridlist:
#        print "#rmdir {} # File list {}, Dir list {}".format(hybrids['path'], hybrids['filepaths'], hybrids['dirpaths'])
#print "#--Done finding hybrid 

def check_user_tags(photos, top, fix = False):
    no_tag_field= photos.find({'path' : tree_regex(top), 'isdir' : False, 'user_tags' : {'$exists' : False}}, {'_id' : False, 'path' : True})
    if no_tag_field.count() > 0:
        for record in no_tag_field:
            print "Error:  No user_tag field exists for: {}".format(record['path'])
        sys.exit(1)
    tag_records = photos.find({'isdir' : False, 'path' : tree_regex(top)})
    tagged = 0
    not_tagged = 0
    for record in tag_records:
        if type(record['user_tags']) != type([]):
            if fix is False:
                print("Error:  Field user_tag must be of type 'list'; got type {}".format(type(record['user_tags'])))
                sys.exit(1)
            else:
                photos.update({'_id' : record['_id']}, {'$set' : {'user_tags' : []}})
                print "Fixed default user_tags for {}".format(record['path'])
                record['user_tags'] = []
        if len(record['user_tags']) > 0:
            tagged += 1
        else:
            not_tagged += 1
    return(tagged, not_tagged)
                  
def dirs_with_no_tags(top):
    pass
    
def create_md5_dict(photos, top):
    cursor = photos([
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
    
def extract_picture_frame_set(photos, tag, output_dir):
    '''
    Scan database and produces shell script suitable to select and convert photos
    '''
    records = photos.aggregate(
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
    for count, entry in enumerate(records['result'], start = 1355):
        path = entry['firstPath']
        print 'convert "{}" -resize x768 "{}/deres{}"'.format(path, output_dir, count)

def compute_dir_match(top):
    create_md5_dict()
    top_path, dirpaths, filepaths = walk_db_tree(top, topdown = False)
    for path in filepaths:
        pass
        
def walk_db_tree(photos, top, topdown = True):
    record = photos.find_one({'path' : top}, {'_id' : False, 'dirpaths' : True, 'filepaths' : True})
    if topdown:
        yield top, record['dirpaths'], record['filepaths']
    for dirs in record['dirpaths']:
        for t in walk_db_tree(dirs):
            yield t
    if not topdown:
        yield top, record['dirpaths'], record['filepaths']
        
def print_tree(photos, top):
    #TODO:  Make this work for both posix and windows.  Hardwired for posix (looking for / and using posixpath now)
    offset = top.count('/')
    indent = 4
    print "Walking tree from: {}".format(top)
    for dirpath, dirs, files in walk_db_tree(top):
        print '{}{}'.format(' ' * indent * (dirpath.count('/') - offset), posixpath.basename(dirpath))
        for f in files:
            print '{}{}'.format(' ' * indent * (f.count('/') - offset), posixpath.basename(f))
        
def compute_tag_distribution(photos, top):
    records = photos.aggregate([
                        {'$match' : {'user_tags' : {'$ne' : ''}}},
                        {'$match' : {'user_tags' : {'$ne' : None}}},
                        {'$unwind' : '$user_tags'},
                        { '$group' : 
                             {
                              '_id' : '$user_tags',
                              'count' : { '$sum' : 1}
                              }
                        },
                         ])
    results = {}
    for record in records['result']:
        results[record['_id']] = record['count']
    print "Warning:  These counts include duplicate results"
    for w in sorted(results, key=results.get, reverse=True):
        print w, results[w]
            
if __name__ == "__main__":
    sys.exit(main())