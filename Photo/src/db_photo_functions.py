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
    
def find_duplicates(db_archive, top_archive, db_target, top_target):
    '''
    If there is a database, mount it
    otherwise extract photo data
    make request to archive
    print results
    '''
    '''
    Mount target db
    if target db is this host:
        sync db
    mount archive db
    for file in target db from top:
        if file is in archive db:
            print relevant data
    '''
    #config_host = db_target.config.find_one('host' : )
    regex = tree_regex(top_target)
    records = db_target.photos.find({'path' : regex})
    for record in records:
        match = db_archive.photos.find_one({'signature' : record['signature']}) #TODO: consider 'top' here
        if match.count() < 1:
            print "{}: No match. Please upload."
        else:
            if record['md5'] == match['md5']:
                print "{}: Exact match: {}".format(match['path'])
            else:
                print "{}: Signature match: {}".format(match['path'])
                
        
def tree_regex(top):
    '''
    Return regex that will extract tree starting from top, independent of OS
    '''
    if top.count('/') > 0: #Linux
        top_tree = '^' + top + '$|^' + top + '/.*'
        top_regex = re.compile(top_tree)
    elif top.count('\\') > 0: #Windows
        path = re.sub(r'\\', r'\\\\', top)
        basepath = '^' + path
        children = '^' +  path + '\\\\.*' 
        pattern = unicode(basepath + '|' + children)
        pattern = unicode(pattern)
        top_regex = re.compile(pattern)
    else:
        print("Error:  Path to top of tree contains no path separators, can't determine OS type.  Tree top received: {}".format(top))
    return top_regex

def find_empty_files(photos, top):
    '''
    Print list of empty files suitable for use in shell script to delete them  TODO:  Move them?
    '''
    emptylist = photos.find({'isdir' : False, 'path' : tree_regex(top), 'size' : long(0)})
    print "#Number of empty files: {}".format(emptylist.count())
    for empty in emptylist:
        print "#rm {} # Size {}".format(empty['path'], empty['size'])
    print "#--Done finding empty files---"

def find_empty_dirs(photos, top):
    '''
    Print list of empty directories suitable for use in shell script to delete them
    '''    
    emptylist = photos.find({'isdir' : True, 'filepaths' : [], 'dirpaths' : [], 'path' : tree_regex(top)},{'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    print "Number of empty dirs: {}".format(emptylist.count())
    for empty in emptylist:
        print "#rmdir {} # File list {}, Dir list {}".format(empty['path'], empty['filepaths'], empty['dirpaths'])
    print "#--Done finding empty dirs---"

def find_unexpected_files(photos, top):
    '''
    Print list of unexpected file types suitable for use in shell script to move them
    '''
    EXPECTED = ['\.jpg$', '\.png$', '\.picasa.ini$', 'thumbs.db$', '\.mov$', '\.avi$', '\.thm$', '\.bmp$', '\.gif$']
    all_targets = ''
    for target in EXPECTED:
        all_targets = all_targets + target + "|"
    all_targets = all_targets[:-1]
    target_regex = re.compile(all_targets, re.IGNORECASE)
    records = photos.aggregate([
                                 {'$match' : { 'path' : tree_regex(top)}},
                                 {'$match' : {'isdir' : False}},
                                 {'$match' : { 'path' : {'$not' : target_regex}}},
                                 {'$project' : {'_id' : False, 'size' : True, 'path' : True}}
                                ])
    for record in records['result']:
        print("#rm {} # Size: {}".format(record['path'], record['size']))
    print "Done"
   
def find_hybrid_dirs(photos, top):  #Broken
    #Look for dirs that have files and dirs in them (this shouldn't happen if the photo directory is clean)
#    hybridlist = db.find({'$and': [{'$not' : {'filepaths' : '[]'}}, {'$not' : {'dirpaths' : '[]'}}]}, {'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    hybridlist = photos.find({'$and' : [{'filepaths' : {'$ne' : '[]'}}, {'dirpaths' : {'$ne' : '[]'}}]}, {'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    print "Length of hybrid list:", hybridlist.count()
 #   for hybrids in hybridlist:
#        print "#rmdir {} # File list {}, Dir list {}".format(hybrids['path'], hybrids['filepaths'], hybrids['dirpaths'])
#print "#--Done finding hybrid 

class tree_stats():
    def __init__(self, photos, top):
        self.photos = photos
        self.top = top
        self.top_regex = tree_regex(top)
        self.total_nodes = 0
        self.total_dirs = 0
        self.total_files = 0
        self.tagged_records = 0
        self.unique_signatures = 0
        self.unique_md5s = 0
        self.compute_tree_stats()
        
    def compute_tree_stats(self):
        self.total_nodes = self.photos.find({'path' : self.top_regex}).count()
        self.total_dirs = self.photos.find({'path' : self.top_regex, 'isdir' : True}).count()
        self.total_files = self.photos.find({'path' : self.top_regex, 'isdir' : False}).count()
        self.tagged_records = self.photos.find({'path' : self.top_regex, 'isdir' : False, 'user_tags' : {'$ne' : []}}).count()
        signatures = self.photos.aggregate([
                                               { "$match" : {
                                                            "path" : self.top_regex,
                                                            "isdir" : False, 
                                                            "signature" : { "$ne" : "" }
                                                            }
                                               },
                                               {"$group" : {
                                                           "_id" : "$signature"
                                                           }
                                                }
                                              ])
        if signatures['ok'] != 1:
            raise RuntimeError('Mongodb return code not = 1.  Got: {}'.format(signatures['ok']))
        self.unique_signatures = len(signatures['result'])
        md5s = self.photos.aggregate([
                                           { "$match" : {
                                                        "path" : self.top_regex,
                                                        "isdir" : False,
                                                        "md5" : { "$ne" : "" }
                                                        }
                                           },
                                           {"$group" : {
                                                       "_id" : "$md5"
                                                       }
                                            }
                                          ])
        if md5s['ok'] != 1:
            raise RuntimeError('Mongodb return code not = 1.  Got: {}'.format(signatures['ok']))
        self.unique_md5s = len(md5s['result'])
        
    def print_tree_stats(self):
        print "Stats for tree rooted at {}".format(self.top)
        print("Total nodes: {}".format(self.total_nodes))
        print("Total dirs: {}".format(self.total_dirs))
        print("Total files: {}".format(self.total_files))
        print("Total tagged: {}, {:.1%} of files".format(self.tagged_records, float(self.tagged_records)/self.total_files))
        print("Unique signatures = {}, {} duplicates ({:.1%})".format(self.unique_signatures, self.total_files - self.unique_signatures, 1.0 - float(self.unique_signatures)/self.total_files))
        print("Unique MD5s = {}, {} duplicates ({:.1%})".format(self.unique_md5s, self.total_files - self.unique_md5s, 1.0 - float(self.unique_md5s)/self.total_files))
        

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