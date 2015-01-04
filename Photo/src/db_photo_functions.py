import re
import time
import sys
import pymongo
from py._path.local import PosixPath
import posixpath
        
def tree_regex(top):
    '''
    Return regex that will extract tree starting from top, independent of OS
    '''
    if top.count('/') > 0: #Linux
        top_tree = '^' + top + '$|^' + top + '/.*' #TODO Make filesystem agnostic
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

def find_empty_files(top):
    '''
    Print list of empty files suitable for use in shell script to delete them  TODO:  Move them?
    '''
    emptylist = photos.find({'isdir' : False, 'path' : tree_regex(top), 'size' : long(0)})
    print "#Number of empty files: {}".format(emptylist.count())
    for empty in emptylist:
        print "#rm {} # Size {}".format(empty['path'], empty['size'])
    print "#--Done finding empty files---"

def find_empty_dirs(top):
    '''
    Print list of empty directories suitable for use in shell script to delete them
    '''    
    #Look for empty dirs
    emptylist = photos.find({'isdir' : True, 'filepaths' : [], 'dirpaths' : [], 'path' : tree_regex(top)},{'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    print "Number of empty dirs: {}".format(emptylist.count())
    for empty in emptylist:
        print "#rmdir {} # File list {}, Dir list {}".format(empty['path'], empty['filepaths'], empty['dirpaths'])
    print "#--Done finding empty dirs---"

def find_unexpected_files(top):
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
   
def find_hybrid_dirs():  #Broken
    #Look for dirs that have files and dirs in them (this shouldn't happen if the photo directory is clean)
#    hybridlist = db.find({'$and': [{'$not' : {'filepaths' : '[]'}}, {'$not' : {'dirpaths' : '[]'}}]}, {'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    hybridlist = photos.find({'$and' : [{'filepaths' : {'$ne' : '[]'}}, {'dirpaths' : {'$ne' : '[]'}}]}, {'path' : 1, 'filepaths' : 1, 'dirpaths' : 1})
    print "Length of hybrid list:", hybridlist.count()
 #   for hybrids in hybridlist:
#        print "#rmdir {} # File list {}, Dir list {}".format(hybrids['path'], hybrids['filepaths'], hybrids['dirpaths'])
#print "#--Done finding hybrid 

def tree_stats(top):
    print "Stats for tree rooted at {}".format(top)
    total_nodes = photos.find({'path' : tree_regex(top)}).count()
    print("Total nodes: {}".format(total_nodes))
    total_dirs = photos.find({'path' : tree_regex(top), 'isdir' : True}).count()
    print("Total dirs: {}".format(total_dirs))
    total_files = photos.find({'path' : tree_regex(top), 'isdir' : False}).count()
    print("Total files: {}".format(total_files))
    tagged_records = photos.find({'path' : tree_regex(top), 'isdir' : False, 'user_tags' : {'$ne' : []}})
    print("Total tagged: {}, {:.1%} of files".format(tagged_records.count(), float(tagged_records.count())/total_files))
    signatures = photos.aggregate([
#                                           { "$project" : {
#                                                           "signature" : 1
#                                                           }
#                                            },
                                           { "$match" : {
                                                        "path" : tree_regex(top),
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
    unique_signatures = len(signatures['result'])
    print("Unique signatures = {}, {} duplicates ({:.1%})".format(unique_signatures, total_files - unique_signatures, 1.0 - float(unique_signatures)/total_files))
    md5s = photos.aggregate([
#                                       { "$project" : {
#                                                       "md5" : 1
#                                                       }
#                                        },
                                       { "$match" : {
                                                    "path" : tree_regex(top),
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
    unique_md5s = len(md5s['result'])
    print("Unique MD5s = {}, {} duplicates ({:.1%})".format(unique_md5s, total_files - unique_md5s, 1.0 - float(unique_md5s)/total_files))

def check_user_tags(top, fix = False):
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
    
def create_md5_dict():
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
    
def extract_picture_frame_set(tag, output_dir):
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
        
def walk_db_tree(top, topdown = True):
    record = photos.find_one({'path' : top}, {'_id' : False, 'dirpaths' : True, 'filepaths' : True})
    if topdown:
        yield top, record['dirpaths'], record['filepaths']
    for dirs in record['dirpaths']:
        for t in walk_db_tree(dirs):
            yield t
    if not topdown:
        yield top, record['dirpaths'], record['filepaths']
        
def print_tree(top):
    #TODO:  Make this work for both posix and windows.  Hardwired for posix (looking for / and using posixpath now)
    offset = top.count('/')
    indent = 4
    print "Walking tree from: {}".format(top)
    for dirpath, dirs, files in walk_db_tree(top):
        print '{}{}'.format(' ' * indent * (dirpath.count('/') - offset), posixpath.basename(dirpath))
        for f in files:
            print '{}{}'.format(' ' * indent * (f.count('/') - offset), posixpath.basename(f))
        
def compute_tag_distribution(top):
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

db = pymongo.MongoClient().phototest
config = db.config
photos = db.photo_archive
photos.ensure_index('path', unique = True)

top = u'/media/526b46db-af0b-4453-a835-de8854d51c2b/Photos/Upload'
#find_empty_files(top)
#find_empty_dirs(top)
#find_hybrid_dirs(top)
#check_user_tags(top, fix = True)
tree_stats(top)
#create_md5_dict(top)
#extract_picture_frame_set(' JAJ Frame', '/home/scott/Desktop/JAJframe')
#compute_dir_match(top)
#compute_tag_distribution(top)
#walk_db_tree(top)
#print_tree(top)
#find_unexpected_files(top)

#-------------------------------old stuff--------------

'''    
    def get_statistics(self):
        class statistics:
            def __init__(self):
                self.dircount = 0
                self.filecount = 0
                self.unique_count = 0
                self.dup_count = 0
                self.dup_fraction = 0
        stats = statistics()
        
        photo_set = set()
        for archive_file in self.node:  #Got rid of keys() in case this breaks...  TODO need a test here
            if self[archive_file].isdir:
                stats.dircount += 1
            else:
                stats.filecount += 1
            sig = self[archive_file].signature
            if sig in photo_set:
                stats.dup_count += 1
            else:
                photo_set.add(sig)
                
        stats.unique_count = len(photo_set)
        stats.dup_fraction = stats.dup_count * 1.0 / (stats.filecount + stats.dup_count)
        logging.info("Collection statistics:  Directories = {0}, Files = {1}, Unique signatures = {2}, Duplicates = {3}, Duplicate Fraction = {4:.2%}".format(
            stats.dircount, stats.filecount, stats.unique_count, stats.dup_count, stats.dup_fraction))    
        return(stats)
            
    def print_statistics(self):
        result = self.get_statistics()
        print "Directories: {0}, Files: {1}, Unique PhotoData: {2}, Duplicates: {3} ({4:.2%})".format(result.dircount, result.filecount, result.unique_count, result.dup_count, result.dup_fraction)
        return
'''                