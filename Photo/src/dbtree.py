'''
Created on Feb 15, 2014

@author: scott_jackson
'''

import pymongo
import re
import collections
import time

class photo_mongo(object):
    def __init__(self):
        pass
    
    def connect(self, db, collection):
        

def main():
    client = pymongo.MongoClient()
    if 'photo_database' not in client.database_names():
        print "Database photo_database does not exist - something's wrong.  Bailing out..."
        exit(1)
    db = client.photo_database
    if 'barney' not in db.collection_names():
        #{'isdir': False, 'node': '/home/shared/Photos/Upload/2009/090503/img_7791.jpg', 'user_tags': None, 'timestamp': datetime.datetime(2009, 5, 3, 6, 17, 28), 'got_tags': True, 'dirpaths': [], 'filepaths': [], 'in_archive': False, 'mtime': 1241345848.0, 'signature': 'ea53d7acda8dce6d7735b07db2cd7203', 'size': 1397295, 'md5': '8b6b90d89a7831cca8864da7968465ca'}
        #    fp = open("C:/Users/scott_jackson/Desktop/jsonPhotos", 'r')
        #Populate the database
#     count = 0
#     for line in fp:
#         temp = eval(line)
#         collection.insert(eval(line))
#         count += 1
#         if count%1000 == 0:
#             print count
        print "Collection barney does not exist - something's wrong.  Bailing out..."
        exit(1)
    collection = db.barney

#Generate indices based on node and signature
    collection.ensure_index("node", name = "node_id", unique = True)    
    collection.ensure_index("signature", name = "signature_id")

    print "Total number of photos:", collection.find({"isdir" : False}).count()
    print "Total number of directories:", collection.find({"isdir" : True}).count()
    print "Total number of unique photos:", len(collection.find({"signature" : { "$ne" : "" }}).distinct("signature"))
    
    print "Checking for non-dirs that have no checksum:",
    no_sums = collection.find({"signature" : "", "isdir" : False})
    if no_sums.count() > 0:
        print "Whoa! Found documents that aren't directories with no signature"
        for i in no_sums:
            print i
        print "Exiting..."
        exit(1)
    print "All OK"
    
#    db.duplicates.drop()  #Uncomment to force recalculation of duplicates
    target_tree = "/home/shared/Photos/2008"
    if 'duplicates' not in db.collection_names():
        print "Doing an aggregation pipeline to group duplicates",
        cursor = collection.aggregate([  #This aggregates _id instead of node because of file size limitations, so another query is required later to resolve :-(
                                                  
                                                     { "$match" : {
                                                                   "node" : {'$regex' : re.compile(target_tree)}
                                                                   }
                                                     },
                                                     { "$project" : {
                                                                   "signature" : 1,
                                                                   "_id" : 1
                                                                   }
                                                    },{ "$match" : {
                                                                 "signature" : { "$ne" : "" } 
                                                                 }
                                                    },
                                                    {"$group" : {
                                                                "_id" : "$signature",
                                                                 "nodeSet" : { "$push" : "$_id"},
                                                                "nodeCount" : {"$sum" : 1}
                                                                }
                                                    }
                                                   ])
        print "- Done"
        if len(cursor['result']) == 0:
            print "No duplicate results found!"
            exit()
        print "Save the results in a 'duplicates' database:", #This whole section can be avoided once $out is implemented by pymongo for aggregations
        db.duplicates.drop() #Doesn't complain if collection does not exist - which is good
        db.duplicates.insert(cursor['result'])
        print "- Done"
        
#     print "How long to read the whole db into Python?"
#     start = time.time()
#     trash = []
#     trash_cur = collection.find()
#     for record in trash_cur:
#         trash.append(record)
#     print time.time() - start, "seconds"  #Answer was 16.25 seconds
        
        print "Marking duplicates in main database:"
        count = 0
        collection_cursor = collection.find({"node" : {"$regex" : re.compile(re.escape(target_tree))}}, {"node", "signature", "isdir"}) #_id comes by default
        num_records = collection_cursor.count()
        print "Number of records:", num_records
        for record in collection_cursor:
            if record['isdir']:
                node_count = -1
                node_list = None
            else:
                dup_data = db.duplicates.find_one({"_id" : record["signature"]}, {"nodeSet", "nodeCount"})
                node_names = collection.find({"_id" : {'$in' : dup_data['nodeSet']}})
                node_list = [x['node'] for x in node_names]
                node_list.remove(record['node'])
                node_count = dup_data['nodeCount'] - 1
            depth = record['node'].count('/')
            collection.update({"_id" : record["_id"]}, {"$set" : {"nodeCount" : node_count, "nodeSet" : node_list, 'depth' : depth}})
            count += 1
            if count % 1000 == 0:
                print count,"/",num_records
        print "Done"
            
#    cursor = collection.find({"node" : {"$regex" : re.compile(re.escape(target_tree))},"nodeCount" : {"$gt":2}}) #_id comes by default
#    if cursor.count() > 0:
#        for record in cursor:
#            print record
#    exit()
            
    print "Sorting out paths by number of dupes:"
    
    dup_path_list = db.duplicates.aggregate([
                                            {'$group' : {
                                                         "_id" : "$nodeCount",
                                                         "allNodes" : {"$push" : "$nodeSet"}
                                                        }
                                             }
                                            ])

    exit(1)


if __name__ == '__main__':
    main()