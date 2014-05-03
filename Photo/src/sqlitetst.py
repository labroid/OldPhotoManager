'''
Created on Mar 20, 2014

@author: scott_jackson
'''
'''This turned into something of a mess.  To put in SQLite lists have to be combined, and if lists are empty (or None) they have to be handled to be storable.
At this point I'm not sure it saves a whole heck of a lot; it would be nice to JOIN using SQL for finding duplicates, but then we'd have to process the lists.
It is looking like I stay in noSQL or pull it into Python and do processing there...'''
import pymongo
import datetime
import calendar
import re
import collections
import time
import sqlite3
import itertools
from pywin.debugger.dbgcon import DoGetOption

def main():
#Set up database
    fp = open("C:/Users/scott_jackson/Desktop/jsonPhotos", 'r') 
    r = eval(fp.readline())
    print r
    print time.mktime(r['timestamp'].timetuple())
    conn = sqlite3.connect('example.db')
    c = conn.cursor()
    c.execute('''DROP TABLE IF EXISTS archive''')
    c.execute('''CREATE TABLE archive (isdir int, node text, timestamp int, got_tags int, in_archive int, mtime int, signature text, size int, md5 text)''')
    #dog = (r['isdir'], r['node'], '|'.join(r['user_tags']), r['timestamp'], r['got_tags'], '|'.join(r['dirpaths']), '|'.join(r['filepaths']), r['in_archive'], r['mtime'], r['signature'], r['size'], r['md5'])
    count = 0
    for record in fp.readline():
        print record
        r = eval(record)
        print r
        count += 1
        if count % 1000:
            print count
        dog = (r['isdir'], r['node'], time.mktime(r['timestamp'].timetuple()), r['got_tags'], r['in_archive'], r['mtime'], r['signature'], r['size'], r['md5'])
        c.executemany("INSERT INTO archive VALUES  (?, ?, ?, ?, ?, ?, ?, ?, ?)", dog)
    print "committing"
    conn.commit()
    print "put is in!"
    conn.close()
    exit()
# Save (commit) the changes

#{'isdir': False, 'node': '/home', 'user_tags': None, 'timestamp': , 'got_tags': True, 'dirpaths': [], 'filepaths': [], 'in_archive': False, 'mtime': 1241345848.0, 'signature': 'ea53d7acda8dce6d7735b07db2cd7203', 'size': 1397295, 'md5': '8b6b90d89a7831cca8864da7968465ca'}
   
#Populate the database
    count = 0
    for line in fp:
        temp = eval(line)
        collection.insert(eval(line))
        count += 1
        if count%1000 == 0:
            print count

    exit(1)

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
    
#    db.duplicates.drop()
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
            collection.update({"_id" : record["_id"]}, {"$set" : {"nodeCount" : node_count, "nodeSet" : node_list}})
            count += 1
            if count % 1000 == 0:
                print count,"/",num_records
        print "Done"
            
    cursor = collection.find({"node" : {"$regex" : re.compile(re.escape(target_tree))},"nodeCount" : {"$gt":2}}) #_id comes by default
    if cursor.count() > 0:
        for record in cursor:
            print record
    exit()
            
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
