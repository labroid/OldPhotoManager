'''
Created on Feb 15, 2014

@author: scott_jackson
'''
'''
Thoughts on duplicate finding using database

Algorithm 1:
for each record, find records with matching signature, and populate duplicates
- n^2 reads + m writes (m = number of duplicates)

Algorithm 2:
find unique signatures.  For each one that appears more than once, mark each file with the others
- n reads + m writes

Algorithm 3:
aggregate id's of similar nodes.  For each set > 1 in length, mark each file with the others

Unfortunately, to find out where duplicates are stored and if they are all included, one needs the paths in advance, no?

Other thoughts on overall flow:

FOR INTERNAL DUPLICATES
Extract data from photo files
Put data in database [this will be default condition in cloud]
do
    Find duplicates, update data to database (or separate one?)
    Display data, choose files to move out
    Move records to 'moved' database
until done
use 'moved' database to physically move files
check 'moved' files are in archive before deleting

FOR EXTERNAL DUPLICATES
Extract data from archive, put in database [default condition in cloud]
Extract data from candidates
Put candidate data in database
do
    check if candidates in archive
    Display data, chose files to move out
    Move candidates to 'moved' database
until done
use 'moved' database to physically move files
check 'moved' files are all in archive before deleting

 
'''


import pymongo
import re
import collections
import time
import json

class photo_collection(object):
    '''This class creates a client and connection to a mongodb db and collections necessary to service photo needs. Intended for a single collection'''
    
    def __init__(self):
        try:
            self.client = pymongo.MongoClient()
        except:
            print "Problem starting pymongo client.  Is database running?  Bailing out." #TODO change to logger; does this suppress the mongo error?
            exit(1)
        self.db = None
        self.collection = None
        self.duplicates = None
        self.db_name = None
        self.collection_name = None
        self.duplicates_name = None
        self.target_tree = None
    
    def connect(self, db_name, collection_name):
        self.db_name = db_name
        self.collection_name = collection_name
        self.duplicates_name = collection_name + "_duplicates"
           
        if self.db_name not in self.client.database_names():
            print 'Database "{}" does not exist - something is wrong.  Bailing out.'.format(self.db_name)  #TODO change to logger
            exit(1)
        self.db = self.client[self.db_name]
        
        if self.collection_name not in self.db.collection_names():
            print 'Collection "{}" does not exist - something is wrong.  Bailing out...'.format(self.collection_name)  #TODO change to logger
            exit(1)
        self.collection = self.db[self.collection_name]
        self.index_check_collection()
            
    def create(self, input_file, db_name, collection_name):  #This is probably totally wrong in setting up database
        self.db_name = db_name
        self.collection_name = collection_name
        self.duplicates_name = collection_name + "_duplicates"
        
        if self.db_name in self.client.database_names():
            print 'Warning: database {} exists.'.format(self.db_name)
        self.db = self.client.self.db_name
        
        if self.collection_name in self.db.collection_names():
            print 'Warning: collection {} exists.'.format(self.collection_name)
        self.collection = self.db.self.collection_name
        
        #sample line from file: {'isdir': False, 'node': '/home/shared/Photos/Upload/2009/090503/img_7791.jpg', 'user_tags': None, 'timestamp': datetime.datetime(2009, 5, 3, 6, 17, 28), 'got_tags': True, 'dirpaths': [], 'filepaths': [], 'in_archive': False, 'mtime': 1241345848.0, 'signature': 'ea53d7acda8dce6d7735b07db2cd7203', 'size': 1397295, 'md5': '8b6b90d89a7831cca8864da7968465ca'}
        try:
            fp = open(input_file, 'r')
        except:
            print 'Problem opening file "{}".  Bailing out.'.format(input_file)
        #Populate the database
        count = 0
        for line in fp:
            self.collection.insert(eval(line))
            count += 1
            if count%1000 == 0:
                print count
        self.index_check_collection()
                
    def index_check_collection(self):
    #Generate indices based on node and signature
        self.collection.ensure_index("node", name = "node_id", unique = True)    
        self.collection.ensure_index("signature", name = "signature_id")
        print "Checking for non-dirs that have no checksum:",
        no_sums = self.collection.find({"signature" : "", "isdir" : False})
        if no_sums.count() > 0:
            print "Whoa! Found documents that aren't directories with no signature"
            for i in no_sums:
                print i
            print "Exiting..."
            exit(1)
        print "All OK"
        
    def show_stats(self):
        print "Total number of photos:", self.collection.find({"isdir" : False}).count()
        print "Total number of directories:", self.collection.find({"isdir" : True}).count()
        print "Total number of unique photos:", len(self.collection.find({"signature" : { "$ne" : "" }}).distinct("signature"))
        
    def mark_duplicates(self, target_tree):
        self.target_tree = target_tree
        print "Doing an aggregation pipeline to group duplicates",
        cursor = self.collection.aggregate([  #This aggregates _id instead of node because of file size limitations, so another query is required later to resolve :-(
                                                  
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
            return
        print "Save the results in a 'duplicates' database:", #This whole section can be avoided once $out is implemented by pymongo for aggregations
        self.duplicates = self.db[self.duplicates_name]  #Shortcut name
        self.duplicates.drop() #Doesn't complain if collection does not exist - which is nice
        self.duplicates.insert(cursor['result'])  
        print "- Done"
               
        print "Marking duplicates in main database:"
        count = 0
        collection_cursor = self.collection.find({"node" : {"$regex" : re.compile(re.escape(target_tree))}}, {"node", "signature", "isdir"}) #_id comes by default
        num_records = collection_cursor.count()
        print "Number of records:", num_records
        for record in collection_cursor:
            if record['isdir']:
                node_count = -1
                node_list = None
            else:
                dup_data = self.duplicates.find_one({"_id" : record["signature"]}, {"nodeSet", "nodeCount"})
                node_names = self.collection.find({"_id" : {'$in' : dup_data['nodeSet']}})
                node_list = [x['node'] for x in node_names]
                node_list.remove(record['node']) #Take one's own name off the list
                node_count = dup_data['nodeCount'] - 1  #Decrement since one's name was removed
            depth = record['node'].count('/')
            self.collection.update({"_id" : record["_id"]}, {"$set" : {"nodeCount" : node_count, "nodeSet" : node_list, 'depth' : depth}})
            count += 1
            if count % 1000 == 0:
                print count,"/",num_records
        print "Done"
        
    def show_node(self, path):
        print "Received from browser:",path
        if path == 'treegrid_node': #If null, return base path
            path = self.target_tree
            
        records = self.collection.find_one({"node" : path}, {"filepaths", "dirpaths"})
        files = []
        if len(records['filepaths']) > 0:
            records['filepaths'].sort()
            for file_name in records['filepaths']:
                files.append({'id':file_name, 'name':file_name, 'size':0, 'date':1, 'state':'open'})
        dirs = []
        if len(records['dirpaths']) > 0:
            records['dirpaths'].sort()
            for dir_name in records['dirpaths']:
                dirs.append({'id':dir_name, 'name':dir_name, 'size':0, 'date':0, 'state':'closed'})
        return json.dumps(files + dirs)
            



if __name__ == '__main__':
    test = photo_collection()
    test.connect('photo_database', 'barney')
    test.mark_duplicates("/home/shared/Photos/2008")