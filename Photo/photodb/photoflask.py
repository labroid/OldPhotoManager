__author__ = 'scott_jackson'

import sys
import re
import json
import pymongo
from bson.objectid import ObjectId
from flask import Flask, render_template, request
import photo_data

app = Flask(__name__)
database = None
photo_base = None


def basename(path):
    base = re.search('.*[\\\\/](.+)$', path)
    return base.group(1)


@app.route('/')
def root():
    print "It went to root"
    global database
    global photo_base
    host = 'localhost'
    collection = 'photos'
    database_name = 'test_photos'
    photo_base = 'C:\\Users\\scott_jackson\\git\\PhotoManager\\Photo\\tests\\test_photos\\archive'
    database = photo_data.set_up_db(database_name, host)
#    latest_clean_scan = database.config.find({'database_state': 'clean'}).limit(1).sort({'$natural': -1})
#     latest_clean_scan = database.config.find({'database_state': 'clean'}).limit(1)
#     for latest in latest_clean_scan:
#         photo_base = latest['traverse_path']
    return render_template('fancytree_example.html', database=database_name, host=host, collection=collection)


@app.route('/db_init')
def db_init():
    print "It went to db_init)"
    record = database.photos.find_one({'path': photo_base})
    if not record:
        raise(ValueError, "Error - nothing in database at {}".format(photo_base))
    json_input = [{"title": record['path'], "key": str(record['_id']), "folder": record['isdir'], "lazy": True}]
    return json.dumps(json_input)


@app.route('/db')
def db():
    print "It went to db"
    top_record = database.photos.find_one({'_id': ObjectId(request.args['parent'])})
    json_input = []
    for dir_record in database.photos.find({'path': {'$in': top_record['dirpaths']}}):  # TODO: Put paths in alphabetical order
        if 'all_match' in dir_record and 'none_match' in dir_record:
            if dir_record['all_match']:
                if dir_record['none_match']:    #Empty director (other cases?)
                    color = 'tag_blue'
                else:                           #All simply match
                    color = 'tag_red'
            else:
                if dir_record['none_match']:
                    color = 'tag_green'         #Simple none match
                else:
                    color = 'tag_yellow'        #Some match, some don't
        else:
            color = 'purple'
        json_input.append({'folder': True, 'lazy': True, 'key': str(dir_record['_id']), 'title': basename(dir_record['path']), 'extraClasses': color})
    for file_record in database.photos.find({'path': {'$in': top_record['filepaths']}}):
        json_input.append({'key': str(file_record['_id']), 'title': basename(file_record['path'])})
    print json.dumps(json_input)
    return json.dumps(json_input)


if __name__ == '__main__':
    app.run()