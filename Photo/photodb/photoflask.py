__author__ = 'scott_jackson'

import json
from flask import Flask, render_template
import photo_data

app = Flask(__name__)

@app.route('/')
def hello():
    print "It went to root"
    return render_template('fancytree_example.html')
#    return page

@app.route('/db_init')
def db_init():
    print "It went to db_init)"
    return('''
[
  {
    "folder": true,
    "key": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos",
    "title": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos"
  },
  {
    "folder": true,
    "key": "blah",
    "title": "This is the second folder",
    "children": [
        {"title": "child 1", "key": "child1"},
        {"title": "child 2", "key": "child2"}
        ]
  },
  {
    "folder": false,
    "key": "foo",
    "title": "foo file"
  }
]
''')
#     return('''
# [
#   {
#     "folder": true,
#     "key": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos",
#     "title": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos"
#   },
#   [
#     {
#       "folder": true,
#       "key": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos.archive",
#       "title": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos.archive"
#     },
#     {
#       "folder": true,
#       "key": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos.target",
#       "title": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos.target"
#     }
#   ]
# ]
#     ''')
    return('[{"folder": true,"key": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos","title": "C:.Users.scott_jackson.git.PhotoManager.Photo.tests.test_photos"}]')
#    return('[{"folder": true, "key": "Hello","title": "There"}]')

@app.route('/db', methods=['GET', 'POST'])
def db(*args):
    print "It went to db"
    for n, arg in enumerate(args):
        print "Argument {}: {}".format(n, arg)
    database = photo_data.set_up_db('test_photos', 'localhost')
    test_photos_base = "C:\\Users\\scott_jackson\\git\\PhotoManager\\Photo\\tests\\test_photos"
    #query top using db_walk
    for top, dirpaths, filepaths in  photo_data.walk_db_tree(database.photos, test_photos_base, topdown=True):
        json_input = [{"title": top, "key": top, "folder": True}]
        files = []
        dirs = []
        for filepath in filepaths:
            files.append({"title": filepath, "key": filepath})
        for dirpath in dirpaths:
            dirs.append({"title": dirpath, "key": dirpath, "folder": True})
#        json_input.append(files)
        json_input.append(dirs)
        return json.dumps(json_input)

#
# [{"title": "Node 1", "key": "1"},
#  {"title": "Folder 2", "key": "2", "folder": true, "children": [
#     {"title": "Node 2.1", "key": "3"},
#     {"title": "Node 2.2", "key": "4"}
#   ]}
# ]

    #build json
    #return it

if __name__ == '__main__':
    app.run()