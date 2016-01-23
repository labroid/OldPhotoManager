__author__ = 'scott_jackson'
#Build class that can represent Archive and Target (separately assumed)
#Tools to look for (Archive or Target) integrity, printing, etc.
#Tools to compare Target to Archive, or Target to itself

#Examples:
#Hook up to Archive
#Check archive for problems starting from some top
#
#Hook up to Archive
#Hook up to Target
#Look for duplicates from Target(top) in Archive(top)

#archive = Photos('archive', 'localhost', 'barney')
#target = Photos('target', 'localhost', 'path', new=True)
#find_duplicates(archive, target, archive_top, target_top)
#browse(archive)

import mongoengine as me
import mongoengine.context_managers as cm

class Photo(me.Document):
    path = me.StringField()
    dirpaths = me.ListField(me.StringField())
    filepaths = me.ListField(me.StringField())
    in_archive = me.BooleanField()
    isdir= me.BooleanField()
    orphan = me.StringField()
    refresh_time = me.FloatField()
    got_tags = me.BooleanField()
    md5 = me.StringField()
    mtime = me.FloatField()
    signature = me.StringField()
    size = me.LongField()
    timestamp = me.DateTimeField()
    user_tags = me.ListField(me.StringField())
    md5_match = me.ListField(me.StringField())
    meta = {'collection': 'photos'}

class Status(me.Document):
    collection = me.StringField()
    config = me.StringField()
    host = me.StringField()
    traverse_path = me.StringField()
    fs_traverse_time = me.FloatField
    database_state = me.StringField
    meta = {'collection': 'config'}

target = 'target'
archive = 'archive'
me.connect(db='test_photos', host='localhost', alias=target)
me.connect(db='barney', host='localhost', alias=archive)

photo=Photo()
status=Status()

with me.context_managers.switch_db(Photo, archive):
    print Photo.objects().count()
#    for z in Photo.objects(isdir = False):
#        print z.isdir, z.path