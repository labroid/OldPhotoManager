'''
Created on Nov 23, 2011

@author: scott_jackson

Finding if a node is represented in an archive
    [TODO: take care of special case of a file and not a directory]
    traverse candidate tree and get sizes and mtimes (SumFileandDirSize())
    traverse candidate tree
        for each file 
            get EXIF tags
            search archive for file with same timestamp
            if no files with same timestamp in archive
                mark candidate file for inclusion in archive
            else
                if file checksums are identical
                    mark candidate as included
                else
                    compute photo signature (thumbnail checksum, tag checksum)
                    if thumbnail checksums different [Watch out, thumnail might be start/lengt]
                        mark candidate for inclusion in archive
                    else
                        if tags are identical
                            mark candidate as included
                            note strangeness (thumbnails and tags same but file checksum different)
                        else
                            mark for review by user
            accululate status for root (all_included, all_include)
        
        for each directory
            accumulate status for root (all_included, all_include)
                mark directory as included
            
        for root mark with accumulated status
'''

import os
import datetime
import EXIF

def isNodeInArchive(archive, node):
    for root, dirs, files in os.walk(node.path, topdown=False):
        for file in files:
            filename = os.path.join(root,file)
            node.data[filename].tags = getTags(filename)
            node.data[filename].timestamp = getTimestampFromTags(node.data[filename].tags)
            
            
def getTags(filename):    
    try:
        fp = open(filename,'rb')
    except:
        print "getTags():", filename, "can't be opened."
        return({})
    tags = EXIF.process_file(fp, details = False)
    return(tags)

def getTimestampFromTags(tags):
    defaultTimestamp = datetime.datetime.strptime('1950:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
    if 'EXIF DateTimeOriginal' in tags:
        try:
            timestamp = datetime.datetime.strptime(tags['EXIF DateTimeOriginal'].values,'%Y:%m:%d %H:%M:%S')
        except ValueError:
            #Log: "Time cannot be converted. Timestring -->" + tags['EXIF DateTimeOriginal'].values + "<--")
            timestamp = defaultTimestamp
    else:
        #Log:  'File has no EXIF DateTimeOriginal tag'
        timestamp = defaultTimestamp
    return(timestamp)
            
    
