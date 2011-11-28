'''
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
from photoData import photoData

def traverse(self, sumFunction):
    if os.path.isfile(self.path):  #For the case when the root_in is just a file and not a directory
        self.sumFunction("", [], [self.path])
    else:
        for root, dirs, files in os.walk(self.path, topdown=False):
            self.sumFunction(root, dirs, files)
        
def sumFileDirSize(self, root, dirs, files):
    total_size = 0
    for file in files:
        filename = os.path.join(root, file)
        self.data[filename] = photoData.photoUnitData()
        self.data[filename].dirflag = False
        try:
            self.data[filename].size = os.path.getsize(filename)
        except:
            self.data[filename].size = "Can't determine size of " + filename
            #todo this should be logged and filesize set to -1
            break
        total_size += self.data[filename].size
        try:
            self.data[filename].mtime = os.path.getmtime(filename)
        except:
            self.data[filename].mtime = "Can't determine mtime of " + filename
            break
    for dir in dirs:
        dirname = os.path.join(root, dir)
        total_size += self.data[dirname].size
    if root != '':  #When root is a directory and not just a simple file
        self.data[root] = photoUnitData.photoUnitData()
        self.data[root].size = total_size
        self.data[root].dirflag = True
        if len(dirs) == 1 and len(files) == 0:
            self.data[root].degenerateParent = True
        else:
            self.data[root].degenerateParent = False