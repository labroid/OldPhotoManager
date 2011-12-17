'''
Created on Oct 21, 2011

@author: scott_jackson

'''
import sys
import os
import datetime
from fileMD5sum import fileMD5sum, stringMD5sum

class photoUnitData():
    def __init__(self):
        self.size=0
        self.mtime = -(sys.maxint - 1) #Set default time to very old
        self.timestamp = datetime.datetime.strptime('1700:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
        self.tags = ''
        self.thumbnailMD5 = ''
        self.dirflag = False
        self.fileMD5 = ''
        self.degenerateParent = False
            
class photoData:
    def __init__(self, path):
        self.data = dict()
        self.path = path
        self.dupSizeList = ['NA']
        self.traverse(self.path, self.sumFileDirSize)
        
    def _walkError(self,walkErr):
        global _walkErrorFlag
        print "Error",walkErr.errno, walkErr.strerror
        return
                
    def traverse(self, root_in, sumFunction):
        if os.path.isfile(root_in):  #For the case when the root_in is just a file and not a directory
            sumFunction("", [], [root_in])
        else:
            for root, dirs, files in os.walk(root_in, topdown=False):
                sumFunction(root, dirs, files)
            
    def sumFileDirSize(self, root, dirs, files):
        if root not in self.data:
            total_size = 0
            for file in files:
                filename = os.path.join(root, file)
                self.data[filename] = photoUnitData()
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
            self.data[root] = photoUnitData()
            self.data[root].size = total_size
            self.data[root].dirflag = True
            if len(dirs) == 1 and len(files) == 0:
                self.data[root].degenerateParent = True
            else:
                self.data[root].degenerateParent = False
            
    def md5FilesAndDirs(self, root, dirs, files):
            catenatedMD5 = ''
            for file in files:
                filename = os.path.join(root, file)
                if self.data[filename].fileMD5 == '':
                    self.data[filename].fileMD5 = fileMD5sum(filename)
                catenatedMD5 += self.data[filename].fileMD5
    #            print self.data[filename].fileMD5, filename
            for dir in dirs:
                dirname = os.path.join(root, dir)
                catenatedMD5 += self.data[dirname].fileMD5
            if root != '':  #When the incoming root is a directory and not just a file
                self.data[root].fileMD5 = stringMD5sum(catenatedMD5)
    #        print self.data[root].fileMD5, root
    #        print "."
    
    def listLargestDuplicateTrees(self, count = 1):
        if self.dupSizeList[0] == 'NA':
            self.findSameSizedTrees()
        dupListLength = len(self.dupSizeList)
        if dupListLength == 0:
            return([])
        if dupListLength < count:
            count = dupListLength
        dupList = []
        for i in range(count):
            sumSet = []
            for node in self.dupSizeList[i]:
                print "Summing:",node
                self.traverse(node, self.md5FilesAndDirs)
                sumSet.append(self.data[node].fileMD5)
            print "Set of resulting MD5s",sumSet
            if reduce(lambda x, y: x == y, sumSet):
                dupList.append(self.dupSizeList[i])
        return(dupList)
        

    def listZeroLengthFiles(self):
        zeroLengthNames = []
        for target in self.data.keys():
            if self.data[target].size == 0:
                zeroLengthNames.append(target)
        return(zeroLengthNames)
            
            
    def findSameSizedTrees(self):
    #Convert dictionary to list
        sizeList = [(k, v) for k, v in self.data.iteritems()]
    #Filter out degenerate parent directories
        validSizes = filter(lambda x: not x[1].degenerateParent, sizeList)
    #Find duplicate file sizes
        seen = set()
        duplicateSizes=set()
        for s in validSizes:
            if s[1].size in seen:
                duplicateSizes.add(s[1].size)
            else:
                seen.add(s[1].size)
    #Sort list in order of size and remove from list children of nodes
        duplicateSizes = sorted(duplicateSizes, reverse = True)
            
    #Now build a nested list collecting file names of duplicates
        self.dupSizeList = []
        for dups in duplicateSizes[0:10]:
            dupeSubList = []
            for n in validSizes:
                if n[1].size == dups:
                    dupeSubList.append(n[0])
            self.dupSizeList.append(dupeSubList)
                    
        return()
    
    def listSameSizedTrees(self):
        if self.dupSizeList[0] == 'NA':
            self.findSameSizedTrees()
        return(self.dupSizeList)

        
        
    