'''
Created on Oct 21, 2011

@author: scott_jackson

'''
import sys
import os
import datetime
import logging
from fileMD5sum import fileMD5sum, stringMD5sum
from photoFunctions import getTagsFromFile, getTimestampFromTags,\
    thumbnailMD5sum, getTagsFromFile, getUserTagsFromTags

class photoUnitData():
    def __init__(self):
        self.size=0
        self.mtime = -(sys.maxint - 1) #Set default time to very old
        self.timestamp = datetime.datetime.strptime('1700:1:1 00:00:00','%Y:%m:%d %H:%M:%S')
        self.gotTags = False
        self.thumbnailMD5 = ''
        self.dirflag = False
        self.fileMD5 = ''
        self.userTags = ''
        self.inArchive = False
        self.candidates = []
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
                print "Same"
            else:
                print "Not same"
        return(dupList)
        

    def listZeroLengthFiles(self):
        zeroLengthNames = []
        for target in self.data.keys():
            if self.data[target].size == 0:
                zeroLengthNames.append(target)
        return(zeroLengthNames)
            
            
    def findDuplicateSizes(self,fileList):
        seen = set()
        duplicateSizes=set()
        for s in fileList:
            if s[1].size in seen:
                duplicateSizes.add(s[1].size)
            else:
                seen.add(s[1].size)
#Sort list in order of size and remove from list children of nodes
        duplicateSizes = sorted(duplicateSizes, reverse = True)
        return(duplicateSizes)
    
    def findSameSizedTrees(self, suppressChildren = True):
    #Convert dictionary to list
        sizeList = [(k, v) for k, v in self.data.iteritems()]
    #Filter out degenerate parent directories
        validSizes = filter(lambda x: not x[1].degenerateParent, sizeList)
    #Find duplicate file sizes
        duplicateSizes = self.findDuplicateSizes(validSizes)
            
    #Now build a nested list collecting file names of duplicates
        self.dupSizeList = []
        while len(duplicateSizes) > 0:
            print "Length of 'same size' list:", len(duplicateSizes)
            dupeSubList = []
            for n in validSizes:
                if n[1].size == duplicateSizes[0]:
                    dupeSubList.append(n[0])
            self.dupSizeList.append(dupeSubList)
            if suppressChildren: #Prune children from candidate list
                for root in dupeSubList:
                    for n in validSizes:
                        if root in n[0]:
                            validSizes.remove(n)
            duplicateSizes = self.findDuplicateSizes(validSizes)                 
        return()
    
    def listSameSizedTrees(self, suppressChildren = True):
        if len(self.dupSizeList) > 0:
            if self.dupSizeList[0] == 'NA': #NA indicates list is initialized by not computed.  Empty list means it is computed but there are no duplicates.
                self.findSameSizedTrees(suppressChildren)
        return(self.dupSizeList)
    
#    def extractTags(self, filelist = []):
#        if len(filelist) == 0:  #Might be better to make an iterator here and use that
#            filelist = self.data.keys()
#        tagsChanged = False
#        for file in filelist:
#            if not self.data[file].dirflag and not self.data[file].gotTags:
#                tags = getTagsFromFile(file)
#                self.data[file].timestamp, success, message = getTimestampFromTags(tags)
#                #if not success:
#                #    print "Unsuccessful timestamp conversion for file", file, message, "using mtime"
#                #self.data[file].userTags = getUserTagsFromTags(tags)
#                tagsChanged = True
#                self.data[file].thumbnailMD5 = thumbnailMD5sum(tags)
#                self.data[file].gotTags = True
#        return(tagsChanged)
#    
    def extractTags(self, filelist = []):
        logger = logging.getLogger('extractTags')  #Is there some automatic way to get func name??
        if len(filelist) == 0:
            filelist = self.data.keys()
        tagsChanged = False
        filecount = len(filelist)
        filesProcessed = 0
        reportBlock = 200
        lastBlock = 0
        for file in filelist:
            if not self.data[file].dirflag and not self.data[file].gotTags:
                tags = getTagsFromFile(file)
                if tags != None:
                    self.data[file].timestamp = getTimestampFromTags(tags)
                    self.data[file].thumbnailMD5 = thumbnailMD5sum(tags)
                    self.data[file].userTags = getUserTagsFromTags(tags)
                    tagsChanged = True
                    self.data[file].gotTags = True
#                    print self.data[file].timestamp, "|", self.data[file].userTags
                else:
                    logger.debug("%s: No Tags Returned." % file)
                    #print file, self.data[file].timestamp, self.data[file].userTags, self.data[file].thumbnailMD5
            filesProcessed += 1
            if (filesProcessed/reportBlock) > lastBlock:
                lastBlock = filesProcessed/reportBlock
                print filesProcessed, filecount, float(filesProcessed)/filecount * 100, "%"    
        return(tagsChanged)
    
    
    def extractThumbnailMD5(self, filelist = []):
        if len(filelist) == 0:
            filelist = self.data.keys()
        tagsChanged = False
        for file in filelist:
            if not self.data[file].dirflag:
                if self.data[file].thumbnailMD5 == '':
                    self.data[file].thumbnailMD5 = thumbnailMD5sum(self.data[file].tags)
                    tagsChanged = True
        return(tagsChanged)
        

        
        
    