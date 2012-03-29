'''
Created on Oct 21, 2011

@author: scott_jackson
'''
import sys
import logging
import pprint
import stopwatch
from photoFunctions import isNodeInArchive
from photoData import photoData
from photoUtils import printNow, environment
from pickleManager import photoPickler

def main():
    env = environment()
    logging.basicConfig(filename = env.options['logfile'], level = logging.DEBUG, filemode = 'w')
    timer = stopwatch.stopWatch()
    pickle = photoPickler(env.options['archivepickle'])
    if pickle.pickleExists:                
        archive = pickle.loadPickle(verbose = True)
    else:
        printNow("No pickle found.  Building Photo database.") 
        timer.start()     
        archive = photoData(env.options['archive'])
    print "Number of nodes: ", len(archive.data)
    print "Elapsed Time:",timer.read(),"or",timer.read()/len(archive.data) * 1000,"ms/node"
    print ""

    
    print "Counting node types"
    dircount = 0
    filecount = 0
    timer.start()
    for file in archive.data.keys():
        if archive.data[file].dirflag:
            dircount += 1
        else:
            filecount += 1
    print "Total time:",timer.read(),"or",timer.read()/(filecount+dircount)*1000000.0,"us/file"
    print "Dircount:",dircount,"Filecount",filecount,"Total",dircount+filecount        
        
    printNow("Extracting tags")
    timer.start()
    tagsChanged = archive.extractTags() 
    if tagsChanged:
        elapsedTime = timer.read()
        print "Tags extracted.  Elapsed time:", elapsedTime, "or", elapsedTime/filecount * 1000, "ms per file, or", elapsedTime/filecount * 100000/60, "Minutes for 100k files"
        pickle.dumpPickle(archive, verbose = True)
    else:
        print "No tags changed."
    
    print "Counting unique photos"
    dupCount = 0
    timer.start()
    photoSet = set()
    for file in archive.data.keys():
        if not archive.data[file].dirflag:
            md5 = archive.data[file].thumbnailMD5
            if md5 in photoSet:
                dupCount += 1
            else:
                photoSet.add(md5)
    print "Total time:",timer.read(),"or",timer.read()/(filecount+dircount)*1000000.0,"us/file"
    print "Dircount:",dircount,"Filecount",filecount,"Total",dircount+filecount
    print "Unique photos",len(photoSet),"(",len(photoSet)*1.0/(dircount+filecount)*100.0,"%) Duplicates:",dupCount,"(",dupCount*1.0/(dircount+filecount)*100.0,"%)"        

    print "Zero-length files:"
    zeroFiles = archive.listZeroLengthFiles()
    if len(zeroFiles) == 0:
        print "No zero-length files."
    else:
        for names in zeroFiles:
            print names
        print ""
          
    print "Finding same-sized trees..."
    timer.start()
    sameSizedTrees = archive.listSameSizedTrees(suppressChildren = True)
    print "Top same-sized nodes(found in", timer.read(),"seconds):"
    listLength = len(sameSizedTrees)
    if listLength == 0:
        print "No duplicate nodes found."
    else:
        if listLength > 1000:
            listLength = 1000
    for showLargest in range(0,listLength):
        print "Largest node, rank:",showLargest+1
        for node in sameSizedTrees[showLargest]:
            print node, archive.data[node].size/1000000.0,"MB"
    print ""
    
    timer.start()
    print "Checking Checksums to confirm duplication"
    dupList = archive.listLargestDuplicateTrees(1000)
    print "Checksums completed in",timer.read(),"Seconds."
    for entry in dupList:
        areDirs = True
        areFiles = True
        for file in entry:
            areDirs = areDirs and archive.data[file].dirflag
            areFiles = areFiles and not archive.data[file].dirflag
        areMixed = not areDirs and not areFiles
        if areDirs:  print "Duplicate Dirs:", entry
        if areFiles:  print "Duplicate Files:", entry
        if areMixed:  print "Mixed Dirs/Files:", entry, 'this should never happen!'

    #Re-pickle with hashes
    pickle.dumpPickle(archive)
    
    printNow("Checking candidate directory for inclusion in archive")    
    #Finishes silently after printng this.
    candidate = photoData("C:\\Users\\scott_jackson\\Pictures\\20110217 Herzaliya")
    candidate.extractTags()
    nodeInArchive = isNodeInArchive(archive, candidate)
    if nodeInArchive:
        print "Whole node is in archive"
    else:
        print "Part of node is in archive"
    print "Files in archive:"
    for file in candidate.data.keys():
        if not candidate.data[file].dirflag and candidate.data[file].inArchive:
            print file, candidate.data[file].timestamp
    print "Files not in archive:"
    for file in candidate.data.keys():
        if not candidate.data[file].dirflag and not candidate.data[file].inArchive:
            print file, candidate.data[file].timestamp    
    print "Done"
#Probably should clean the duplicate status etc. between runs to avoid carry forward from pickle          
    
if __name__ == '__main__':
    main()