'''
Created on Oct 21, 2011

@author: scott_jackson
'''
import sys
import os.path
import pprint
import stopwatch
from photoFunctions import isNodeInArchive, getTags, getTimestampFromTags
from photoData import photoData

#import photoUnitData

def main():
    timer = stopwatch.stopWatch()
    print "Initializing photo database..."
    archive = photoData("C:\Users\scott_jackson\Pictures")
    print "Number of file sizes measured: ", len(archive.data)
    print "Elapsed Time for file sizes:",timer.read()
    print ""
    
    print "Extracting tags"
    timer.start()
    for file in archive.data.keys():
        if not archive.data[file].dirflag:
            getTags(file)
    print "Tags extracted.  Elapsed time:", timer.read()
    
    print "Converting timestamps"
    timer.start()
    for file in archive.data.keys():
        if not archive.data[file].dirflag:
            getTimestampFromTags(archive.data[file].tags)
    print "time tags converted in:", timer.read()
    
    sys.exit(0)
    
    print "Zero-length files:"
    zeroFiles = archive.listZeroLengthFiles()
    for names in zeroFiles:
        print names
    print ""
        
    timer.start()
    sameSizedTrees = archive.listSameSizedTrees()
    print "Top duplicate nodes(found in", timer.read(),"seconds):"
    listLength = len(sameSizedTrees)
    if listLength == 0:
        print "No duplicate nodes found."
    else:
        if listLength > 3:
            listLength = 3
    for showLargest in range(0,listLength):
        print "Largest node, rank:",showLargest+1
        for node in sameSizedTrees[showLargest]:
            print node, archive.data[node].size/1000000.0,"MB"
    print ""
    
    dupList = archive.listLargestDuplicateTrees(2)
    pprint.pprint(dupList)
    
    candidate = photoData("C:\\Users\\scott_jackson\\Pictures\\20110217 Herzaliya - Copy")
    print isNodeInArchive(archive, candidate)
    for file in candidate.data.keys():
        print file, candidate.data[file].timestamp
#        print "Node is in archive"
#    else:
#        print "something else"
        
    
if __name__ == '__main__':
    main()