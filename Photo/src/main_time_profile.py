'''
Created on Oct 21, 2011

@author: scott_jackson
'''
import sys
import os.path
import socket
import pprint
import stopwatch
from photo_functions import isNodeInArchive, getTags, getTimestampFromTags
from photoData import photoData
from pickle import Pickler

#Machine names and paths
SMITHERS_HOSTNAME = "smithers"
SMITHERS_ARCHIVE = "/home/shared/Photos/2011"
SMITHERS_ARCHIVE_PICKLE = "/home/scott/PhotoPythonFiles/pickle.txt"
LAPTOP_HOSTNAME = "4DAA1001312"
LAPTOP_ARCHIVE = "C:\Users\scott_jackson\Pictures"
LAPTOP_ARCHIVE_PICKLE = "C:\Users\scott_jackson\Desktop\PhotoPickle.txt"


#import photoUnitData

def main():
    #Identify the environment
    machineName= socket.gethostname()
    if machineName == SMITHERS_HOSTNAME:
        archivePath = SMITHERS_ARCHIVE
        picklePath = SMITHERS_ARCHIVE_PICKLE
    elif machineName == LAPTOP_HOSTNAME:
        archivePath = LAPTOP_ARCHIVE
        picklePath = LAPTOP_ARCHIVE_PICKLE
    else:
        print "Error:  Unknown machine name:",machineName
        sys.exit(1)
        
    timer = stopwatch.stopWatch()
    print "Initializing photo database..."
    sys.stdout.flush()
    archive = photoData(archivePath)
    print "Number of file sizes measured: ", len(archive.data)
    print "Elapsed Time for file sizes:",timer.read()
    print ""
    
    print "Extracting tags"
    sys.stdout.flush()
    timer.start()
    for file in archive.data.keys():
        if not archive.data[file].isdir:
            getTags(file)
    print "Tags extracted.  Elapsed time:", timer.read()
    
    print "Converting timestamps"
    sys.stdout.flush()
    timer.start()
    for file in archive.data.keys():
        if not archive.data[file].isdir:
            getTimestampFromTags(archive.data[file].tags)
    print "time tags converted in:", timer.read()
    
    timer.start()
    print "Pickling results:"
    sys.stdout.flush()
    pickle_fp = open(picklePath,"w")
    pickle = Pickler(pickle_fp)
    pickle.dump(archive)
    print "Pickle done.  Elapsed time:",timer.read()
    
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
    print "Done"
    sys.exit(0)    
    
    candidate = photoData("C:\\Users\\scott_jackson\\Pictures\\20110217 Herzaliya - Copy")
    print isNodeInArchive(archive, candidate)
    for file in candidate.data.keys():
        print file, candidate.data[file].timestamp
#        print "Node is in archive"
#    else:
#        print "something else"
        
    
if __name__ == '__main__':
    main()
