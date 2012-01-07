'''
Created on Oct 21, 2011

@author: scott_jackson
'''
import sys
import pprint
import stopwatch
from photoFunctions import isNodeInArchive, getTagsFromFile, getTimestampFromTags
from photoData import photoData
from photoUtils import printNow, environment
from pickleManager import photoPickler

def main():
    env = environment()
    timer = stopwatch.stopWatch()
    pickle = photoPickler(env.options['archivepickle'])
    if pickle.pickleExists:                
        archive = pickle.loadPickle()
    else:
        printNow("No pickle found.  Building Photo database.") 
        timer.start()     
        archive = photoData(env.options['archive'])
    print "Number of files: ", len(archive.data)
    print "Elapsed Time:",timer.read()
    print ""
    
    printNow("Extracting tags")
    timer.start()
    tagsChanged = archive.extractTags() 
    if tagsChanged:
        print "Tags extracted.  Elapsed time:", timer.read(), "Picking..."
        timer.start()
        pickle.dumpPickle(archive)
        print "Pickeled.  Elapsed time:", timer.read()
    else:
        print "No tags changed."
        
#    printNow("Extracting tags 2")
#    timer.start()
#    tagsChanged = archive.extractTags2() 
#    if tagsChanged:
#        print "Tags extracted.  Elapsed time:", timer.read()
#    else:
#        print "No tags changed."
    
    print "Zero-length files:"
    zeroFiles = archive.listZeroLengthFiles()
    if len(zeroFiles) == 0:
        print "No zero-length files."
    else:
        for names in zeroFiles:
            print names
        print ""
          
    timer.start()
    sameSizedTrees = archive.listSameSizedTrees()
    print "Top same-sized nodes(found in", timer.read(),"seconds):"
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
    
    timer.start()
    print "Checking Checksums to confirm duplication"
    dupList = archive.listLargestDuplicateTrees(2)
    print "Checksums completed in",timer.read(),"Seconds."
    pprint.pprint(dupList)
    
    #Re-pickle with hashes
    pickle.dumpPickle(archive)
    
    printNow("Checking candidate directory for inclusion in archive")    
    candidate = photoData("C:\\Users\\scott_jackson\\Pictures\\20110217 Herzaliya - Copy")
    candidate.extractTags()
    candidateList = isNodeInArchive(archive, candidate)
    print "There are", len(candidateList), "candidates:"
    for file in candidateList:
        print file, candidate.data[file].timestamp
#        print "Node is in archive"
#    else:
#        print "something else"
    print "Done"
    sys.exit(0)          
    
if __name__ == '__main__':
    main()