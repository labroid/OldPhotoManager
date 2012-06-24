'''
Created on Oct 21, 2011

@author: scott_jackson
'''
import os.path
import logging
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
    extensions = set()
    for archiveFile in archive.data.keys():
        extensions.add(str.lower(os.path.splitext(archiveFile)[1]))
        if archive.data[archiveFile].dirflag:
            dircount += 1
        else:
            filecount += 1
    print "Total time:",timer.read(),"or",timer.read()/(filecount+dircount)*1000000.0,"us/file"
    print "Dircount:",dircount,"Filecount",filecount,"Total",dircount+filecount        
    print "File extensions in archive:", extensions
        
    printNow("Extracting tags")
    timer.start()
    archive.extractTags()
    if archive.datasetChanged:
        elapsedTime = timer.read()
        print "Tags extracted.  Elapsed time:", elapsedTime, "or", elapsedTime/filecount * 1000, "ms per file, or", elapsedTime/filecount * 100000/60, "Minutes for 100k files"
        pickle.dumpPickle(archive, verbose = True)
        archive.datasetChanged = False
    else:
        print "No tags changed."
    
    print "Counting unique photos"
    dupCount = 0
    timer.start()
    photoSet = set()
    for archiveFile in archive.data.keys():
        if not archive.data[archiveFile].dirflag:
            md5 = archive.data[archiveFile].thumbnailMD5
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
          
    timer.start()
    printNow("Checking candidate directory for inclusion in archive.  Scanning candidates..."),    
    candidate = photoData("C:\\Users\\scott_jackson\\Pictures\\Uploads\\20120427 Zach Confirmation COPY")
    printNow("done.  Elapsed time:" + str(timer.read()) + " Extracting tags..."),
    timer.start()
    candidate.extractTags()
    printNow("Candidate tags extracted.  Elapsed time:" + str(timer.read()))

    printNow("Looking for duplicates...")
    timer.start()
    nodeInArchive = isNodeInArchive(archive, candidate)
    printNow("Done. Elapsed time:" + str(timer.read()))
    if archive.datasetChanged:
        pickle.dumpPickle(archive, verbose = True)
        archive.datasetChanged = False 
        
    if nodeInArchive:
        print "Whole node is in archive"
    else:
        print "Part of node is in archive"
    print "Files in archive:"
    for candidateFile in candidate.data.keys():
        if not candidate.data[candidateFile].dirflag and candidate.data[candidateFile].inArchive:
            print candidateFile, candidate.data[candidateFile].candidates
    print "Files not in archive:"
    for candidateFile in candidate.data.keys():
        if not candidate.data[candidateFile].dirflag and not candidate.data[candidateFile].inArchive:
            print candidateFile, candidate.data[candidateFile].timestamp    
    print "Done"
#Probably should clean the duplicate status etc. between runs to avoid carry forward from pickle          
    
if __name__ == '__main__':
    main()