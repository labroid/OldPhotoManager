'''
Created on Oct 21, 2011

@author: scott_jackson

TODO:
1. Think about how this will behave filling out database with new roots (e.g. root changes)
2. Make sure we can use one database for comparisons
3. Make configuration file universal (use in all functions) if needed
'''
import os.path
import logging
import stopwatch
from photo_functions import isNodeInArchive, count_unique_photos
from photoData import photoData, get_photo_data
from photo_utils import print_now, environment
from pickle_manager import photo_pickler

def main():
    env = environment()
    timer = stopwatch.stopWatch()
    logging.basicConfig(filename = env.options['logfile'], level = logging.DEBUG, filemode = 'w')
    
    archive_file = env.options['archive']
    archive_pickle_file = env.options['archivepickle']    
    archive = get_photo_data(archive_file, archive_pickle_file)
    archive.node_statistics()
    archive.extract_tags()
    
    result = count_unique_photos(archive)
    print "Directories: {0}, Files: {1}, Unique photos: {2}, Duplicates: {3} ({4:.2%})".format(result.dircount, result.filecount, result.unique_count, result.dup_count, result.dup_fraction)

    
    zeroFiles = archive.listZeroLengthFiles()
    if len(zeroFiles) == 0:
        print "No zero-length files."
    else:
        print "Zero-length files:"
        for names in zeroFiles:
            print names
        print ""
          
    timer.start()
    print_now("Checking candidate directory for inclusion in archive.  Scanning candidates..."),    
    candidate = get_photo_data("C:\\Users\\scott_jackson\\Pictures\\Uploads\\20120427 Zach Confirmation COPY", None)
    print_now("done.  Elapsed time:" + str(timer.read()) + " Extracting tags..."),
    timer.start()
    candidate.extract_tags()

    print_now("Looking for duplicates...")
    timer.start()
    nodeInArchive = isNodeInArchive(archive, candidate)
    print_now("Done. Elapsed time:" + str(timer.read()))
    if archive.datasetChanged:
        archive.pickle.dumpPickle(archive)
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