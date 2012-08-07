'''
Created on Oct 21, 2011

@author: scott_jackson

TODO:
1. Think about how this will behave filling out database with new roots (e.g. root changes)
2. Make sure we can use one database for comparisons
3. Make configuration file universal (use in all functions) if needed
'''
import sys
from collections import defaultdict
import stopwatch
#from photo_functions import isNodeInArchive, get_photo_data
from photo_utils import print_now, environment
import photo_functions

def main():
    env = environment('Configuration')
    timer = stopwatch.stopWatch()
    
    #Get the archive database
#    archive = photo_functions.get_photo_data(None, env.get('archivepickle'))
    archive = photo_functions.get_photo_data(env.get('archive'), env.get('archivepickle'))
#    archive.dump_pickle()
    print "For {0}".format(archive.path)
    photo_functions.print_statistics(archive)
    photo_functions.print_zero_length_files(archive)

    #Now get candidate database
    #candidate = photo_functions.get_photo_data(env.get('candidate'), env.get('candidatepickle'))
#    candidate = photo_functions.get_photo_data(None, env.get('candidatepickle'))
#    candidate.extract_populate_tags()
#    photo_functions.print_statistics(candidate)
#    photo_functions.print_zero_files(candidate)
    
    photo_functions.populate_duplicate_candidates(archive, archive)
    
    duparray=defaultdict()
    for photo in archive.data.keys():
        if len(archive.data[photo].candidates) != 0:
            duparray[archive.data[photo].size * len(archive.data[photo].candidates)] = photo
    print "Number of duplicate files = ",len(duparray)
    orderedkeys = sorted(duparray.keys(), reverse = True)
    count=0
    for x in orderedkeys:
        print duparray[x], x/1000000000, "GB", archive.data[duparray[x]].candidates
        for y in archive.data[duparray[x]].candidates:
            print "    ",y, ">",archive.data[y].signature,"<", archive.data[y].size
        count += 1
        if count > 3: break
    sys.exit()

    print_now("Looking for duplicates...")
    timer.start()
    nodeInArchive = photo_functions.isNodeInArchive(archive, candidate)
    print_now("Done. Elapsed time:" + str(timer.read()))
        
    if nodeInArchive:
        print "Whole node is in archive"
    else:
        print "Part of node is in archive"
    print "Files in archive:"
    for candidateFile in candidate.data.keys():
        if not candidate.data[candidateFile].isdir and candidate.data[candidateFile].inArchive:
            print candidateFile, candidate.data[candidateFile].candidates
    print "Files not in archive:"
    for candidateFile in candidate.data.keys():
        if not candidate.data[candidateFile].isdir and not candidate.data[candidateFile].inArchive:
            print candidateFile, candidate.data[candidateFile].timestamp    
    print "Done"
#Probably should clean the duplicate status etc. between runs to avoid carry forward from pickle          
    
if __name__ == '__main__':
    main()