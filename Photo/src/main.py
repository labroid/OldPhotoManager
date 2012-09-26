'''
Created on Oct 21, 2011

@author: scott_jackson

'''
import sys
import logging
from collections import defaultdict
from photo_utils import environment
import photo_functions
import pickle_manager

def main():
    env = environment('Configuration')
    logger = logging.getLogger()
    
#Get the archive database
    archive = photo_functions.get_photo_data(None, env.get('archivepickle'))
#    archive = photo_functions.get_photo_data(env.get('archive'), env.get('archivepickle'))
    print "For {0}:{1}".format(archive.host, archive.path)
    photo_functions.print_statistics(archive)
#    photo_functions.print_zero_length_files(archive)
#    photo_functions.print_tree(archive)
#    print "Finished!"
#    sys.exit()
    
#Now get candidate database
    candidate = photo_functions.get_photo_data(env.get('candidate'), env.get('candidatepickle'))
#    candidate = photo_functions.get_photo_data(None, env.get('candidatepickle'))
    print "For {0}:{1}".format(candidate.host, candidate.path)
    photo_functions.print_statistics(candidate)
#    photo_functions.print_zero_files(candidate)
    photo_functions.populate_duplicate_candidates(archive, candidate) #Maybe this should be a separate structure so it won't double-write...
    logger.info("Finding if node is in archive (logged in main since function is recursive")
    status = photo_functions.is_node_in_archive(archive, candidate)
    print "Is candidate in archive?:", status
    photo_functions.print_tree(candidate)
    print "Finished!"
    sys.exit()
    
    duparray=defaultdict()
    for photo in archive.photo.keys():
        if len(archive.photo[photo].signature_match) != 0:
#            print "Main Duplicate:",photo,archive.photo[photo].signature_match
            duparray[archive.photo[photo].size * len(archive.photo[photo].signature_match)] = photo
    print "Number of duplicate files = ",len(duparray)
    orderedkeys = sorted(duparray.keys(), reverse = True)
    count=0
    for x in orderedkeys:
        print duparray[x], x/1000000000, "GB", archive.photo[duparray[x]].signature_match
        for y in archive.photo[duparray[x]].signature_match:
            print "    ",y, ">",archive.photo[y].signature,"<", archive.photo[y].size
        count += 1
        if count > 10: break
    sys.exit()

#    print_now("Looking for duplicates...")
#    timer = stopwatch.stopWatch()
#    nodeInArchive = photo_functions.is_node_in_archive(archive, candidate)
#    print_now("Done. Elapsed time:" + str(timer.read()))
#        
#    if nodeInArchive:
#        print "Whole node is in archive"
#    else:
#        print "Part of node is in archive"
#    print "Files in archive:"
#    for candidateFile in candidate.photo.keys():
#        if not candidate.photo[candidateFile].isdir and candidate.photo[candidateFile].inArchive:
#            print candidateFile, candidate.photo[candidateFile].signature_match
#    print "Files not in archive:"
#    for candidateFile in candidate.photo.keys():
#        if not candidate.photo[candidateFile].isdir and not candidate.photo[candidateFile].inArchive:
#            print candidateFile, candidate.photo[candidateFile].timestamp    
#    print "Done"
##Probably should clean the duplicate status etc. between runs to avoid carry forward from pickle          
#    
if __name__ == '__main__':
    main()

