'''
Created on Oct 21, 2011

@author: scott_jackson

TODO:
1. Think about how this will behave filling out database with new roots (e.g. root changes)
2. Make sure we can use one database for comparisons DONE
'''
import sys
from collections import defaultdict
import stopwatch
from photo_utils import print_now, environment
import photo_functions

def main():
    env = environment('Configuration')
#Get the archive database
#    archive = photo_functions.get_photo_data(None, env.get('archivepickle'))
    archive = photo_functions.get_photo_data(env.get('archive'), env.get('archivepickle'))
#    archive.dump_pickle()  #TODO make sure pickle dumps happen at the right places
    print "For {0}".format(archive.path)
    photo_functions.print_statistics(archive)
#    photo_functions.print_zero_length_files(archive)
#    photo_functions.print_tree(archive)

    #Now get candidate database
    #candidate = photo_functions.get_photo_data(env.get('candidate'), env.get('candidatepickle'))
#    candidate = photo_functions.get_photo_data(None, env.get('candidatepickle'))
#    candidate.extract_populate_tags()
#    photo_functions.print_statistics(candidate)
#    photo_functions.print_zero_files(candidate)
    
    photo_functions.populate_duplicate_candidates(archive, archive)
    
    duparray=defaultdict()
    for photo in archive.photo.keys():
        if len(archive.photo[photo].candidates) != 0:
#            print "Main Duplicate:",photo,archive.photo[photo].candidates
            duparray[archive.photo[photo].size * len(archive.photo[photo].candidates)] = photo
    print "Number of duplicate files = ",len(duparray)
    orderedkeys = sorted(duparray.keys(), reverse = True)
    count=0
    for x in orderedkeys:
        print duparray[x], x/1000000000, "GB", archive.photo[duparray[x]].candidates
        for y in archive.photo[duparray[x]].candidates:
            print "    ",y, ">",archive.photo[y].signature,"<", archive.photo[y].size
        count += 1
        if count > 10: break
    sys.exit()

    print_now("Looking for duplicates...")
    timer = stopwatch.stopWatch()
    nodeInArchive = photo_functions.is_node_in_archive(archive, candidate)
    print_now("Done. Elapsed time:" + str(timer.read()))
        
    if nodeInArchive:
        print "Whole node is in archive"
    else:
        print "Part of node is in archive"
    print "Files in archive:"
    for candidateFile in candidate.photo.keys():
        if not candidate.photo[candidateFile].isdir and candidate.photo[candidateFile].inArchive:
            print candidateFile, candidate.photo[candidateFile].candidates
    print "Files not in archive:"
    for candidateFile in candidate.photo.keys():
        if not candidate.photo[candidateFile].isdir and not candidate.photo[candidateFile].inArchive:
            print candidateFile, candidate.photo[candidateFile].timestamp    
    print "Done"
#Probably should clean the duplicate status etc. between runs to avoid carry forward from pickle          
    
if __name__ == '__main__':
    main()
