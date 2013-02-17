'''
Created on Oct 21, 2011

@author: scott_jackson

'''
import sys
import logging
from collections import defaultdict
from photo_utils import environment
from operator import itemgetter
import photo_functions
import photo
import pickle_manager

def main():
    env = environment('Configuration')
    logger = logging.getLogger()
    
#Get the archive database
#    archive = photo_functions.get_photo_data(None, env.get('archivepickle'))
#    archive = photo_functions.get_photo_data(env.get('archive'), env.get('archivepickle'))
#    print "For {0}:{1}".format(archive.host, archive.path)
#    photo_functions.print_statistics(archive)
#    photo_functions.print_zero_length_files(archive)
#    photo_functions.print_tree(archive)
#    print "Finished!"
#    sys.exit()
    
#Now get candidate database
#    candidate = photo_functions.get_photo_data(env.get('candidate'), env.get('candidatepickle'))
    candidate = photo.get_photo_data(None, env.get('candidatepickle'))
    print "For {0}:{1}".format(candidate.host, candidate.path)
    photo_functions.print_statistics(candidate)
#    photo_functions.print_zero_files(candidate)
    target = "/home/shared/Photos/2011"
    photo_functions.populate_duplicate_candidates(candidate, candidate, node_path = target) #Maybe this should be a separate structure so it won't double-write...
    status = photo_functions.recurse_node_inclusion_check(candidate, node_path = target)
    print "Is candidate in archive?:", status
    photo_functions.print_tree(candidate, top = target)
#    print "Biggest duplicate nodes:"
#    photo_functions.print_largest_duplicates(candidate)
    print "Finished!"
#    sys.exit()
    

#    
if __name__ == '__main__':
    main()

