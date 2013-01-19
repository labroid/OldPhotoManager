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
    print "Biggest duplicate nodes:"
    photo_functions.print_largest_duplicates(candidate)
    print "Finished!"
#    sys.exit()
    

#    
if __name__ == '__main__':
    main()

