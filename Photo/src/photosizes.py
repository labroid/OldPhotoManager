'''
Created on Apr 28, 2014

@author: scott_jackson
'''

import os
import os.path
from PIL import Image
import collections

def main():
    def printstats():
        for index in imcount.keys():
            print index, imcount[index], imsize[index]/1e6
        print "Bigcount: {}, Bigsize: {}, Smallcount: {}, Smallsize: {}".format(big_count, big_size/1e6, small_count, small_size/1e6)
        
    imcount = collections.defaultdict(int)
    imsize = collections.defaultdict(int)
    
    filecount = 0
    big_count = 0
    big_size = 0
    small_count = 0
    small_size = 0
    for root, dirs, files in os.walk("C:/Users/scott_jackson/Pictures/Uploads"):
        for filename in files:
            path = os.path.normpath(os.path.join(root, filename))
           # path = unicode(path)
            #path = path.decode('utf8')
            
            try:
                filesize = os.stat(path).st_size
            except:
                print "Filename:",path
            try:
                image = Image.open(path)
            except:
                pass
            else:
                image_max_dim = max(image.size)
                imcount[image_max_dim] += 1
                imsize[image_max_dim] += filesize
                if image_max_dim > 2048:
                    big_count += 1
                    big_size += filesize
                else:
                    small_count += 1
                    small_size += filesize 
                filecount += 1
                if not filecount%1000:
                    print filecount
                    printstats()
    print "Done"
    printstats()



if __name__ == '__main__':
    main()