
# import sys


import logging
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.propagate = False
fh = logging.FileHandler(filename = __file__ + '.log', mode = 'w')
frmt = logging.Formatter(fmt = '%(levelname)s ; %(asctime)s ; %(message)s', 
                        datefmt = '%Y/%m/%d %I:%M:%S')
fh.setFormatter( frmt )
log.addHandler( fh )




log.info('sdfgsdfghdfhg')