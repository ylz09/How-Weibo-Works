#!/usr/bin/env python
import sys
# maps words to their counts
word2count = {}
 
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words while removing any empty strings
    words = filter(lambda word: word, line.split())
    # increase counters
    for word in words:
        print '%s\t%s' % (word, 1)
