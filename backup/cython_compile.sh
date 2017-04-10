#!/usr/bin/bash
cython --embed -o mp2.c mp2.py && \
gcc -v -Os -fPIC -fwrapv -Wall -fno-strict-aliasing -I/usr/include/python2.7 -L/usr/lib64 -o mp2 mp2.c -lpython2.7 -lpthread -lm -lutil -ldl &&\
ldd mp2
