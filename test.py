#!/usr/bin/env python

import random
import getopt
import sys
import time
try:
    import json
except:
    import simplejson as json

import load_runner

def usage(err=None):
    if err:
        print "Error: %s\n" % err
        r = 1
    else:
        r = 0

    print """\
Syntax: testrunner [options]

Options:
 -s <host:port>   hostname:port to connect to
 -i <itemcount>   number of items to work with
 -o <operations>  number of operations to do
 -m <minsize>     min size of items
 -M <maxsize>     max size of items
 -K <prefix>      item key prefix
 -l               loop over keys
 -P <percent>     percent of sets
"""
    sys.exit(r)

class Config(object):
    def __init__(self):
        self.server = "localhost"
        self.port = 11211
        self.itemcount = 100000
        self.minsize = 1024
        self.maxsize = 1024
        self.prefix = ""
        self.loop = False
        self.operations = 100000
        self.set_percent = 20
        self.load_runner = True

class Server(object):
    def __init__(self,ip="localhost"):
        self.ip = ip

def parse_args(argv):
    config = Config()

    try:
        (opts, args) = getopt.getopt(argv[1:],
                                     'h:s:i:m:M:K:lo:P:', [])
    except IndexError:
        usage()
    except getopt.GetoptError, err:
        usage(err)

    for o, a in opts:
        if o == "-h":
            usage()
        elif o == "-s":
            hp = a.split(":")
            config.server = hp[0]
            if len(hp) > 1:
                config.port = int(hp[1])
            else:
                config.port = 11211
        elif o == "-i":
            config.itemcount = int(a)
        elif o == "-m":
            config.minsize = int(a)
        elif o == "-M":
            config.maxsize = int(a)
        elif o == "-K":
            config.prefix = a
        elif o == "-l":
            config.loop = True
        elif o == "-o":
            config.operations = int(a)
        elif o == "-P":
            config.set_percent = int(a)

    return config


if __name__ == "__main__":
    config = parse_args(sys.argv)


    load_info = {
        'server_info' : [Server(config.server)],
        'memcached_info' : {
            'bucket_name':"",
            'bucket_port':`config.port`,
            'bucket_password':"",
        },
        'operation_info' : {
            'operation_distribution':{'set':config.set_percent, 'get':(100-config.set_percent)},
            'valuesize_distribution':dict((k,1) for (k) in range(config.minsize,config.maxsize+1,1+(config.maxsize-config.minsize)/10)),
            'create_percent':25,
            'threads':4,
            'operation_rate':250,
            'key_prefix':config.prefix,
        },
        'limit_info' : {
            'max_size':0,
            'max_time':0,
            'items':config.itemcount,
            'operations':config.operations,
        },
    }
    
    load = load_runner.LoadRunner(load_info, dryrun=False)
    load.loop()
