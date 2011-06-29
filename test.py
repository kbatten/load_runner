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
 -p <processes>   num processes
 -t <threads>     num threads
"""
    sys.exit(r)

class Config(object):
    def __init__(self):
        self.servers = []
        self.port = 11211
        self.itemcount = 100
        self.minsize = 1024
        self.maxsize = 1024
        self.prefix = ""
        self.loop = False
        self.operations = 5000
        self.set_percent = 20
        self.load_runner = True
        self.processes = 0
        self.threads = 0
        self.bulkload = False

class Server(object):
    def __init__(self,ip="localhost"):
        self.ip = ip

def parse_args(argv):
    config = Config()

    try:
        (opts, args) = getopt.getopt(argv[1:],
                                     'hs:i:m:M:K:lo:P:t:p:b', [])
    except IndexError:
        usage()
    except getopt.GetoptError, err:
        usage(err)

    for o, a in opts:
        if o == "-h":
            usage()
        elif o == "-s":
            hp = a.split(":")
            config.servers.append(hp[0])
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
        elif o == "-p":
            config.processes = int(a)
            config.threads = 0
        elif o == "-t":
            if not config.processes:
                config.threads = int(a)
        elif o == "-b":
            config.bulkload = True

    if not config.servers:
        config.servers = ['localhost']

    return config


if __name__ == "__main__":
    config = parse_args(sys.argv)

    task_info = {
        'server_info' : config.servers,
        'memcached_info' : {
            'bucket_name':"",
            'bucket_port':`config.port`,
            'bucket_password':"",
        },
        'operation_info' : {
            'operation_distribution':{'set':config.set_percent, 'get':(100-config.set_percent)},
            'valuesize_distribution':dict((k,1) for (k) in range(config.minsize,config.maxsize+1,1+(config.maxsize-config.minsize)/10)),
            'create_percent':25,
            'processes':config.processes,
            'threads':config.threads,
            'rate':0,
            'prefix':config.prefix,
            'cache_data':False,
        },
        'limit_info' : {
            'bulkload_size':0,
            'bulkload_items':config.itemcount,
            'size':0,
            'time':0,
            'items':0,
            'operations':config.operations,
        },
    }


    load = load_runner.LoadRunner(dryrun=False)
    if config.bulkload:
        load.add_bulkload_task(task_info)
    else:
        load.add_task(task_info)
    s = time.time()
    load.loop()
    e = time.time()
    print e-s


    sys.exit()


#    task_info['operation_info']['processes'] = 4
#    task_info['operation_info']['threads'] = 0
#    load = load_runner.LoadRunner(task_info=task_info, dryrun=True)
#    load.loop()
#    sys.exit()

    load = load_runner.LoadRunner(dryrun=False)

    tasknum = 10

    task_info['operation_info']['processes'] = tasknum
    task_info['operation_info']['threads'] = 0
    task_info['operation_info']['prefix'] = config.prefix+"p"
#    load.add_bulkload_task(task_info)
    task_info['operation_info']['processes'] = 0
    task_info['operation_info']['threads'] = tasknum
    task_info['operation_info']['prefix'] = config.prefix+"t"
    load.add_bulkload_task(task_info)
    load.loop()

    sys.exit()

    task_info['operation_info']['processes'] = tasknum
    task_info['operation_info']['threads'] = 0
    task_info['operation_info']['prefix'] = config.prefix+"p"
#    load.add_task(task_info)
    task_info['operation_info']['processes'] = 0
    task_info['operation_info']['threads'] = tasknum
    task_info['operation_info']['prefix'] = config.prefix+"t"
    load.add_task(task_info)
    load.loop()
#    load.start()
#    load.wait(2)
