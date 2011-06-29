# TASK_INFO format
#
#    task_info = {
#        'server_info' : [ip1, ip2],
#        'memcached_info' : {
#            'bucket_name':"",
#            'bucket_port':11211,
#            'bucket_password':"",
#        },
#        'operation_info' : {
#            'operation_distribution':{'set':3,'get':7},
#            'valuesize_distribution':{30:3,100:3,5000:10},
#            'create_percent':25,
#            'processes':4,
#            'threads':0,
#            'rate':1000,
#            'prefix':'foo',
#            'cache_data':True,
#        },
#        'limit_info' : {
#            'bulkload_size':0,
#            'bulkload_items':50000,
#            'size':0,
#            'time':300,
#            'items':0,
#            'operations':0,
#        },
#    }


try:
    import multiprocessing
except:
    class multiprocessing(object):
        Process = object

import threading
import time
import random
from copy import deepcopy

# if uuid doesn't exist (python 2.4) then just return something with similar properties
try:
    from uuid import uuid3
except:
    def uuid3(namespace, name):
        return namespace+name
try:
    from uuid import uuid4
except:
    def uuid4():
        return `random.random()`

try:
    import json
except:
    import simplejson as json

try:
    from fractions import gcd
except:
    # gdc from python 2.6
    def gcd(a, b):
        """Calculate the Greatest Common Divisor of a and b.

        Unless b==0, the result will have the same sign as b (so that when
        b is divided by it, the result comes out positive).
        """
        while b:
            a, b = b, a%b
        return a

import mc_bin_client


##########


class FakeMemcachedClient(object):
    def __init__(self, *args):
        self.db = {}
    def set(self, key, exp, flags, val):
        self.db[key] = val
    def get(self, key):
        return (0,0,self.db[key])
    def add(self, key, exp, flags, val):
        self.db[key] = val
    def delete(self, key):
        del self.db[key]
    def sasl_auth_plain(self, *args):
        pass


class LoadSubtask(object):
    def __init__(self, subtask_info):
        self.subtask_info = deepcopy(subtask_info)

        # thread state info
        self.paused = True
        self.stopped = False

        self.mutation_index = 0
        self.get_index = 0
        self.value_failures = 0
        self.backoff = 0

        self.mutation_index_max = 0
        self.items = 0
        self.operations = 0
        self.time = 0
        self.size = 0
        self.operation_rate = 0

        # cache info
        self.cache_data = subtask_info['operation_info'].get('cache_data', False)
        self.data_cache = {}

        # server info
        self.server_ip = subtask_info['server_info'][0]
        self.server_port = int(subtask_info['memcached_info'].get('bucket_port', 11211))
        self.bucket_name = subtask_info['memcached_info'].get('bucket_name', '')
        self.bucket_password = subtask_info['memcached_info'].get('bucket_password', '')

        # operation info
        self.create = subtask_info['operation_info']['create_percent'] / gcd(subtask_info['operation_info']['create_percent'], 100 - subtask_info['operation_info']['create_percent'])
        self.nocreate = (100 - subtask_info['operation_info']['create_percent']) / gcd(subtask_info['operation_info']['create_percent'], 100 - subtask_info['operation_info']['create_percent'])

        self.operation_sequence = []
        for op in subtask_info['operation_info']['operation_distribution']:
            for i in range(subtask_info['operation_info']['operation_distribution'][op]):
                self.operation_sequence.append(op)

        self.valuesize_sequence = []
        for op in subtask_info['operation_info']['valuesize_distribution']:
            for i in range(subtask_info['operation_info']['valuesize_distribution'][op]):
                self.valuesize_sequence.append(op)

        self.max_operation_rate = int(subtask_info['operation_info'].get('rate', 0))

        self.key_prefix = subtask_info['operation_info'].get('prefix', '')

        self.uuid = uuid4()

        # limit info
        # all but time needs to be divided equally amongst threads
        self.limit_items = int(subtask_info['limit_info'].get('items', 0))
        self.limit_operations = int(subtask_info['limit_info'].get('operations', 0))
        self.limit_time = int(subtask_info['limit_info'].get('time', 0))
        self.limit_size = int(subtask_info['limit_info'].get('size', 0))

        # connect
        self.server = mc_bin_client.MemcachedClient(self.server_ip, self.server_port)
        if self.bucket_name or self.bucket_password:
            self.server.sasl_auth_plain(self.bucket_name,self.bucket_password)


    def unpause(self):
        self.paused = False

    def pause(self):
        self.paused = True

    def stop(self):
        self.stopped = True

    def update_state(self):
        pass


    def run(self):
        while True:
            # handle stop/pause
            self.update_state()
            while self.paused:
                if self.stopped:
                    return
                time.sleep(1)
                self.update_state()
            if self.stopped:
                return

            start_time = time.time()

            # stop thread if we hit a limit (first limit we hit ends the thread)
            if self.limit_items and self.items >= self.limit_items-1:
                return
            if self.limit_operations and self.operations >= self.limit_operations-1:
                return
            if self.limit_time and self.time >= self.limit_time:
                return
            if self.limit_size and self.size >= self.limit_size:
                return

# xxx
#            if self.limit_items and self.items % 10 == 0:
#                print "items: " + `self.items` + " < " + `self.limit_items`

            # rate limit if we need to
            if self.max_operation_rate and self.operation_rate > self.max_operation_rate:
                time.sleep(0.5/float(self.max_operation_rate))

            # do the actual work
            operation = self.get_operation()
            if operation == 'set':
                key = self.key_prefix + self.name + '_' + `self.get_mutation_key()`
                try:
#                    print `self.mutation_index` + " : " + `self.get_mutation_key()`
                    self.server.set(key, 0, 0, self.get_json_data())
                    self.operations += 1
                    self.backoff -= 1

                    # update number of items
                    # for now the only time we have new items is with ever increasing mutation key indexes
                    if self.get_mutation_key() > self.mutation_index_max:
                        self.mutation_index_max = self.get_mutation_key()
                        # looks like this will miss the first mutation
                        self.items += 1

                    # TODO: verify that this works, we may need to take the second to max index
                    # update the size of all data (values, not keys) that is in the system
                    # this can be either from new keys or overwriting old keys
                    prev_indexes = self.get_mutation_indexes(self.get_mutation_key())
                    prev_size = 0
                    if prev_indexes:
                        prev_size = self.get_data_size(max(prev_indexes))
                    self.size += self.get_data_size() - prev_size

                    self.mutation_index += 1
                except mc_bin_client.MemcachedError, e:
                    if self.backoff < 0:
                        self.backoff = 0
                    if self.backoff > 30:
                        self.backoff = 30
                    self.backoff += 1
                    # temporary error
                    if e.status == 134:
                        time.sleep(self.backoff)
                    else:
                        print `time.time()` + ' ' + self.name + ' set(' + `self.backoff` + ') ',
                        print e
                        time.sleep(self.backoff)
            elif operation == 'get':
                key = self.key_prefix + self.name + '_' + `self.get_get_key()`
                try:
                    vdata = self.server.get(key)
                    self.operations += 1
                    self.backoff -= 1
                    data = vdata[2]
                    try:
                        # with json data, this direct comparison may not be sufficient
                        data_expected = self.get_json_data(max(self.get_mutation_indexes(self.get_get_key())))
                        if data != data_expected:
                            self.value_failures += 1
                            raise Exception
                    except :
#                        print e
                        print "len(data) "+`len(data)`+" != len(data_expected) "+`len(data_expected)`
#                        print self.server.db
#                        print "create: " + `self.create`
#                        print "nocreate: " + `self.nocreate`
#                        print "get_index: " + `self.get_index`
#                        print "get_key: " + `self.get_get_key()`
#                        print "mutation_index_max: " + `self.mutation_index_max`
#                        print "mutation_indexes: " + `self.get_mutation_indexes(self.get_get_key())`
#                        print "getting data for mutation index: " + `max(self.get_mutation_indexes(self.get_get_key()))`
#                        print "got:      \'" + data + "\'"
#                        print "expected: \'" + data_expected + "\'"
#                        raise ValueError
                    self.get_index += 1
                except mc_bin_client.MemcachedError, e:
                    if self.backoff < 0:
                        self.backoff = 0
                    if self.backoff > 30:
                        self.backoff = 30
                    self.backoff += 1
                    print `time.time()` + ' ' + self.name + ' get(' + `self.backoff` + ') ',
                    print e
                    time.sleep(self.backoff)

            end_time = time.time()
            self.time += (end_time-start_time)
            self.operation_rate = (float(self.operations) / self.time)

    # get the current operation based on the get and mutation indexes
    def get_operation(self):
        return self.operation_sequence[(self.mutation_index + self.get_index) % len(self.operation_sequence)]

    # mutation_index -> mutation_key : based on create/nocreate
    def get_mutation_key(self, index=None):
        if index == None:
            index = self.mutation_index
        return index-self.nocreate*(index/(self.create+self.nocreate))

    # get_index -> get_key : based on get_index % mutation_index
    def get_get_key(self):
        return self.get_index % self.get_mutation_key(self.mutation_index)

    # key -> mutation_indexes
    def get_mutation_indexes(self, key):
        # starting point to find an index
        s=key*(self.nocreate+self.create)/self.create

        mutation_indexes=[]

        # for now we will scan all possible gcs even though we knows the step size
        # if we could guarentee that our calculated s was actually a gc it would be faster
        # scan a range (right now nocreate^2) incrementing by 1
        #  once we find a valid index, increment by nocreate
        index = s-(self.nocreate*self.nocreate)
        if index < 0:
            index = 0
        index_max = s+(self.nocreate*self.nocreate)
        incr = 1
        while index <= index_max and incr > 0:
            if index >= 0 and index <= self.mutation_index and self.get_mutation_key(index) == key:
                incr = self.nocreate
                mutation_indexes.append(index)
            index += incr
        return mutation_indexes

    # mutation_index -> mutation_data : based on create/nocreate
    def get_data(self, index=None):
        if index == None:
            index = self.mutation_index

        valuesize = self.valuesize_sequence[index % len(self.valuesize_sequence)]
        if self.cache_data:
            if not valuesize in self.data_cache:
                self.data_cache[valuesize] = (str(uuid3(self.uuid,`index`)) * (1+valuesize/36))[:valuesize]
            return `index` + self.data_cache[valuesize]
        else:
            return (str(uuid3(self.uuid,`index`)) * (1+valuesize/36))[:valuesize]

    # mutation_index -> mutation_data : based on create/nocreate
    # shortcut for getting the expected size of a mutation without generating the data
    def get_data_size(self, index=None):
        if index == None:
            index = self.mutation_index

        valuesize = self.valuesize_sequence[index % len(self.valuesize_sequence)]
        return valuesize

    def get_json_data(self, index=None):
        if index == None:
            index = self.mutation_index

        valuesize = self.valuesize_sequence[index % len(self.valuesize_sequence)]
        if self.cache_data:
            if not valuesize in self.data_cache:
                self.data_cache[valuesize] = (str(uuid3(self.uuid,`index`)) * (1+valuesize/36))[:valuesize]
            return json.dumps({'index':index,'data':self.data_cache[valuesize],'size':valuesize})
        else:
            return json.dumps({'index':index,'data':(str(uuid3(self.uuid,`index`)) * (1+valuesize/36))[:valuesize],'size':valuesize})

    def log(self, msg):
        pass
#        print name + ":" + msg


class LoadProcess(LoadSubtask, multiprocessing.Process):
    def __init__(self, subtask_info):
        LoadSubtask.__init__(self, subtask_info)
        self.msg_queue = multiprocessing.Queue()
        multiprocessing.Process.__init__(self, args=(self.msg_queue,))
        self.daemon = True

        # if name wasn't specified (2.4), create one
        try:
            self.name
        except AttributeError:
            self.name = "Process-"+subtask_info['operation_info']['prefix']

        self.log("Created")


    def unpause(self):
        self.msg_queue.put("unpause")

    def pause(self):
        self.msg_queue.put("pause")

    def stop(self):
        self.msg_queue.put("stop")

    def update_state(self):
        while self.msg_queue and not self.msg_queue.empty():
            msg = self.msg_queue.get()
            if msg == "unpause":
                self.paused = False
            elif msg == "pause":
                self.paused = True
            elif msg == "stop":
                self.stopped = True

    def log(self, msg):
        pass
#        print "LoadTask:LoadProcess:"+self.name+": " + msg


class LoadThread(LoadSubtask, threading.Thread):
    def __init__(self, subtask_info):
        LoadSubtask.__init__(self, subtask_info)
        threading.Thread.__init__(self)
        self.daemon = True

        # if name wasn't specified (2.4), create one
        try:
            self.name
        except AttributeError:
            self.name = "Thread-"+subtask_info['operation_info']['prefix']

        self.log("Created")

        # python 2.4 fix
    def is_alive(self):
        try:
            return threading.Thread.is_alive(self)
        except AttributeError:
            return threading.Thread.isAlive(self)

    def log(self, msg):
        pass
#        print "LoadTask:LoadThread:"+self.name+": " + msg


class LoadTask(object):
    def __init__(self, taskid, task_info):
        self.task_info = deepcopy(task_info)
        self.taskid = taskid
        self.subtasks = []

        self.log("Created")

        servers = self.task_info['server_info']

        # split options based on number of subtasks (either threads or processes)
        operation_info = self.task_info.get('operation_info', {})
        processes = operation_info.get('processes', 0)
        threads = operation_info.get('threads', 0)

        use_processes = True
        if processes > 0:
            subtasks = processes
        elif threads > 0:
            subtasks = threads
            use_processes = False
        if subtasks < len(servers):
            subtasks = len(servers)

        rate = operation_info.get('rate', 0) / subtasks
        prefix = operation_info.get('prefix', '')
        if use_processes:
            operation_info['processes'] = subtasks
            operation_info['threads'] = 0
        else:
            operation_info['processes'] = 0
            operation_info['threads'] = subtasks
        operation_info['rate'] = rate
        operation_info['prefix'] = prefix

        limit_info = self.task_info.get('limit_info', {})
        size = limit_info.get('size', 0) / subtasks
        items = limit_info.get('items', 0) / subtasks
        operations = limit_info.get('operations', 0) / subtasks
        limit_info['size'] = size
        limit_info['items'] = items
        limit_info['operations'] = operations

        subtask_info = deepcopy(self.task_info)
        for i in range(subtasks):
            subtask_info['server_info'] = [servers[i%len(servers)]]
            subtask_info['operation_info']['prefix'] = prefix + `i`
            if use_processes:
                t = LoadProcess(subtask_info)
            else:
                t = LoadThread(subtask_info)
            self.subtasks.append(t)
            t.start()


    def start(self):
        self.log("Started")
        for subtask in self.subtasks:
            subtask.unpause()

    def stop(self):
        for subtask in self.subtasks:
            subtask.stop()

    def pause(self):
        for subtask in self.subtasks:
            subtask.pause()

    # update list of subtasks, removing dead
    def update_subtasks(self):
        self.subtasks = [(v) for (v) in self.subtasks if v.is_alive()]

    def is_alive(self):
        alive = False
        self.update_subtasks()
        if len(self.subtasks) > 0:
            alive = True
        else:
            self.log("Died")

        return alive

    def log(self, msg):
        pass
#        print "LoadTask:"+self.taskid+": " + msg



class LoadRunner(object):
    def __init__(self, task_info=None, dryrun=False):
        self.log("Created")

        self.tasks = {}
        self.task_index = 0

        if dryrun:
            mc_bin_client.MemcachedClient = FakeMemcachedClient
            self.log("Running in DRYRUN mode")

        if task_info:
            self.add(task_info)

    # add a new task
    def add_task(self, task_info):
        self.task_index += 1
        self.tasks[`self.task_index`] = LoadTask(`self.task_index`, task_info)
        return self.task_index

    # add a new bulkload task
    def add_bulkload_task(self, task_info):
        bulkload_info = deepcopy(task_info)
        bulkload_info['operation_info']['operation_distribution'] = {'set':1}
        bulkload_info['operation_info']['create_percent'] = 100
        bulkload_info['operation_info']['rate'] = 0
        bulkload_info['limit_info']['items'] = bulkload_info['limit_info'].get('bulkload_items',0)
        bulkload_info['limit_info']['size'] = bulkload_info['limit_info'].get('bulkload_size',0)
        bulkload_info['limit_info']['time'] = 0
        bulkload_info['limit_info']['opertations'] = 0

        if bulkload_info['limit_info']['items'] == 0 and bulkload_info['limit_info']['size'] == 0:
            raise Exception

        self.task_index += 1
        self.tasks[`self.task_index`] = LoadTask(`self.task_index`, bulkload_info)
        return self.task_index

    # start up all or specific task
    def start(self, taskid=None):
        if taskid == None:
            for taskid,task in self.tasks.iteritems():
                task.start()
        else:
            self.tasks[taskid].start()

    # pause all or specific task
    def pause(self, taskid=None):
        if taskid == None:
            for taskid,task in self.tasks.iteritems():
                task.pause()
        else:
            self.tasks[taskid].pause()

    # stop all or specific task
    def stop(self, taskid=None):
        if taskid == None:
            for taskid,task in self.tasks.iteritems():
                task.stop()
        else:
            self.tasks[taskid].stop()

    # dump entire sequence of operations to a file
    def dump(self):
        pass

    # verify entire dataset is correct in membase at our current position
    def verify(self):
        pass

    # get the current state (num ops, percent complete, time elapsed)
    # also get the number of failed ops and failed add/set (failed adds due to item existing don't count)
    # return total ops, op breakdown (gets, sets, etc), total ops/s and ops/s breakdown (gets/s, sets/s, etc)
    def query(self):
        pass

    # start and wait
    def loop(self):
        self.start()
        self.wait()

    # update list of tasks, removing dead tasks
    def update_tasks(self):
        self.tasks = dict((k,v) for (k,v) in self.tasks.iteritems() if v.is_alive())

    # block till condition
    def wait(self, time_limit=None):
        if time_limit == None:
            while len(self.tasks) > 0:
                self.update_tasks()
                time.sleep(1)

        else:
            start_time = time.time()
            end_time = start_time + time_limit
            while len(self.tasks) > 0 and time.time() <= end_time:
                self.update_tasks()
                time.sleep(1)

    def log(self, msg):
        pass
#        print "LoadRunner: " + msg

