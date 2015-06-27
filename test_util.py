<<<<<<< HEAD
import threading
import time
f = open("./.gitignore", "r")

lock = threading.Lock()

class mythread(threading.Thread):
    def __init__(self, num):
        threading.Thread.__init__(self)
        self.num = num

    def run(self):
        global f
        while True:
            with lock:
                line = f.readline()
                if len(line) == 0:
                    global start
                    end = time.time()
                    print end - start
                    return
                #print "thread ",self.num," ",line
                #time.sleep(0.1)

t1 = mythread(1)
t2 = mythread(2)
print t1.daemon
start = time.time()
t1.start()
t2.start()
=======
#! /usr/bin/env python2.7
# coding=utf-8
import json

f = open("./data/output.json", "r")
line = f.readline().rstrip(",")
dict = json.loads(line)
print dict['labels']['en']
print dict.keys()
print dict['sitelinks']['enwiki']
print dict['descriptions']['en']['value']
>>>>>>> origin/master
