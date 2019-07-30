#!/usr/bin/python2.7
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

import threading
import time
import random

class ActivePool(object):
    def __init__(self):
        super(ActivePool, self).__init__()
        self.active=[]
        self.lock=threading.Lock()
    def makeActive(self, name):
        with self.lock:
            self.active.append(name)
    def makeInactive(self, name):
        with self.lock:
            self.active.remove(name)
    def numActive(self):
        with self.lock:
            return len(self.active)
    def __str__(self):
        with self.lock:
            return str(self.active)

def worker(pool):
    name=threading.current_thread().name
    pool.makeActive(name)
    print 'Now running: %s' % str(pool)
    time.sleep(random.randint(1,3))
    pool.makeInactive(name)

if __name__=='__main__':
    poolA=ActivePool()
    poolB=ActivePool()
    jobs=[]
    for i in range(5):
        jobs.append(
            threading.Thread(target=worker, name='A{0}'.format(i),
                             args=(poolA,)))
        jobs.append(
            threading.Thread(target=worker, name='B{0}'.format(i),
                             args=(poolB,)))
    for j in jobs:
        j.daemon=True
        j.start()
    while threading.activeCount()>1:
        for j in jobs:
            j.join(1)
            print 'A-threads active: {0}, B-threads active: {1}'.format(
                poolA.numActive(),poolB.numActive())