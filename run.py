# Copyright 2022 by Minh Nguyen. All rights reserved.
#     @author Minh Tu Nguyen
#     @email  nmtu.mia@gmail.com
# 
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""
!This file is NOT to be modified
"""

from worker.base.subscriber import Subscriber
import json
import os
from utils.utils import log
import threading
from worker_threads import worker_threads

# open('worker.log', 'w').write('') #? reset log


"""
Load configuration from config.json file.
This file HAS TO exist and contain these information.
This file will be provided when you create a module on the system.
"""
if not os.path.isfile('config.json'):
    log('Config file not exist', 'error')
    exit()
__CONFIG__ = json.load(open('config.json'))


"""
Define other threads to run
"""
wthreads = []
if worker_threads is not None and isinstance(worker_threads, list):
    wthreads = worker_threads


"""
Init SilentWorker 
"""
worker = Subscriber(__CONFIG__)


"""
worker's main thread. 
This is to be run no matter what.
"""
def main_wthread():
    worker.__subscribe__()



if __name__ == '__main__':
    tmain = threading.Thread(target=main_wthread, daemon=False)
    tmain.start()


    """ Run other optional threads """
    ts = {}
    if len(wthreads) > 0:
        for i,wthread in enumerate(wthreads):
            ts[i] = threading.Thread(target=wthread, args=(worker,), daemon=False)
            ts[i].start()

        #? if multiple threads, run join
        tmain.join()
        for i in range(len(wthreads)):
            ts[i].join()
