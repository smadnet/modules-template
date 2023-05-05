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
Define multiple threads to run in silence
Has to define worker_threads.
---
These functions shall not have any other arguments other than `worker`.
`worker` points to the `SilentWorker` object. Use this to access functions and variables defined in the `silentworker.py`
"""

def worker_thread1(worker):
    print('[worker_thread1] worker.module_code', worker.module_code)
    return

def worker_thread2(worker):
    print('[worker_thread2] worker.module_code', worker.module_code)
    return


#! This has to be defined. An array of functions which you want to run as separate threads. If this is undefined, no declared functions defined in this file would be executed.
# worker_threads = [worker_thread1, worker_thread2]
worker_threads = []
