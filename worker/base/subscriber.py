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
#! This file is NOT to be modified
"""

from kafka import KafkaConsumer
from worker.silentworker import SilentWorker
from utils.utils import log
import json
import os
import base64


class Subscriber():
    """
    This class reads datastream from a topic named `task_<module_name>`
    and process the data

    We provide both options of tasking a modules to infer and to train.

    * For infer request, issue a message like this:
        {
            'analysis_request_ids': analysis_request_ids,

            'modules_flow_code': modflow_code,
            'config_data': config_data, #? of a module
            'callback_flow': self._modules_flows[modflow_code]['callback_flow'], #? 

            'processing_mode': 'batch', #? (optional, `batch` by default)

            'mode': 'infer', #? (optional, `infer` by default)
            'modules_modes': {'0':'infer','1':'infer',...}, #? (optional, all `infer` by default)

            'orig_path': orig_path,
            'path': path,
            'orig_hash': orig_hash,

            'timestamp': timestamp
        }
        for each record you want to infer

    * For train request, issue a message like this:
        {
            'analysis_request_ids': [analysis_request_trainid],

            'modules_flow_code': modflow_code, #? can pass a train request to a modules_flow, where inference output of the prior modules are input of a module that runs train
            'config_data': config_data, #? of a module
            'callback_flow': self._modules_flows[modflow_code]['callback_flow'], #?

            'processing_mode': 'batch', #? (optional, `batch` by default)

            'mode': 'train', #! (required) mode of the whole batch. In the above example, even when we declare the mode for the whole batch of requests is training, but when breaking down to `modules_modes` = {'0':infer,'1':train}, the first module (module 0) still takes the mode of infer.

            'train_options': {
                'analysis_request_trainid': analysis_request_trainid, #! (required) for train requests, this must be defined. All messages coming from the same train request must have the same value

                #? this is a bit different. for training mode, the dataset used for training can be huge, so it may be better to separate the ingesting and the processing. 
                #? one can have different ways to ingest data to the system (for batch learning), and it can vary depending on each system.
                #? so supposed we have all the data ingested, just pass the filepath that contains list of training data and use a customable function to readInputFiles 
                'filepath_inputs_train': filepath_inputs_train, #! (required) filepath that contains list of all inputs for train set (eg. list of filepaths, list of urls, csv file contains all training records)
                'filepath_orig_inputs_train': filepath_orig_inputs_train, #! (required) filepath that contains list of all original inputs for train set (before processed by some prior preprocessor modules)
                'filepath_orig_inputs_val': filepath_orig_inputs_val, #? (optional) filepath that contains list of all original inputs for validation set
                'filepath_inputs_val': filepath_inputs_val, #? (optional) filepath that contains list of all inputs for validation set
                'train_ratio': train_ratio, #? (optional) used when filepath_inputs_val not declared. to define the ratio of data inside filepath_inputs_train should be used for train and val. if both filepath_inputs_val and train_ratio not declared, all data will be used for training (no validation)
            }
            'modules_modes': {}, #! (required) for train requests, this must be defined, a dict that maps a `module_code` with its expected action. eg: {'0': 'infer', '1': 'train'}, meaning the module 0 should run infer method, and module 1 should run train method (if the callback_flow is [0,1] then infer output of module 0 is used for module 1's training input)
            'save_infer_result': True, #? (optional) used for train requests, declared this as True if you want to save output of modules prior to the trainer

            'timestamp': timestamp
        }
        for each training request

    * (Dev) For online learning (processing stream data), each message within the stream must be in format:
        {
            'analysis_request_ids': analysis_request_ids, #? similar to `infer` mode in `batch` processing

            'modules_flow_code': modflow_code, #? can pass a train request to a modules_flow, where inference output of the prior modules are input of a module that runs train
            'config_data': config_data, #? of a module
            'callback_flow': self._modules_flows[modflow_code]['callback_flow'], #?

            'processing_mode': 'stream', #! (required)

            'mode': '', #! will be ignored. it's up to each module to decide whether to retrain the model on the fly
            'modules_modes': {}, #! will be ignored.it's up to each module to decide whether to retrain the model on the fly

            'analysis_request_streamid': analysis_request_streamid, #! (required) for processing stream data, this must be defined. All messages coming from the same stream must have the same value

            #? similar to `infer` mode in `batch` processing
            'orig_path': orig_path,
            'path': path,
            'orig_hash': orig_hash,

            'timestamp': timestamp
        }
    """

    kafka_topic_name = 'task_<module_code>'

    #? initial config, this is used at initialization of SilentWorker object only
    #? config options could be overwritten at each request, under `config_data` options
    _config = {}


    def __init__(self, config) -> None:
        self._config = config

        if 'module_code' not in config:
            log('[x] No `module_code` defined. Exit')
            exit()
        if 'module_otype' not in config:
            log('[x] No `module_otype` defined. Exit')
            exit()
        if 'kafka_bootstrap_servers' not in config:
            log('[x] No `kafka_bootstrap_servers` defined. Exit')
            exit()
        if 'module_indir' not in config:
            log('[x] No `module_indir` defined. Exit')
            exit()

        #? `module_code` and `module_otype` cannot be changed
        self.module_code = config['module_code']
        self.module_otype = config['module_otype']
        
        #? where does this module access input files
        self.module_indir = config['module_indir']
        if len(self.module_indir) > 0 and not os.path.isdir(self.module_indir):
            os.makedirs(self.module_indir)


        self.kafka_bootstrap_servers = config['kafka_bootstrap_servers'] #! for subscriber, this cannot be changed. try catch function inside __subscribe__ should exit the code when the module failed to connect to kafka servers

        #? which topic should the module subscribe from (each module has a separate topic to receive task)
        self.kafka_topic_name = 'task_{}'.format(self.module_code)


        """
        self.workers: list of SilentWorker objects
        
        For inference task, we can safely use one object to process every requests,
        but for train task, we must separate each train request, since each train request needs distinctive inputs used as dataset
        
        Hence, we use a dict,
            - with one fixed key `inferer` that maps to a SilentInferer object, this one receives all infer requests and process by batch as configured
            - each input record from the same training request should have the same `analysis_request_trainid`, this will be used to distinguish different training requests.
        
        NOTE that a training task can be preceeded by another preprocessor module (for example, filepaths -> prp-disasm -> (train) cnn-asm). 
        The flow process is no difference than inference process, the only difference is that because we task module by each file (each file has one separate message, rather than one message contains the whole input batch) we will assign the same `analysis_request_trainid` for all requests coming from the same training request.
        """
        self.workers = {
            'inferer': SilentWorker(self._config), #? basic infer for the whole flow
            # ... #? each training request has a different SilentWorker object
            # ... #? for online learning (processing stream data), each stream source has a different SilentWorker object
        }

        self._inferer = self.workers['inferer']

        return
    

    def __processStream__(self, message) -> None:
        """ 
        For stream processing, the only difference is that the user can have more control over all data coming from the same `analysis_request_streamid` 
        but it still can utilise the batch processing (`num_per_batch` config option still available for use)
        """
        if 'analysis_request_streamid' not in message: #? required for stream type
            return

        analysis_request_streamid = message['analysis_request_streamid']

        if 'command' in message and message['command'] == 'kill': #? task to kill this stream processor
            if analysis_request_streamid in self.workers:
                del self.workers[analysis_request_streamid]

        #? create a SilentWorker object for this stream if it's not there.
        if analysis_request_streamid not in self.workers:
            options = {
                'processing_mode': 'stream',
                'analysis_request_streamid': analysis_request_streamid
            }
            self.workers[analysis_request_streamid] = SilentWorker(self._config, options)


        self.workers[analysis_request_streamid].__onReceiveMsg__(message, 
            self.workers[analysis_request_streamid].onChangeConfig, 
            self.workers[analysis_request_streamid].processStream, 
            None,
            None #? we need a separate message to tell when to stop this stream and kill the worker for this stream, it's processed above
        )
        return
    

    def __processBatch__(self, message) -> None:
        #? the mode of the whole batch for the whole flow is to train
        if 'mode' in message and message['mode'] == 'train':
            if 'train_options' not in message: #? required for train requests
                return

            train_options = message['train_options']

            #? each training request has a different `analysis_request_trainid`
            analysis_request_trainid = train_options['analysis_request_trainid']
            #? options for train
            filepath_orig_inputs_train = train_options['filepath_orig_inputs_train']
            filepath_inputs_train = train_options['filepath_inputs_train'] #! need to recheck the input patj
            filepath_orig_inputs_val = train_options['filepath_orig_inputs_val']
            filepath_inputs_val = train_options['filepath_inputs_val'] #! need to recheck the input patj
            train_ratio = train_options['train_ratio']

            #? batch mode for the whole flow is to `train`
            #? but which mode for this specific module ?
            module_mode = message['modules_modes'][self.module_code]

            #? save infer results of modules that run infer
            save_infer_result = bool(message['save_infer_result']) if 'save_infer_result' in message else False


            #? create a SilentWorker object for this train request.
            #! each train request should have 1 and ONLY 1 message
            options = {
                'mode': 'train',
                'analysis_request_trainid': analysis_request_trainid,
                'filepath_orig_inputs_train': filepath_orig_inputs_train,
                'filepath_orig_inputs_val': filepath_orig_inputs_val,
                'filepath_inputs_train': filepath_inputs_train,
                'filepath_inputs_val': filepath_inputs_val,
                'train_ratio': train_ratio,
                'module_mode': module_mode,
                'modules_modes': message['modules_modes'],
                'save_infer_result': save_infer_result,
            }
            self.workers[analysis_request_trainid] = SilentWorker(self._config, options)

            
            if module_mode == 'train': #? this module is tasked to run `train` method
                self.workers[analysis_request_trainid].__onReceiveMsg__(message, 
                    self.workers[analysis_request_trainid].onChangeConfig, 
                    self.workers[analysis_request_trainid].train, 
                    self.workers[analysis_request_trainid].readInputsFilepath, #? for train requests, this is required. points to the function where you processed input file and return lists of inputs
                    self.__cleanObject__)
                        
            else: #? the whole flow is to train, but this module is still tasked to do inference. Still run infer, inside __onFinishInfer__ will determine what to do, corresponding with each `mode` of the whole flow, `train` or `infer`
                self.workers[analysis_request_trainid].__onReceiveMsg__(message, 
                    self.workers[analysis_request_trainid].onChangeConfig, 
                    self.workers[analysis_request_trainid].infer, 
                    self.workers[analysis_request_trainid].train, 
                    self.workers[analysis_request_trainid].readInputsFilepath, #? for train requests, this is required. points to the function where you processed input file and return lists of inputs
                    self.__cleanObject__)

        else:
            #? callback function for `infer` requests
            self._inferer.__onReceiveMsg__(message, self._inferer.onChangeConfig, self._inferer.infer)

        return


    def __subscribe__(self) -> None:
        outdir = self.module_indir #? received files will be stored to module's input folder

        consumer = KafkaConsumer(
            self.kafka_topic_name,
            bootstrap_servers=self.kafka_bootstrap_servers,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=f'module_{self.module_code}',
            value_deserializer=lambda x: x.decode('utf-8')
        )
        log(f'[ ] Listening on {self.kafka_topic_name}')

        for message in consumer:
            # print(message)
            isend = False
            
            headers = message.headers

            if headers is None or len(headers) == 0:
                log(f'[x] headers cannot be empty', 'error')
                continue

            #* headers contain 5 required fields: [domain_id, capturer_id, filepath, isend, sinkfile]
            #? data sent for which file (if not sending file, this value will be empty: '')
            filepath = headers[2][1].decode('utf-8') #? filepath
            filename = os.path.basename(filepath)
            #? save to which file
            outpath = os.path.join(outdir, filename)

            isend = headers[3][1].decode('utf-8')
            if isinstance(isend, str):
                isend = True if isend.lower() in ['true', '1'] else False
            sinkfile = headers[4][1].decode('utf-8')
            if isinstance(sinkfile, str):
                sinkfile = True if sinkfile.lower() in ['true', '1'] else False
            
            #? when all chunks of the file are received
            if (sinkfile is True and isend is True) or sinkfile is False:
                if len(filepath) > 0:
                    log(f'\n---------------\n[+] Received and wrote all to {outpath}. Proceed...')
                else:
                    log(f'\n---------------\n[+] Received. Proceed...')

                #? is there any data sent along this ending message ?
                if len(message.value) > 0:
                    data = json.loads(message.value)
                    self.__processMsg__(data)
            
            elif isend is False: #? not end yet
                #? new chunk coming. decode content and store on machine
                payload = base64.b64decode(message.value)
                if len(payload) > 0:
                    #? append to it then
                    open(outpath, 'ab').write(payload)
                    # print(f'[+] Wrote to {outpath}')

        return



    def __processMsg__(self, message) -> None:
        # print(message)

        if 'analysis_request_ids' not in message:
            return

        #? processing stream data
        if 'processing_mode' in message and message['processing_mode'] == 'stream':
            self.__processStream__(message)

        else: #? batch processing
            self.__processBatch__(message)

        return



    def __cleanObject__(self, analysis_request_trainid) -> None:
        """
        Since each training request creates an object, after training is done, make sure to clean things up
        """
        del self.workers[analysis_request_trainid]
        return