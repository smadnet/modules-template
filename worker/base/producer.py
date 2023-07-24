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

from kafka import KafkaProducer
from utils.utils import log
import os
import base64
import time
import json
import shutil


class Producer():
    """
    This class reads datastream from a topic named `task_<module_name>`
    and process the data
    """

    #? predefined config (to initialize model)
    _config = {}
    module_code = ''
    module_type = ''
    module_otype = ''
    module_output_format = ''
    module_input_format = ''
    kafka_bootstrap_servers = None
    #? output dir for this module
    module_outdir = ''
    #? input dir for this module
    module_indir = ''

    _processing_mode = 'batch'

    _cleanCallbackFcn = None

    #? dict of analysis_request_ids
    #? map `orig_input_hash` => `array of analysis_request_id of files that have the same hash = orig_input_hash`
    _map_ohash_requestids = {}

    #? dict of modules_flow_code
    #? map `orig_input_hash` => `array of modules_flow_code`
    _map_ohash_modflow = {}

    #? map a `modules_flow_code` => `callback_flow` array (array of modules left in the flow to task)
    _map_modflow_flow = {}

    #? dict of original inputs that is fed to this module flow.
    #? map `orig_input_hash` => one `orig_path`.
    #? (multiple orig_path might have the same hash, but just map to one path)
    _map_ohash_oinputs = {}

    #? map_ohash_inputs: dict of inputs to feed this module (eg. filepath to the executable files already stored on the system / url).
    #? map `orig_input_hash` => `prev module's output for orig_path correspond with orig_input_hash`
    _map_ohash_inputs = {}


    #? config for publishing
    _topic2pool = '' #? pool of this module. If this module publishes files, data centres should sink files from this topic
    _publish_file = False
    _chunk_size = 8190
    _after_publish = 'keep'


    """ for stream processing """
    #? data coming from the same source must have the same streamid
    _analysis_request_streamid = None


    """ all things below are only of concern in `train` requests (message with `processing_mode` = 'batch' & `mode` = 'train') """
    #? mode for the whole batch of the flow (`mode` field in request)
    _mode = 'infer' #? by default
    #? mode (the object is for infer or train) (value in `modules_modes[self.module_code]` field in request)
    _module_mode = 'infer' #? by default
    #? dict that maps each module code to its desired action. passed at each request
    _modules_modes = {}
    #? used only when _mode = 'train', to declare if we want to store outputs of modules prior to the trainer
    _save_infer_result = False
    #? distinctive field for `train` requests
    _analysis_request_trainid = ''
    #? train options
    _filepath_inputs_train = None
    _filepath_orig_inputs_train = None
    _filepath_orig_inputs_val = None
    _filepath_inputs_val = None
    _train_ratio = None
    

    def __init__(self, config, options=None) -> None:
        #? initial config
        self._config = config

        #? basic and required
        if 'module_code' not in config:
            log('[x][Producer][__init__] No `module_code` defined. Exit', 'error')
            exit()
        if 'module_type' not in config:
            log('[x][Producer][__init__] No `module_type` defined. Exit', 'error')
            exit()
        if 'module_otype' not in config:
            log('[x][Producer][__init__] No `module_otype` defined. Exit', 'error')
            exit()
        if 'module_input_format' not in config:
            log('[x][Producer][__init__] No `module_input_format` defined. Exit', 'error')
            exit()
        if 'module_output_format' not in config:
            log('[x][Producer][__init__] No `module_output_format` defined. Exit', 'error')
            exit()
        if 'module_indir' not in config:
            log('[x][Producer][__init__] No `module_indir` defined. Exit', 'error')
            exit()
        if 'module_outdir' not in config:
            log('[x][Producer][__init__] No `module_outdir` defined. Exit', 'error')
            exit()
        if 'kafka_bootstrap_servers' not in config:
            log('[x][Producer][__init__] No `kafka_bootstrap_servers` defined. Exit', 'error')
            exit()
        
        #? still basic stuff
        self.module_code = config['module_code']
        self.module_type = config['module_type']
        self.module_otype = config['module_otype']
        self.module_input_format = config['module_input_format']
        self.module_output_format = config['module_output_format']
        self.kafka_bootstrap_servers = config['kafka_bootstrap_servers']
        #? set module_outdir. check dir. if not exist, create it
        self.__change_module_outdir__(config['module_outdir'])
        #? set module_indir. check dir. if not exist, create it
        self.__change_module_indir__(config['module_indir'])

        #? publishing configurations
        self._topic2pool = f'pool_from_{self.module_code}'
        self.__change_module_publishing__(config)


        #? producer to produce message
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)

        #? for `stream` processing
        if options is not None and 'analysis_request_streamid' in options and 'processing_mode' in options and options['processing_mode'] == 'stream':
            self._processing_mode = 'stream'
            self._analysis_request_streamid = options['analysis_request_streamid']

        #? for `train` request (`mode` = 'train')
        if (options is not None and 'analysis_request_trainid' in options and 'mode' in options and options['mode'] == 'train' \
            and 'filepath_inputs_train' in options and len(options['filepath_inputs_train']) > 0 \
            and 'filepath_orig_inputs_train' in options and len(options['filepath_orig_inputs_train']) > 0):

            self._mode = 'train'
            self._analysis_request_trainid = options['analysis_request_trainid']

            self._filepath_inputs_train = options['filepath_inputs_train']
            self._filepath_orig_inputs_train = options['filepath_orig_inputs_train']
            
            if 'filepath_inputs_val' in options and len(options['filepath_inputs_val']) > 0:
                self._filepath_inputs_val = options['filepath_inputs_val']
            if 'filepath_orig_inputs_val' in options and len(options['filepath_orig_inputs_val']) > 0:
                self._filepath_orig_inputs_val = options['filepath_orig_inputs_val']
            if 'train_ratio' in options:
                self._train_ratio = options['train_ratio']

            if 'module_mode' in options and options['module_mode'] in ['train', 'infer']:
                self._module_mode = options['module_mode']
            if 'modules_modes' in options and isinstance(options['modules_modes'], dict):
                self._modules_modes = options['modules_modes']
            if 'save_infer_result' in options:
                self._save_infer_result = options['save_infer_result']

        return


    def __change_module_publishing__(self, config) -> bool:
        """
        Is this module configured to publish data to data centre ?
        Note that this is different from sharing files to the next modules in chain.
        """
        if 'publishing' in config:
            if 'publish_file' in config['publishing']:
                self._publish_file = config['publishing']['publish_file']
            if 'chunk_size' in config['publishing']:
                self._chunk_size = int(config['publishing']['chunk_size'])
            if 'after_publish' in config['publishing']:
                self._after_publish = config['publishing']['after_publish'].strip().lower()
        return True


    def __change_module_outdir__(self, path) -> bool:
        """
        Each module has its own output dir to store its processed outputs.
        This function checks whether the output dir exists, if not, creates it.
        """
        self.module_outdir = path
        if len(self.module_outdir) == 0:
            log(f'[x][Producer][__change_module_outdir__] len(self.module_outdir) == 0', 'error')
            return False
        try:
            if not os.path.isdir(self.module_outdir):
                os.makedirs(self.module_outdir)
            return True
        except:
            log(f'[x][Producer][__change_module_outdir__] failed to create self.module_outdir = {self.module_outdir}', 'error')
            return False

    def __change_module_indir__(self, path) -> bool:
        """
        When data dir is not shared, on each machine, the module must have an input dir where its input files are put.
        This function checks whether the input dir exists, if not, creates it.
        """
        self.module_indir = path
        if len(self.module_indir) == 0:
            log(f'[x][Producer][__change_module_outdir__] len(self.module_indir) == 0', 'error')
            return False
        try:
            if not os.path.isdir(self.module_indir):
                os.makedirs(self.module_indir)
            return True
        except:
            log(f'[x][Producer][__change_module_indir__] failed to create self.module_indir = {self.module_indir}', 'error')
            return False
    


    def __reConnectKafka__(self, config) -> None:
        """
        Reconnect to kafka server
        """
        self.kafka_bootstrap_servers = config['kafka_bootstrap_servers']
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_bootstrap_servers)
        return



    def __findNextModules__(self, callback_flow):
        """
        Find next module to task

        To do that, first need to remove the module itself from the `callback_flow` (since this `callback_flow` will be sent to the next modules for those modules to identify which ones to send next after finishing analyzing)

        Some examples of `callback_flow` and how it should be interpreted:
            [0,1,2]         0->1->2
            [0,[1,2]]       0->(1, 2) (output of 0 sent to 1 and 2)
            [0,[[1,3],2]]   0->2, 0->1->3 (output of 0 sent to 1 and 2, output of 1 sent to 3)
            [0,[[1,[3,4]],2]]

        #? Important NOTE: 
            Of same level, dim of the "behind element" will always >= dim of the previous one, because output of 1 module can be sent to multiple modules, but one module can NOT receive outputs from multiple modules. Not now, damn too complicated.
        #? Important NOTE 2:
            In the egs in comments within this code block, I simplify by using only id as a `module` data, but in reality, instead of sending only string of `module_code`, we send the whole dict that contains: {'code': module_code, 'config': module_config}
            Therefore, where we need instance type == `str`, we actually want the instance to be `dict`
        """
        next_mods = {}  #? next modules to task. a dict that maps next_module_code => the callback_flow for that next module

        #? [1] => 1 | [[1,2]] => [1,2] | [0,[[1,3],2]] remains
        while isinstance(callback_flow, list) and len(callback_flow) == 1:
            callback_flow = callback_flow[0] #? prevent something like [[[[9]]]]

        log(f'      [ ][__findNextModules__] callback_flow = {callback_flow}')

        """ First thing first, remove this module in the callback_flow """
        if isinstance(callback_flow, list):  #? [1,2], [0,[[1,3],2]]
            log(f'      [ ][__findNextModules__] is list. Still something left in chain!')
            # if isinstance(callback_flow[0], str): #? first one must be a string
            if isinstance(callback_flow[0], dict): #? actually, dict instead
                #? the first element of the callback_flow should be this module (as string, not array), so remove the first one. but just to be sure, double-check it
                if callback_flow[0]['code'] == self.module_code:
                    #? eg. callback_flow = [0,[[1,3],2]]
                    #?     self.module_code == 0
                    #?     => new callback_flow = [[[1,3],2]]
                    del callback_flow[0]
                else:  #? if not as expected, definitely something wrong
                    return None

            else:  #? the first element of the callback_flow should be a string, and this module code. if it's not string, it's of wrong format. something's wrong
                return None

        # elif isinstance(callback_flow, str): #? if there's only one module in the flow, it should be string and it should be this module itself.
        elif isinstance(callback_flow, dict):  #? actually, dict instead
            log(f'      [ ][__findNextModules__] is dict. One module left in chain!')
            if callback_flow['code'] == self.module_code:  #? it's this module
                callback_flow = []  #? nothing left
            else:  #? if it's not this module, something went wrong on the way
                return None

        else:  #? not of acceptable format
            return None

        log(f'      [+][__findNextModules__] almost done. callback_flow = {callback_flow}')

        """ Now, in the remaining callback_flow, what modules should I sent to """
        #? eg. new callback_flow = [[1,3],[2,[4,5]],[6,7,8],9]
        #? meaning, output of this module should be sent to:
        #?  1->3        [1,3]
        #?  2->(4,5)    [2,[4,5]]
        #?  6->7->8     [6,7,8]
        #?  9           [9]
        for mods in callback_flow:
            # if isinstance(mods, str): #? eg. 9
            if isinstance(mods, dict):  #? actually, dict instead
                next_mods[mods['code']] = [mods]
            elif isinstance(mods, list):  #? [1,3], [2,[4,5]], [6,7,8]
                # if isinstance(mods[0], str): #? the first module in the new callback_flow must be str. eg: 1 or 3. 2 or 4. 6
                if isinstance(mods[0], dict):  #? actually, dict instead
                    next_mods[mods[0]['code']] = mods
                else:  #? otherwise it's in the wrong format
                    return None

        return next_mods



    def __publishFiles__(self, module_output, nextmod_code, topic_name='', callback=None, args=None) -> None:
        """
        When data dir is not shared, this module might need to publish processed file so that the next module in chain can pull and keep processing
        but we need to publish only `filepath` result
        #?  for `datastring` or `boolean` or `dict`, no need. 
        #?  for `folderpath`, we need to zip the folder and send with `unzip` header
        """
        if len(topic_name) == 0:
            topic_name = f'task_{nextmod_code}'

        if self.module_output_format == 'filepath':
            #? send file by file
            for ohash, output in module_output.items():
                self.__send_file__(topic_name, output, unzip=False, callback=callback, args=args)

        elif self.module_output_format == 'folderpath': #? if the module needs to send the whole folder, then zip the folder and add header `unzip`=True in message's header
            #? send file by file
            for ohash, output in module_output.items():
                #? make sure it's a folder
                if not os.path.isdir(output):
                    continue
                #? zip the folder
                shutil.make_archive(output, 'zip', output)
                zippath = output+'.zip'
                self.__send_file__(topic_name, zippath, unzip=True, callback=callback, args=args)

        elif isinstance(self.module_output_format, list):
            for k in range(len(self.module_output_format)): #? loop through each output
                #? only publish files of the output whose format is `filepath` or `folderpath`
                if self.module_output_format[k] == 'filepath':
                    #? send file by file
                    for ohash, output in module_output.items():
                        self.__send_file__(topic_name, output[k], unzip=False, callback=callback, args=args)

                elif self.module_output_format[k] == 'folderpath':
                    #? send file by file
                    for ohash, output in module_output.items():
                        #? make sure it's a folder
                        if not os.path.isdir(output[k]):
                            continue
                        #? zip the folder
                        shutil.make_archive(output[k], 'zip', output[k])
                        zippath = output[k]+'.zip'
                        self.__send_file__(topic_name, zippath, unzip=True, callback=callback, args=args)



    def __taskNextModulesInferMode__(self, module_resp) -> bool:
        """
        #! Only for `_mode` = 'infer' (whole batch mode is infer)
        At each request, a module will receive a `callback_flow`, so that the module will know what modules to send processed data to next.

        It's soooo complicated when dealing with batch, as when processing by batch, we check for overlap hash to reduce the number of files the module needs to analyse.

        Therefore, it's difficult to check `callback_flow`, as one hash might be of multiple files, which belong to different analysis requests that are issued for different `modules_flow` (therefore different `callback_flow`)

        We use `_map_ohash_modflow` to store the modules_flow that each hash is tasked from,
        and `_map_modflow_flow` to map a `modules_flow` to its `callback_flow` (array of modules in the flow)

        ---
        Params:
            - module_resp: output of this module, format defined in Silentworker
        Return:
            - boolean: task successfully
        """

        log(f'[ ][__taskNextModulesInferMode__] CALLED')

        #? for any reason, the module's output is in wrong format, return
        if 'result' not in module_resp or module_resp['result'] is None or not isinstance(module_resp['result'], dict) or len(module_resp['result']) == 0:
            return False


        for orig_hash in list(self._map_ohash_modflow.keys()):
            #? for any reason the module failed to output for this hash, skip processing this hash
            if orig_hash not in module_resp['result']:
                continue
            
            if orig_hash not in self._map_ohash_requestids:
                continue

            for modflow_code in self._map_ohash_modflow[orig_hash]:
                requestids = '*'.join(self._map_ohash_requestids[orig_hash])
                try:
                    # log(f'[ ][__taskNextModulesInferMode__] modflow_code : {modflow_code}')
                    
                    # callback_flow = self._map_modflow_flow[modflow_code]
                    # del self._map_modflow_flow[modflow_code]
                    callback_flow = self._map_modflow_flow[f'{orig_hash}_{modflow_code}_{requestids}']

                    log(f'[ ][__taskNextModulesInferMode__] Deleting key `{orig_hash}_{modflow_code}_{requestids}` from `self._map_modflow_flow`')
                    del self._map_modflow_flow[f'{orig_hash}_{modflow_code}_{requestids}']
                except:
                    log(f'[!][__taskNextModulesInferMode__] No key `{orig_hash}_{modflow_code}_{requestids}` found in `self._map_modflow_flow`', 'error')
                    continue

                #? of each modflow (or callback_flow), find next modules to task
                next_mods = self.__findNextModules__(callback_flow)
                log(f'   [ ] callback_flow = {callback_flow}')
                log(f'   [ ] next_mods = {next_mods}')

                if next_mods is None:
                    log(f'[!][__taskNextModulesInferMode__] Errors when finding next modules', 'error')
                    continue

                log(f'[ ][__taskNextModulesInferMode__] next_mods : {next_mods}')


                for mod_code, cflow in next_mods.items():
                    if orig_hash not in module_resp['result']:
                        log(f'[!][__taskNextModulesInferMode__] {orig_hash} not found in modules_resp', 'error')
                        continue

                    mod = cflow[0]
                    #? construct message
                    msg = {
                        'analysis_request_ids': self._map_ohash_requestids[orig_hash],
                        'orig_path': self._map_ohash_oinputs[orig_hash],
                        'path': module_resp['result'][orig_hash],
                        'orig_hash': orig_hash,
                        'modules_flow_code': modflow_code,
                        'callback_flow': cflow,
                        'mode': self._mode, #? mode of the whole batch
                        'config_data': cflow[0]['config'],
                        'timestamp': time.time(),

                        'output_format': self.module_output_format #? send my output's format so that the next module will know what to do with received data
                    }
                    log(f'[ ][__taskNextModulesInferMode__] Tasking to `'+mod['code']+'` : '+json.dumps(msg))

                    #? send the files to next modules, only if this particular next module in chain requires me to send files
                    if 'require_files' in mod and mod['require_files'] is True:
                        self.__publishFiles__(module_resp['result'], mod['code'])
                    #? task the next module by producing to topic `task_<next_module_code>`
                    self.__produce_msg__(mod['code'], msg)

            #? del the hash from vars
            del self._map_ohash_modflow[orig_hash]

        return True


    def __taskNextModulesTrainMode__(self, module_resp, filepath_module_outputs_train, filepath_module_outputs_val) -> bool:
        """
        #! Only for `_mode` = 'train' (whole batch mode is train) and `_module_mode` = 'infer' (as a module that runs `train` cannot be followed by another module)
        Similar to `__taskNextModulesInferMode__`. The only difference is that for train mode, before tasking to the next module, it needs to collect all output of this module and put it in a `_file_input_...`
        
        NOTE: For train mode, processed files will still be sent to the next module. However, large train set also means many many files need to be transferred. Keep in mind that.

        ---
        Params:
            - module_resp: output of this module, format defined in Silentworker
            - filepath_module_outputs_train: filepath that stores outputs of the module for train inputs
            - filepath_module_outputs_val: filepath that stores outputs of the module for val inputs
        Return:
            - boolean: task successfully
        """

        log(f'[ ][__taskNextModulesTrainMode__] CALLED')

        """ train requests required all data written in one file. 
            for prior modules that run infer, we must write outputs of this module to a file first, before tasking the next module in train """
        mapkey = self._analysis_request_trainid
        
        for modflow_code in self._map_ohash_modflow[mapkey]:
            requestids = self._analysis_request_trainid
            try:
                callback_flow = self._map_modflow_flow[f'{mapkey}_{modflow_code}_{requestids}']
                log(f'[ ][__taskNextModulesTrainMode__] Deleting key `{mapkey}_{modflow_code}_{requestids}` from `self._map_modflow_flow`')
                del self._map_modflow_flow[f'{mapkey}_{modflow_code}_{requestids}']
            except:
                log(f'[!][__taskNextModulesTrainMode__] No key `{mapkey}_{modflow_code}_{requestids}` found in `self._map_modflow_flow`', 'error')
                continue

            #? of each modflow (or callback_flow), find next modules to task
            next_mods = self.__findNextModules__(callback_flow)

            if next_mods is None:
                log(f'[!][__taskNextModulesTrainMode__] Errors when finding next modules', 'error')
                continue

            log(f'[ ][__taskNextModulesTrainMode__] next_mods : {next_mods}')

            for mod_code, cflow in next_mods.items():
                mod = cflow[0]
                #? construct message
                msg = {
                    'analysis_request_ids': self._analysis_request_trainid,
                    'modules_flow_code': modflow_code,
                    'callback_flow': cflow,
                    'config_data': mod['config'],

                    'mode': self._mode, #? mode of the whole batch

                    'analysis_request_trainid': self._analysis_request_trainid, #? trainid for train requests
                    'filepath_orig_inputs_train': self._filepath_inputs_train,
                    'filepath_inputs_train': filepath_module_outputs_train,
                    'filepath_orig_inputs_val': self._filepath_inputs_val,
                    'filepath_inputs_val': filepath_module_outputs_val,
                    'train_ratio': self._train_ratio,
                    'modules_modes': self._modules_modes, #? dict of each module's mode
                    'save_infer_result': self._save_infer_result,

                    'timestamp': time.time(),

                    'output_format': self.module_output_format #? send my output's format so that the next module will know what to do with received data
                }
                log(f'[ ][__taskNextModulesTrainMode__] Tasking to `'+mod['code']+'` : '+json.dumps(msg))

                #? send the files to next modules, only if this particular next module in chain requires me to send files
                if 'require_files' in mod and mod['require_files'] is True:
                    self.__publishFiles__(module_resp['result'], mod['code']) #? this might take time dear
                #? task the next module by producing to topic `task_<next_module_code>`
                self.__produce_msg__(mod['code'], msg)

        #? del the hash from vars
        del self._map_ohash_modflow[mapkey]

        return True



    def __sendInferResults__(self, module_resp) -> bool:
        """
        #! Only for `_module_mode` = 'infer'
        Send analysis results to a topic, collector will collect these results and insert to db
        These modules can be implemented by 3rd parties, so no db authentication info should be provided
        NOTE: For infer output, the results collector will not care about any files this module generated, so no need to publish files to the collector
        """

        log(f'[ ][__sendInferResults__] CALLED')

        #? for any reason, the module's output is in wrong format, return
        if 'result' not in module_resp or module_resp['result'] is None or not isinstance(module_resp['result'], dict) or len(module_resp['result']) == 0:
            return False

        # results = [] #? store all analysis results (all outputs)
        # warnings = [] #? store only suspicious results (detector's outputs = 1)
        # detected_objs = [] #? store detected objects' identifiers (hash, url, etc.)
        for orig_hash, orig_input in self._map_ohash_oinputs.items():
            if orig_hash not in self._map_ohash_requestids:
                continue

            #? construct message
            for analysis_request_id in self._map_ohash_requestids[orig_hash]:
                if orig_hash not in module_resp['result']:
                    log(f'[!][__sendInferResults__] {orig_hash} not found in modules_resp', 'error')

                # self.__publishFiles__(module_resp['result'], 'analysis_results') #? for `analysis_results` (received on Collector), no need to know the file

                tmpar = analysis_request_id.split('--[')
                domain_id = tmpar[1].split(']')[0]
                capturer_id = tmpar[2].split(']')[0]

                #* always push back analysis result
                msg = {
                    'analysis_request_id': analysis_request_id,
                    'analysis_request_trainid': self._analysis_request_trainid,
                    'mode': 'infer',
                    'domain_id': domain_id,
                    'capturer_id': capturer_id,
                    'module': self.module_code,
                    'module_type': self.module_type,
                    'otype': self.module_otype,
                    'path': orig_input,
                    'hash': orig_hash,
                    'output': module_resp['result'][orig_hash] if orig_hash in module_resp['result'] else 'ERROR',
                    'note': module_resp['note'][orig_hash] if 'note' in module_resp and module_resp['note'] is not None and isinstance(module_resp['note'], dict) and orig_hash in module_resp['note'] else '',
                    'time_inserted': time.time(),

                    'output_format': self.module_output_format #? send my output's format so that the next module will know what to do with received data
                }
                #? check if the detector's result is 1. if so, it's suspicious
                if self.module_type == 'detector' and orig_hash in module_resp['result'] and module_resp['result'][orig_hash] == 1: #? suspicious object
                    msg['detected_objs'] = [orig_hash]

                #? send to analysis_results topic
                log(f'[ ][__sendInferResults__] Sending 1 result to topic `{domain_id}--analysis_results`')
                msg2send = {
                    'type': 'result',
                    'data': msg
                }
                self.__produce_msg__('', msg2send, f'{domain_id}--analysis_results')

            #? del the hash from vars
            del self._map_ohash_requestids[orig_hash]
            # del self._map_ohash_oinputs[orig_hash]
            # del self._map_ohash_inputs[orig_hash]

        #? if this module is configured to publish files to data centre (so that Data Centres can sink this module's output)
        if self._publish_file:
            print('module_resp[result]', module_resp['result'])
            self.__publishFiles__(module_resp['result'], '', self._topic2pool)

        return True


    def __sendTrainResults__(self, module_resp) -> bool:
        """
        #! Only for `_module_mode` = 'train' 
        Send analysis results to a topic, collector will collect these results and insert to db
        These modules can be implemented by 3rd parties, so no db authentication info should be provided
        NOTE: For train output, the module will output a model, so the module will need to submit the model file to the collector, the collector will save to its datacentre for the users to download if they issue
        """

        log(f'[ ][__sendTrainResults__] CALLED')

        #? for any reason, the module's output is in wrong format, return
        if 'result' not in module_resp or module_resp['result'] is None or not isinstance(module_resp['result'], str) or len(module_resp['result']) == 0:
            return False

        tmpar = self._analysis_request_trainid.split('--[')
        domain_id = tmpar[1].split(']')[0]
        capturer_id = tmpar[2].split(']')[0]

        #? construct message
        msg = {
            'analysis_request_id': self._analysis_request_trainid,
            'analysis_request_trainid': self._analysis_request_trainid,
            'mode': 'train',
            'domain_id': domain_id,
            'capturer_id': capturer_id,
            'module': self.module_code,
            'module_type': self.module_type,
            'otype': self.module_otype,
            'output': module_resp['result'],
            'note': module_resp['note'] if 'note' in module_resp and module_resp['note'] is not None else '',
            'time_inserted': time.time()
        }

        #? pool the model ? (so that Data Centres can sink this model) (maybe no need)
        # self.__send_file__(self._topic2pool, module_resp['result'], False)
        #? send to analysis_results topic
        msg2send = {
            'type': 'result',
            'data': msg
        }
        log(f'[#][__sendTrainResults__] Sending train results for analysis_request_id {self._analysis_request_trainid} to topic `analysis_results` (commented)')
        # self.__produce_msg__('',  msg2send, f'{domain_id}--analysis_results')

        return True



    def __send_file__(self, topic_name, filepath, unzip=False, callback=None, args=None) -> None:
        if not os.path.isfile(filepath):
            log(f'[!][Producer][produce_file] {filepath} not exist')
            return

        print(f'[ ][__send_file__] Sending {filepath} to {topic_name}')

        headers = [('domain_id', ''.encode('utf-8'))]
        headers.append(('capturer_id', ''.encode('utf-8'))) #? not available for python code
        headers.append(('filepath', filepath.encode('utf-8')))
        headers.append(('isend', '0'.encode('utf-8')))
        headers.append(('sinkfile', '1'.encode('utf-8')))

        with open(filepath, 'rb') as fin:
            while True:
                data = fin.read(self._chunk_size)
                if not data:
                    break

                b64data = base64.b64encode(data)
                self.producer.send(topic_name, value=b64data, headers=headers)
        # self.producer.flush()

        if self._after_publish == 'delete':
            os.remove(filepath)

        #? finish sending this file, now if we need the receiving end to unzip this file, send a message with `isend`=True and `unzip`=True so that the puller know when the file is end and to unzip it. You can include a process signal if you want
        if unzip is True:
            # headers.append(('isend', 'True'.encode('utf-8')))
            # headers[3] = ('isend', '1'.encode('utf-8'))
            #? tuple cannot be changed once defined. convert to list to update then convert back to tuple
            headers_list = list(headers)
            headers_list[3] = ('isend', '1'.encode('utf-8'))
            headers = tuple(headers_list)
            #? then append a field to signal the receiver end to unzip
            headers.append(('unzip', 'True'.encode('utf-8')))
            self.producer.send(topic_name, value=''.encode('utf-8'), headers=headers)
            self.producer.flush()

        if callback is not None:
            if args is not None:
                callback(*args)
            else:
                callback()

        return


    def __produce_msg__(self, module_code, msg, topic_name='') -> None:
        #? finish sending this file, now send a message with `isend`=True so that the puller know when the file is end. You can include a process signal if you want
        if len(topic_name) == 0:
            topic_name = f'task_{module_code}'

        #? domain_id in header is to identified data is issued from which domain
        #? here we process data of all domains in one (master) topic
        #? after finishing we submit back to domains via different topics accordingly
        headers = [('domain_id', ''.encode('utf-8'))]
        headers.append(('capturer_id', ''.encode('utf-8'))) #? not available for python code
        headers.append(('filepath', ''.encode('utf-8')))
        headers.append(('isend', '1'.encode('utf-8')))
        headers.append(('sinkfile', '0'.encode('utf-8')))

        log(f'[ ][__produce_msg__] Producing {msg} to {topic_name}')
        self.producer.send(topic_name, value=json.dumps(msg).encode('utf-8'), headers=headers)
        self.producer.flush()
        return
