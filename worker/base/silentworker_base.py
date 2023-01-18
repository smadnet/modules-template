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

import os
from utils.utils import log
from worker.base.producer import Producer
#? for python < 3.9
from typing import Tuple as tuple


class SilentWorkerBase(Producer):
    """
    This is the baseline for the SilentWorker.

    SilentWorker should be the one carrying the main work.
    Since sometimes a module might take a great deal of time to generate outputs. 
    A request cannot be opened for too long. 
    So, it's best to just return something simple (like a boolean signifying if the request has arrived), then use another thread for the real work.

    Whenever your model finishes an operation, make sure to call `__onFinish__` function to populate your results to db as well as to the next module in the chain.
        Command:
            self.__onFinish__(output[, note])

        whereas:
            - output (mandatory): A dict that maps a `orig_input_hash` => output
                Output could be of any type. Just make sure to note that in your module's configuration and description for others to interpret your results.
                For example,
                    - a detector module can return a boolean (to represent detection result).
                    - a non-detector module (eg. processor or sandbox) can return the path storing processed result.
                eg.
                    {
                        'hash_of_input_1': true,
                        'hash_of_input_2': false,
                        'hash_of_input_3': false,
                        ...
                    }
            - note (optional).
                It could be a dict or a string.
                If it's a dict, it must be a map of a filepath to a note (string). The system will find and save the note accordingly with the file. 
                If it's not a map, use a string. The system will save this note for all the files analyzed in this batch
    """

    prv_config = {} #? store of full config sent in previous request. used to detect when passed config_data is changed
    
    #? input batch by hash
    #? dict that maps hash => input path
    batch_inputs_by_hash = {}

    #? dict that maps hash => original input path
    batch_oinputs_by_hash = {}
    

    def __init__(self, config, options=None) -> None:
        #? re check the input path for the `filepath_orig_inputs_train` etc. first
        if options is not None:
            if 'filepath_orig_inputs_train' in options:
                options['filepath_orig_inputs_train'] = self.__recheck_input_filepath_fcn__('filepath_orig_inputs_train', 'filepath')
            if 'filepath_orig_inputs_val' in options:
                options['filepath_orig_inputs_val'] = self.__recheck_input_filepath_fcn__('filepath_orig_inputs_val', 'filepath')
            if 'filepath_inputs_train' in options:
                options['filepath_inputs_train'] = self.__recheck_input_filepath_fcn__('filepath_inputs_train', 'filepath')
            if 'filepath_inputs_val' in options:
                options['filepath_inputs_val'] = self.__recheck_input_filepath_fcn__('filepath_inputs_val', 'filepath')
        #? now call to the parent config
        super().__init__(config, options)

        if 'num_per_batch' not in config:
            self.num_per_batch = 1
        else:
            self.num_per_batch = config['num_per_batch']


    def __check_config_change__(self, config_data, callback) -> None:
        """
        Callback function when module's config is changed.
        (usually at each request to analyze, when config_data is sent along as a parameter)
        ---
        This is the module's config which is passed at each analysis request. (usually the config to initialize the model)
        """
        if config_data is not None and len(config_data) > 0:
            self.prv_config = {**self._config}
            #! disable overwritting these params
            # for key in ['kafka_bootstrap_servers', 'module_code', 'module_otype', 'module_input_format', 'module_output_format', 'module_indir', 'module_outdir', 'publishing']:
            for key in ['kafka_bootstrap_servers', 'module_code', 'module_otype', 'module_input_format', 'module_output_format']:
                if key in config_data:
                    del config_data[key]
                if key in self.prv_config:
                    del self.prv_config[key]
            
            if config_data == self.prv_config: #? if same as current config, no need to process
                return
            
            #! can overwrite _config here, but maybe better if we let users decide to change the global _config or not
            # self._config = config_data #? completely overwrite
            # for key, val in config_data.items(): #? only overwrite updated keys
            #     self._config[key] = val
            #! should we allow overwrite these params ?
            # self.module_code = self._config['module_code']
            # self.module_otype = self._config['module_otype']
            #? module_outdir needs to change if it's changed in config
            if 'module_outdir' in config_data:
                self.__change_module_outdir__(config_data['module_outdir'])
            #? module_indir needs to change if it's changed in config
            if 'module_indir' in config_data:
                self.__change_module_outdir__(config_data['module_indir'])
            #? change publishing config ?
            self.__change_module_publishing__(config_data)
            # #? or if kafka servers changed
            # self.__reConnectKafka__(config_data)


            #? customable callback functions when config_data changed in db
            callback(config_data)

        return 


    def onChangeConfig(self, config_data) -> None:
        """
        #? This function is to be overwritten.
        Callback function when module's config is changed.
        (usually at each request to analyze, when config_data is sent along as a parameter)
        ---
        This is the module's config which is passed at each analysis request. (usually the config to initialize the model)
        """

        log(f'[ ][SilentWorkerBase][onChangeConfig] config_data is passed: {config_data}')

        #! Want to do something with the model when the config is changed ? Maybe reconfig the model's params, batch size etc. ?
        #? overwrite global config
        # self._config = config_data #? completely overwrite
        for key, val in config_data.items(): #? only overwrite updated keys
            self._config[key] = val
        
        return


    def __onFinishInfer__(self, module_output, note='') -> bool:
        """
        #! Only if `_module_mode` = 'infer'
        Callback function once a process work is done. This function:
            - Tasks the next module by producing a message to the topic `task_<next_module_code>`
            - Send result to analysis_results topic
        
        Params:
            - module_output: A dict that maps a `orig_input_hash` => output
            - note: 
        Return:
            - bool. True: successfully calls to after-process functions. False: something's wrong.
        """

        print('[ ] __onFinishInfer__', module_output, note)

        #? for any reason, the module's output is in wrong format, return
        if module_output is None or not isinstance(module_output, dict) or len(module_output) == 0:
            return False


        module_resp = {
            'result': module_output,
            'note': note
        }

        if self._mode == 'infer':
            """ for infer requests, simply task all these outputs to next modules for inference """
            stt_task = self.__taskNextModulesInferMode__(module_resp)
            #? send results of this module to `analysis_results` topic
            stt_send_result = self.__sendInferResults__(module_resp)
            return stt_task and stt_send_result

        elif self._mode == 'train':
            """ train requests required all data written in one file. 
                for prior modules that run infer, we must write outputs of this module to a file first, before tasking the next module """
            if os.path.isdir(self.module_outdir):
                filepath_module_outputs = {} #? store output paths for train and val sets
                #? write all module's output to a txt file
                #? since all inputs in filepath_inputs_train or filepath_inputs_val might be > num_per_batch for this module, this module might have to break these inputs to different batches. 
                #? we will add all outputs of all these batches to one file, using append mode
                #? check the outputs of this batch is for train set or val set first
                set_name = module_output.keys()[0].split('__')[0] #? set_name of the first input in the batch is the set_name of the whole batch
                filepath_module_outputs[set_name] = os.path.join(self.module_outdir, f'{self._analysis_request_trainid}_{set_name}.txt')
                open(filepath_module_outputs[set_name], 'a').write('\n'.join(module_output.values()))

                #? task next modules
                stt_task = self.__taskNextModulesTrainMode__(module_resp, filepath_module_outputs['train'], filepath_module_outputs['val'])

                stt_send_result = True
                if self._save_infer_result is True: #? by default, only infer requests are sent to `analysis_results` topic to keep stored in db. in train mode, if you want to keep output of the modules prior to your trainer, you will need to declare that in your training request
                    #? send results of this module to `analysis_results` topic
                    stt_send_result = self.__sendInferResults__(module_resp)

                return stt_task and stt_send_result

        #? is there clean up callback
        if self._cleanCallbackFcn is not None:
            self._cleanCallbackFcn()

        return False


    def __onFinishTrain__(self, module_output, note='') -> bool:
        """
        #! Only if `_module_mode` = 'train'
        Callback function once a process work is done. This function:
            - Send result to analysis_results_train topic
        
        Params:
            - module_output: Path to saved model
            - note: 
        Return:
            - bool. True: successfully calls to after-process functions. False: something's wrong.
        """

        print('[ ] __onFinishTrain__', module_output, note)

        #? for any reason, the module's output is in wrong format, return
        if module_output is None or not isinstance(module_output, str) or len(module_output) == 0:
            return False

        module_resp = {
            'result': module_output,
            'note': note
        }
        #? send results of this module to `analysis_results` topic
        self.__sendTrainResults__(module_resp)
        
        #? is there clean up callback
        if self._cleanCallbackFcn is not None:
            self._cleanCallbackFcn()
        
        return True
    

    def __recheck_input_filepath_fcn__(self, input_data, input_format):
        """
        When data dir is not shared, path sent in message might be incorrect (path points to location on the previous module)
        This module's sinker has sinked the file to `module_indir`, we need to change the path to point to the folder on this module
        It bases on the `module_input_format` (this module's config), and the `output_format` of the message
        """
        # path = os.path.join(self.module_indir, os.path.basename(path))
        if input_format in ['filepath', 'folderpath']: #? input_data is filepath or folderpath
            if not os.path.exists(input_data): #? only refine if path not found
                log(f'[!] {input_data} not found. Refining path...', 'warning')
                input_data = os.path.join(self.module_indir, os.path.basename(input_data))
                if not os.path.exists(input_data):
                    log(f'[x] {input_data} not exist', 'warning')
                return input_data

        elif isinstance(input_format, list):
            for k in range(len(input_format)): #? loop through each output
                #? only publish files of the output whose format is `filepath`
                if input_format[k] in ['filepath', 'folderpath']:
                    if not os.path.exists(input_data): #? only refine if path not found
                        log(f'[!] {input_data[k]} not found. Refining path...', 'warning')
                        input_data[k] = os.path.join(self.module_indir, os.path.basename(input_data[k]))
                        if not os.path.exists(input_data[k]):
                            log(f'[x] {input_data[k]} not exist', 'warning')
            return input_data
        
        return input_data


    def __recheck_input_filepath__(self, paths, msg) -> list:
        """
        When data dir is not shared, path sent in message might be incorrect (path points to location on the previous module)
        This module's sinker has sinked the file to `module_indir`, we need to change the path to point to the folder on this module
        It bases on the `module_input_format` (this module's config), and the `output_format` of the message
        """
        if 'output_format' in msg:
            if msg['output_format'] in ['filepath', 'folderpath'] or (isinstance(msg['output_format'], list) and ('filepath' in msg['output_format'] or 'folderpath' in msg['output_format'])):
                # log(f'[ ][__recheck_input_filepath__] Updating path, as prior module output_format is: '+ msg['output_format'])
                for i in range(len(paths)):
                    paths[i] = self.__recheck_input_filepath_fcn__(paths[i], msg['output_format'])

        elif self.module_input_format in ['filepath', 'folderpath'] or (isinstance(self.module_input_format, list) and ('filepath' in self.module_input_format or 'folderpath' in self.module_input_format)):
            # log(f'[ ][__recheck_input_filepath__] Updating path, as this module input_format is: {self.module_input_format}')
            for i in range(len(paths)):
                paths[i] = self.__recheck_input_filepath_fcn__(paths[i], self.module_input_format)

        return paths


    def __update_vars__(self, mapkey, analysis_request_ids, modflow_code, callback_flow) -> None:
        """
        Modify some vars for Producer
        #! Only for `_module_mode` = 'infer' (`_mode` can be either 'train' or 'infer')
        For `_module_mode` = 'train' (`_mode` = 'train'), it does not care about next modules (it should be the last module in the chain)
        """
        requestids = '*'.join(analysis_request_ids)
        # self._map_modflow_flow[modflow_code] = msg['callback_flow']
        self._map_modflow_flow[f'{mapkey}_{modflow_code}_{requestids}'] = callback_flow
        log(f'[ ][__update_vars__] `{mapkey}_{modflow_code}_{requestids}` => {callback_flow}')

        if mapkey not in self._map_ohash_requestids:
            self._map_ohash_requestids[mapkey] = analysis_request_ids
        else:
            self._map_ohash_requestids[mapkey].extend(analysis_request_ids)
            
        if mapkey not in self._map_ohash_modflow:
            self._map_ohash_modflow[mapkey] = [modflow_code]
        else:
            self._map_ohash_modflow[mapkey].append(modflow_code)



    def __onReceiveMsg__(self, msg, changeConfigCallbackFcn, processCallbackFcn, readInputFcn=None, cleanCallbackFcn=None) -> bool:
        """
        Callback function on receiving a message (data)
        Check if inputs enough a batch of `num_per_batch` to run
        """

        if cleanCallbackFcn is not None and self._cleanCallbackFcn is None:
            self._cleanCallbackFcn = cleanCallbackFcn
                

        modflow_code = msg['modules_flow_code']

        config_data = msg['config_data'] or self._config
        num_per_batch = self.num_per_batch
        # changed_num_per_batch = False


        """ Check if config change """
        if changeConfigCallbackFcn is not None:
            self.__check_config_change__(config_data, changeConfigCallbackFcn)


        """ check if num_per_batch is changed """
        if 'num_per_batch' in config_data:
            cf_num_per_batch = int(config_data['num_per_batch']) 
            if num_per_batch != cf_num_per_batch and cf_num_per_batch > 0:
                num_per_batch = int(config_data['num_per_batch'])
                # changed_num_per_batch = True


        if self._processing_mode == 'stream':
            """ stream processing """
            log('[ ][__onReceiveMsg__] `stream` processing mode')

            #? modify some vars for Producer
            orig_path = msg['orig_path']
            path = msg['path']
            orig_hash = msg['orig_hash']
            analysis_request_ids = msg['analysis_request_ids']
            self.__update_vars__(orig_hash, analysis_request_ids, modflow_code, msg['callback_flow'])

            #? double check input data
            path = self.__recheck_input_filepath__([path], msg)[0]

            #? process input
            return self.__addInputToBatch__(orig_hash, orig_path, path, processCallbackFcn, config_data, num_per_batch)

        else:
            """ batch processing """
            log('[ ][__onReceiveMsg__] `batch` processing mode')

            if self._mode == 'infer': #? `_mode` = 'infer' => `_module_mode` will always = 'infer'
                #? modify some vars for Producer
                orig_path = msg['orig_path']
                path = msg['path']
                orig_hash = msg['orig_hash']
                analysis_request_ids = msg['analysis_request_ids']
                self.__update_vars__(orig_hash, analysis_request_ids, modflow_code, msg['callback_flow'])
                
                #? double check input data
                path = self.__recheck_input_filepath__([path], msg)[0]
                log(f'[+][__onReceiveMsg__] Rechecked path: {path}')

                #? process input
                return self.__addInputToBatch__(orig_hash, orig_path, path, processCallbackFcn, config_data, num_per_batch)

            elif self._mode == 'train':
                #? modify some vars for Producer
                if self._module_mode == 'infer': #? only needed if this module is in `infer` mode, as for _module_mode = train, it does not care about next modules (it should be the last module in the chain)
                    self.__update_vars__(self._analysis_request_trainid, [self._analysis_request_trainid], modflow_code, msg['callback_flow'])
                
                if readInputFcn is not None:
                    #? process input
                    return self.__processInputsFilepathsTrainRequest__(readInputFcn, msg) #? inside this function, after processing input: for train mode, it will call to `train()` function. for infer mode, it will call to __addInputsToBatchInfer__ to run `infer()`
                else:
                    log('[x][__onReceiveMsg__] readInputFcn cannot be None')
        
        return False



    def __addInputToBatch__(self, orig_hash, orig_path, path, processCallbackFcn, config_data, num_per_batch) -> bool:
        """
        Add input to batch
        """
        log(f'[ ][__addInputToBatch__]  {orig_hash}  {orig_path}  {path}')
        log(f'   [__addInputToBatch__] len(self.batch_inputs_by_hash) = {len(self.batch_inputs_by_hash)}  |  num_per_batch = {num_per_batch}')

        """ Just append the file to the batch first """
        self.batch_inputs_by_hash[orig_hash] = path
        self.batch_oinputs_by_hash[orig_hash] = orig_path

        """ 
        Check the batch so far
        """
        if len(self.batch_inputs_by_hash) == num_per_batch:
            """ too easy case, n batch == num_per_batch """

            log(f'[+][__addInputToBatch__] len(self.batch_inputs_by_hash) == num_per_batch = {len(self.batch_inputs_by_hash)}  |  Calling processCallbackFcn')

            #? call the process function
            self._map_ohash_inputs = {**self.batch_inputs_by_hash}
            self._map_ohash_oinputs = {**self.batch_oinputs_by_hash}
            processCallbackFcn(config_data)
            
            #? clean the batch
            self.batch_inputs_by_hash = {}
            self.batch_oinputs_by_hash = {}

            return True

        elif len(self.batch_inputs_by_hash) > num_per_batch:
            """ more than num_per_batch """

            #? create a tmp object to process
            self._map_ohash_inputs = {}
            self._map_ohash_oinputs = {}
            # tmp__analysis_request_ids = {}

            #? move data to tmp batch until len(tmp_batch) == num_per_batch
            for hashval in list(self.batch_inputs_by_hash.keys()):
                if len(self._map_ohash_inputs) < num_per_batch:
                    self._map_ohash_inputs[hashval] = self.batch_inputs_by_hash[hashval]
                    self._map_ohash_oinputs[hashval] = self.batch_oinputs_by_hash[hashval]

                    del self.batch_inputs_by_hash[hashval]
                    del self.batch_oinputs_by_hash[hashval]
                else:
                    break

            #? when enough batch, process
            #? call the process function
            processCallbackFcn(config_data)

            return True

        elif len(self.batch_inputs_by_hash) > 0:
            """ there's somthing, but not enough a batch to run. 
                wait till next call """
            return True


    def __addInputsToBatchInferTrainRequest__(self, set_name, inputs) -> bool:
        """
        Add multiple inputs to batch to run infer
        #! Only for train requests (`_mode` = 'train') and `_module_mode` = 'infer'
        Do this by simply walk through each input and pass to __addInputToBatch__
        """
        if self._module_mode == 'infer':
            #? make sure with defined `num_per_batch`, this module can output all the inputs
            if len(inputs) % self.num_per_batch != 0:
                log(f'[x][__addInputsToBatchInfer__] {set_name} inputs len must be a multiple of num_per_batch')
                return False
            
            k = 0
            for input in inputs:
                k += 1
                self.__addInputToBatch__(f'{set_name}__{k}', input, input, self.infer, self._config, self.num_per_batch) #? for train options, one train request sends only one message, so we can safely use global settings self._config and self.num_per_batch here
            
            if k == len(inputs): #? all batch has been added to batch
                return True
        return False



    def __processInputsFilepathsTrainRequest__(self, readInputFcn, msg) -> bool:
        """
        For train requests, inputs are put in one file, use this function to retrieve all inputs line by line, just like receiving infer requests one input per message.
        Think of input for train request as a collection of inputs for inference requests.
        #! Only for `_mode` = 'train'
        #? You can decide how to read input files.
        """
        self._orig_inputs_train, self._inputs_train, self._orig_inputs_val, self._inputs_val = readInputFcn(msg)

        if self._module_mode == 'infer': #? this module is to do infer
            #? process as if it's from inference requests
            infer_train_inputs = self.__addInputsToBatchInferTrainRequest__('train', self._orig_inputs_train, self._inputs_train)
            if infer_train_inputs:
                log(f'[+][__processInputsFilepaths__] all `train` inputs are added to batch')

            if len(self._orig_inputs_val) > 0:
                infer_val_inputs = self.__addInputsToBatchInferTrainRequest__('val', self._orig_inputs_val, self._inputs_val)
                if infer_val_inputs:
                    log(f'[+][__processInputsFilepaths__] all `val` inputs are added to batch')
        
        elif self._module_mode == 'train':
            #? if this module is tasked to train, call the train function directly.
            #? the user defines how to deal with all the inputs
            self.train(self._config) #? for train options, one train request sends only one message, so we can safely use global settings self._config here




    def readInputsFilepath(self, msg) -> tuple[list, list, list, list]:
        """
        #? This function is to be overwritten.
        For train requests, inputs are put in one file, use this function to retrieve all inputs line by line, just like receiving infer requests one input per message.
        => NOTE: I recommend you should form your inputs file in the way that each line in `_filepath_inputs...` should be of same format as defined `module_input_format`, it would be more consistent and simple to follow.

        #! Only needed when `_mode` = 'train'
        #? You can decide how to read input files.

        Use these vars to access input data:
            - self._filepath_orig_inputs_train: path to the file that contains all original inputs for train set
            - self._filepath_inputs_train: path to the file that contains all inputs for train set
            - self._filepath_orig_inputs_val: path to the file that contains all original inputs for val set (could be None)
            - self._filepath_inputs_val: path to the file that contains all inputs for val set (could be None)
        
        NOTE: A `filepath_inputs_...` could be:
              - could be a file that lists all the paths to the files that contain data (eg. a txt file that contains all paths to csv files, which contains all records that should be used for train)
              - could be a file that lists all the data for training (eg. a csv file that contains all records that should be used for train)
            It's up to you to decide what to pass as `filepath_inputs` and what to do with those params

        Return:
            4 lists of orig_inputs_train, inputs_train, orig_inputs_val, inputs_val
        """

        """ read train inputs """
        orig_inputs_train = []
        inputs_train = []
        if self._filepath_orig_inputs_train is not None and self._filepath_inputs_train is not None:
            if os.path.isfile(self._filepath_orig_inputs_train):
                orig_inputs_train = [l.strip() for l in open(self._filepath_orig_inputs_train, 'r').readlines()]
            if os.path.isfile(self._filepath_inputs_train):
                inputs_train = [l.strip() for l in open(self._filepath_inputs_train, 'r').readlines()]

        """ read val inputs """
        orig_inputs_val = []
        inputs_val = []
        if self._filepath_orig_inputs_val is not None and self._filepath_inputs_val is not None:
            if os.path.isfile(self._filepath_orig_inputs_val):
                orig_inputs_val = [l.strip() for l in open(self._filepath_orig_inputs_val, 'r').readlines()]
            if os.path.isfile(self._filepath_inputs_val):
                inputs_val = [l.strip() for l in open(self._filepath_inputs_val, 'r').readlines()]

        #! if you structure your input file where each line is of the same format as `module_input_format` and your input format is filepath, then you might need to recheck the input filepath, rename it to the file landed in this module's input_dir.
        inputs_train = self.__recheck_input_filepath__(inputs_train, msg)
        inputs_val = self.__recheck_input_filepath__(inputs_val, msg)

        return orig_inputs_train, inputs_train, orig_inputs_val, inputs_val



    def infer(self, config_data) -> None:
        """
        #? This function is to be overwritten.
        Main `inference` function. 

            #? (used for all modules, `detector`, `(pre)processor`, `sandbox`)
            Whatever you need to do in silence, put it here.
            We provide inference in batch, for heavy models.

        ----
        Use these vars to access input data:
            - self._map_ohash_inputs: dict of inputs to feed this module.
                (eg. filepath to the executable files already stored on the system,
                     url,
                     one traffic flow (one flow in a csv)).
                map `orig_input_hash` => `prev module's output for orig_path correspond with orig_input_hash`
            - self._map_ohash_oinputs: dict of original inputs that is fed to this module flow (similar _map_ohash_inputs).
                map `orig_input_hash` => one `orig_path`.
                (multiple orig_path might have the same hash, but just map to one path)
        
        Params:
            - config_data: modules configuration stored in the db.
        """

        try:
            #! Do something
            log('[ ][SilentWorkerBase][infer] I\'m pretty')


            #! After finish, clean up and return data in appropriate format
            result = {} #? required.  {}
            note = {} #? optional.  {} | ''
                
            #! Call __onFinishInfer__ when the analysis is done. This can be called from anywhere in your code. In case you need synchronous processing
            self.__onFinishInfer__(result, note)

        except:
            log('[x][SilentWorkerBase][infer] Ouch', 'error')
            return


    def train(self, config_data) -> None:
        """
        #? This function is to be overwritten.
        Main `train` function. 

            #? (used for `detector` modules that implement ML/DL algorithms)
            For machine learning / deep learning modules, we provide an option for receiving inputs and training.
            For training, you may want to make sure `num_per_batch` > 1. We will NOT check this option for you, make sure you know what you're doing.
            After training, your module's result should be path to your saved model. It is your option of naming your model file.

        ----
        Use these vars to access input data:
            - self._orig_inputs_train: array of original inputs for train set
            - self._inputs_train: array of inputs for train set
            - self._orig_inputs_val: array of original inputs for valication set (could be empty [])
            - self._inputs_val: array of inputs for val set (could be empty [])
        
        Params:
            - config_data: modules configuration stored in the db.
        """

        try:
            #! Do something
            log('[ ][SilentWorkerBase][train] I\'m pretty')


            #! After finish, clean up and return data in appropriate format
            saved_model_path = '' #? required.  '' (path to saved model)
            note = '' #? optional.  {} | ''
                
            #! Call __onFinishTrain__ when the analysis is done. This can be called from anywhere in your code. In case you need synchronous processing
            self.__onFinishTrain__(saved_model_path, note)

        except:
            log('[x][SilentWorkerBase][train] Ouch', 'error')
            return


    def processStream(self, config_data) -> None:
        """
        #? This function is to be overwritten.
        Process stream data. 

            #? (used for all modules, `detector`, `(pre)processor`, `sandbox`)
            You still have the option of processing in batch even in stream processing.
            The only difference is that you have more power on processing coming messages (erasing or retaining/storing, removing or merging, retraining or not, etc.)

        ----
        Use these vars to access incoming data (in batch):
            - self._map_ohash_inputs: dict of inputs to feed this module.
                (eg. filepath to the executable files already stored on the system,
                     url,
                     one traffic flow (one flow in a csv)).
                map `orig_input_hash` => `prev module's output for orig_path correspond with orig_input_hash`
            - self._map_ohash_oinputs: dict of original inputs that is fed to this module flow (similar _map_ohash_inputs).
                map `orig_input_hash` => one `orig_path`.
                (multiple orig_path might have the same hash, but just map to one path)
        
        Params:
            - config_data: modules configuration stored in the db.
        """

        try:
            print('[ ][SilentWorkerBase][processStream] I\'m pretty')

        except:
            log('[x][SilentWorkerBase][processStream] Ouch', 'error')
            return
