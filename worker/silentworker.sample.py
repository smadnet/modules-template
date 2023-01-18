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

from worker.base.silentworker_base import SilentWorkerBase
from utils.utils import log
import os


class SilentWorker(SilentWorkerBase):
    """
    This is the baseline for the SilentWorker.

    SilentWorker should be the one carrying the main work.
    Sometimes a module might take a great deal of time to generate outputs. 

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

    def __init__(self, config) -> None:
        """ Dont change/remove this super().__init__ line.
            This line passes config to initialize services. 
        """
        super().__init__(config)

        #! Add your parts of initializing the model or anything you want here. 
        #! You might want to load everything at this init phase, not reconstructing everything at each request (which shall then be defined in run())
        print('Nooby doo')

    
    def onChangeConfig(self, config_data):
        """
        Callback function when module's config is changed.
        (usually at each request to analyze, when config_data is sent along as a parameter)
        ---
        This is the module's config which is passed at each analysis request. (usually the config to initialize the model)
        """

        log(f'[ ][SilentWorker][onChangeConfig] config_data is passed: {config_data}')

        #! Want to do something with the model when the config is changed ? Maybe reconfig the model's params, batch size etc. ?
        #? eg. change global module's config
        #self._config = config_data
        
        return


    def readInputsFilepath(self) -> tuple[list, list, list, list]:
        """
        #? This function is to be overwritten.
        For train requests, inputs are put in one file, use this function to retrieve all inputs line by line, just like receiving infer requests one input per message.
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

        return orig_inputs_train, inputs_train, orig_inputs_val, inputs_val


    def infer(self, config_data):
        """
        #? This function is to be overwritten.
        Main `inference` function. 

            #? (used for all modules, `detector`, `(pre)processor`, `sandbox`)
            Whatever you need to do in silence, put it here.
            We provide inference in batch, for heavy models.

        ----
        Use these vars to access input data:
            - self._map_ohash_inputs: dict of inputs to feed this module (eg. filepath to the executable files already stored on the system / url).
                map `orig_input_hash` => `prev module's output for orig_path correspond with orig_input_hash`
            - self._map_ohash_oinputs: dict of original inputs that is fed to this module flow.
                map `orig_input_hash` => one `orig_path`.
                (multiple orig_path might have the same hash, but just map to one path)
        
        Params:
            - config_data: modules configuration stored in the db.
        """

        try:
            #! Do something
            log('[ ][SilentWorker][infer] I\'m pretty')


            #! After finish, clean up and return data in appropriate format
            result = {} #? required.  {}
            note = {} #? optional.  {} | ''
            
            #! Call __onFinishInfer__ when the analysis is done. This can be called from anywhere in your code. In case you need synchronous processing
            self.__onFinishInfer__(result, note)

        except:
            log('[x][SilentWorker][infer] Ouch', 'error')
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
            log('[ ][SilentWorker][train] I\'m pretty')


            #! After finish, clean up and return data in appropriate format
            saved_model_path = '' #? required.  '' (path to saved model)
            note = '' #? optional.  {} | ''
            
            #! Call __onFinishTrain__ when the analysis is done. This can be called from anywhere in your code. In case you need synchronous processing
            self.__onFinishTrain__(saved_model_path, note)

        except:
            log('[x][SilentWorker][train] Ouch', 'error')
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
            print('[ ][SilentWorker][processStream] I\'m pretty')

        except:
            log('[x][SilentWorker][processStream] Ouch', 'error')
            return
