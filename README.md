# Template for a module
```
worker/
    base/
        producer.py
        subscriber.py
        silentworker.base.py
    silentworker.sample.py
utils/
    utils.py
run.py
worker_threads.py
config.json
```

## Build a stupid module:
Copy `worker/silentworker.sample.py` to `worker/silentworker.py`  
Run the module:
```
python3 run.py config.json
```

## What's in the code?
- [`./run.py`](run.py): main file. This script will sfire main worker and open declared threads in `worker_threads.py`
- [`worker_threads.py`](worker_threads.py): declare extra threads to run alongside with the main SilentWorker
- [`producer.py`](producer.py): produce analysis results to the server and the next modules in chain
- [`subscriber.py`](subscriber.py): listens to analysis requests from server or previous modules in chains
- [`silentworker.base.py`](silentworker.base.py): baseline for silentworker
- [`silentwoker.sample.py`](silentwoker.sample.py): a sample file of your `silentworker.py` file, overwrite functions in `silentworker.base.py` per your need.
- [`utils.py`](utils.py): utility functions.

## Some example modules:
https://github.com/shingmt?tab=repositories

## Updating...
