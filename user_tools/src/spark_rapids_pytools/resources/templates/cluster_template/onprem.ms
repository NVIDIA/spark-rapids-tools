{
  "config": {
    "workerConfig": {
{{# GPU_NAME }}
    "gpuInfo": {
      "count": {{ NUM_GPUS }},
      "name": {{{ GPU_NAME }}}
    },
{{/ GPU_NAME }}
      "numCores": {{ NUM_WORKER_CORES }},
      "numWorkers": {{ NUM_WORKER_NODES }}
    }
  }
}
