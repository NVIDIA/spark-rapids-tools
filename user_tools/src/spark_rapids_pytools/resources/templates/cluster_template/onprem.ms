{
  "config": {
    "masterConfig": {
      "numCores": 2,
      "memory": "7680MiB"
    },
    "workerConfig": {
{{# GPU_NAME }}
    "gpuInfo": {
      "count": {{ NUM_GPUS }},
      "name": {{{ GPU_NAME }}}
    },
{{/ GPU_NAME }}
      "numCores": {{ NUM_WORKER_CORES }},
      "memory": "7680MiB",
      "numWorkers": {{ NUM_WORKER_NODES }}
    }
  }
}
