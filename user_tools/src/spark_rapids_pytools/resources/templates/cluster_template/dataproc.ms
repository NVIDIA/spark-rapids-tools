{
  "clusterName": "default-cluster-name",
  "clusterUuid": "1234-5678-1234567",
  "config": {
    "gceClusterConfig": {
      "zoneUri": {{{ ZONE }}}
    },
    "masterConfig": {
      "instanceNames": [
        "test-node-d"
      ],
      "machineTypeUri": {{{ DRIVER_NODE_TYPE }}},
      "numInstances": {{ NUM_DRIVER_NODES }}
    },
    "workerConfig": {
{{# GPU_NAME }}
      "accelerators": [
        {
          "acceleratorCount": {{ NUM_GPUS }},
          "acceleratorTypeUri": {{{ GPU_NAME }}}
        }
      ],
{{/ GPU_NAME }}
      "machineTypeUri": {{{ WORKER_NODE_TYPE }}},
      "numInstances": {{ NUM_WORKER_NODES }}
    },
    "softwareConfig": {
        "imageVersion": {{{ IMAGE }}}
    }
  },
  "status": {
    "state": "STOPPED"
  }
}
