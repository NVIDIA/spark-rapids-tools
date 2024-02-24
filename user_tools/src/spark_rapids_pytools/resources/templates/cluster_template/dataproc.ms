{
  "clusterName": "default-cluster-prop",
  "clusterUuid": "1234-5678-1234567",
  "config": {
    "gceClusterConfig": {
      "zoneUri": "us-central1-a"
    },
    "masterConfig": {
      "instanceNames": [
        "test-node-d"
      ],
      "machineTypeUri": {{{ DRIVER_INSTANCE }}},
      "numInstances": 1
    },
    "workerConfig": {
      "instanceNames": {{{ EXECUTOR_NAMES }}},
      "machineTypeUri": {{{ EXECUTOR_INSTANCE }}},
      "numInstances": {{ NUM_EXECUTOR_NODES }}
    }
  },
  "status": {
    "state": "STOPPED"
  }
}