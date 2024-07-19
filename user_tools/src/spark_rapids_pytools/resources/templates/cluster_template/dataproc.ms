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
      "machineTypeUri": {{{ DRIVER_INSTANCE }}},
      "numInstances": {{ NUM_DRIVER_NODES }}
    },
    "workerConfig": {
      "machineTypeUri": {{{ EXECUTOR_INSTANCE }}},
      "numInstances": {{ NUM_EXECUTOR_NODES }}
    },
    "softwareConfig": {
        "imageVersion": {{{ IMAGE }}}
    }
  },
  "status": {
    "state": "STOPPED"
  }
}