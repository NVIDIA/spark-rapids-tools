{
	"Cluster": {
	  "Id": "j-123456789",
	  "Name": "default-cluster-name",
	  "Status": {
	    "State": "TERMINATED"
	  },
	  "Ec2InstanceAttributes": {
	    "Ec2AvailabilityZone": {{{ ZONE }}}
	  },
	  "InstanceGroups": [
	    {
	      "Id": "ig-123456789012e",
	      "Name": "CORE",
	      "Market": "ON_DEMAND",
	      "InstanceGroupType": "CORE",
	      "InstanceType": {{{ WORKER_NODE_TYPE }}},
	      "RequestedInstanceCount": {{ NUM_WORKER_NODES }}
	    },
	    {
	      "Id": "ig-123456789012d",
	      "Name": "MASTER",
	      "Market": "ON_DEMAND",
	      "InstanceGroupType": "MASTER",
	      "InstanceType": {{{ DRIVER_NODE_TYPE }}},
	      "RequestedInstanceCount": {{ NUM_DRIVER_NODES }}
	    }
	  ],
      "ReleaseLabel": {{{ IMAGE }}}
	}
}
