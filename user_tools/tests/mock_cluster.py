# Copyright (c) 2023, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Mock cluster configurations for unit testing."""

import json

mock_live_cluster = {
    "dataproc": [
        "us-central1",          # gcloud config get compute/region
        "us-central1-a",        # gcloud config get compute/zone
        "dataproc-project-id",  # gcloud config get core/project
        # gcloud dataproc clusters describe test-cluster --format json --region us-central1
        json.dumps({
            "clusterUuid": "11111111-1111-1111-1111-111111111111",
            "config": {
                "masterConfig": {
                    "instanceNames": [
                        "test-master",
                    ],
                    "machineTypeUri": "https://www.googleapis.com/compute/v1/projects/project-id/zones/us-central1-a/"\
                                      "machineTypes/n1-standard-2",
                },
                "workerConfig": {
                    "accelerators": [{
                        "acceleratorTypeUri": "https://www.googleapis.com/compute/beta/projects/project-id/zones/"\
                                              "us-central1-a/acceleratorTypes/nvidia-tesla-t4"
                    }],
                    "instanceNames": [
                        "test-worker-0",
                    ],
                    "machineTypeUri": "https://www.googleapis.com/compute/v1/projects/project-id/zones/us-central1-a/"\
                                      "machineTypes/n1-standard-8",
                },
            },
            "status": {
                "state": "RUNNING",
            },
        }),
        # gcloud compute machine-types describe n1-standard-8 --format json --zone us-central1-a
        json.dumps({
            "guestCpus": 8,
            "memoryMb": 30720,
        }),
        # gcloud compute accelerator-types describe nvidia-tesla-t4 --format json --zone us-central1-a
        json.dumps({
            "description": "NVIDIA T4",
        }),
        # gcloud compute machine-types describe n1-standard-2 --format json --zone us-central1-a
        json.dumps({
            "guestCpus": 2,
            "memoryMb": 7680,
        }),
    ],

    "emr": [
        # aws emr list-clusters --query 'Clusters[?Name==`test-cluster`]'
        json.dumps([{
            "Id": "j-testemr",
        }]),
        # aws emr describe-cluster --cluster-id j-testemr
        json.dumps({
            "Cluster": {
                "Id": "j-testcluster",
                "Status": {
                    "State": "RUNNING"
                },
                "Ec2InstanceAttributes": {
                    "Ec2AvailabilityZone": "us-west-2b",
                },
                "InstanceGroups": [
                    {
                        "Id": "ig-testinstance1",
                        "Market": "ON_DEMAND",
                        "InstanceGroupType": "MASTER",
                        "InstanceType": "m5a.12xlarge",
                        "RequestedInstanceCount": 1
                    },
                    {
                        "Id": "ig-testinstance2",
                        "Market": "ON_DEMAND",
                        "InstanceGroupType": "CORE",
                        "InstanceType": "g4dn.12xlarge",
                        "RequestedInstanceCount": 1
                    }
                ]
            }
        }),
        # aws emr list-instances --cluster-id j-testcluster --instance-group-id ig-testinstance1
        json.dumps({
            "Instances": [{
                "Id": "ci-testinstance1",
                "Ec2InstanceId": "i-testec2id1",
                "PublicDnsName": "ec2-123.456.789.us-west-2.compute.amazonaws.com",
                "Status": {
                    "State": "RUNNING",
                },
            }]
        }),
        # aws emr list-instances --cluster-id j-testcluster --instance-group-id ig-testinstance2
        json.dumps({
            "Instances": [{
                "Id": "ci-testinstance2",
                "Ec2InstanceId": "i-testec2id2",
                "PublicDnsName": "ec2-234.567.890.us-west-2.compute.amazonaws.com",
                "Status": {
                    "State": "RUNNING",
                },
            }]
        }),
        # aws ec2 describe-instance-types --region us-west-2 --instance-types m5a.12xlarge
        json.dumps({
            "InstanceTypes": [{
                "VCpuInfo": {
                    "DefaultVCpus": 48,
                },
                "MemoryInfo": {
                    "SizeInMiB": 196608,
                },
            }]
        }),
        # aws ec2 describe-instance-types --region us-west-2 --instance-types g4dn.12xlarge
        json.dumps({
            "InstanceTypes": [{
                "VCpuInfo": {
                    "DefaultVCpus": 48,
                },
                "MemoryInfo": {
                    "SizeInMiB": 196608,
                },
                "GpuInfo": {
                    "Gpus": [{
                        "Name": "T4",
                        "Manufacturer": "NVIDIA",
                        "Count": 4,
                        "MemoryInfo": {
                            "SizeInMiB": 16384,
                        },
                    }],
                    "TotalGpuMemoryInMiB": 65536,
                },
            }]
        }),
    ],

    "databricks-aws": [
        # databricks clusters get --profile DEFAULT --cluster-name test-cluster
        json.dumps({
            "cluster_id": "1234-567890-test-cluster",
            "driver": {
                "public_dns": "12.34.56.789",
                "node_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            },
            "executors": [
                {
                    "public_dns": "12.34.56.798",
                    "node_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                },
            ],
            "node_type_id": "g4dn.12xlarge",
            "driver_node_type_id": "m5a.12xlarge",
            "instance_source": {
                "node_type_id": "g4dn.12xlarge"
            },
            "driver_instance_source": {
                "node_type_id": "m5a.12xlarge"
            },
            "state": "RUNNING",
            "num_workers": 1
        }),
        # aws ec2 describe-instance-types --region us-west-2 --instance-types m5a.12xlarge
        json.dumps({
            "InstanceTypes": [{
                "VCpuInfo": {
                    "DefaultVCpus": 48,
                },
                "MemoryInfo": {
                    "SizeInMiB": 196608,
                },
            }]
        }),
        # aws ec2 describe-instance-types --region us-west-2 --instance-types g4dn.12xlarge
        json.dumps({
            "InstanceTypes": [{
                "VCpuInfo": {
                    "DefaultVCpus": 48,
                },
                "MemoryInfo": {
                    "SizeInMiB": 196608,
                },
                "GpuInfo": {
                    "Gpus": [{
                        "Name": "T4",
                        "Manufacturer": "NVIDIA",
                        "Count": 4,
                        "MemoryInfo": {
                            "SizeInMiB": 16384,
                        },
                    }],
                    "TotalGpuMemoryInMiB": 65536,
                },
            }]
        })
    ],

    "databricks-azure": [
        # databricks clusters get --profile AZURE --cluster-name test-cluster
        json.dumps({
            "cluster_id": "1234-567890-test-cluster",
            "driver": {
                "public_dns": "12.34.56.789",
                "node_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            },
            "executors": [{
                "public_dns": "12.34.56.798",
                "node_id": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            }
            ],
            "node_type_id": "Standard_NC4as_T4_v3",
            "driver_node_type_id": "Standard_NC4as_T4_v3",
            "instance_source": {
                "node_type_id": "Standard_NC4as_T4_v3"
            },
            "driver_instance_source": {
                "node_type_id": "Standard_NC4as_T4_v3"
            },
            "state": "RUNNING",
            "num_workers": 1,
        }),
        # az vm list-skus --location westus
        # This output is not required for the test because we are using a mock
        # that reads data from the test catalog file instead.
    ]
}
