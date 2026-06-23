# Copyright (c) 2026, NVIDIA CORPORATION.
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

"""Tests for deprecated property container replacements."""

# pylint: disable=protected-access,too-few-public-methods,wrong-import-position

import logging
import warnings

import pandas as pd


DEPRECATED_PROP_CONTAINER_MESSAGE = 'Deprecated: use AbstractPropContainer instead'


with warnings.catch_warnings():
    warnings.filterwarnings('ignore', message=DEPRECATED_PROP_CONTAINER_MESSAGE, category=DeprecationWarning)
    from spark_rapids_pytools.cloud_api.dataproc import DataprocCluster, DataprocNode
    from spark_rapids_pytools.cloud_api.sp_types import SparkNodeType
    from spark_rapids_pytools.common.cluster_inference import ClusterInference
    from spark_rapids_tools import CspEnv
    from spark_rapids_tools.utils import AbstractPropContainer


class _InferencePlatform:
    """Minimal platform implementation for cluster inference tests."""

    def __init__(self):
        self.loaded_cluster_prop = None
        self.loaded_is_inferred = None

    @staticmethod
    def get_platform_name():
        return CspEnv.DATAPROC

    @staticmethod
    def generate_cluster_configuration(render_args):
        return {
            'cluster_id': 'inferred-cluster',
            'config': {
                'workerConfig': {
                    'numInstances': render_args['NUM_WORKER_NODES'],
                    'machineType': render_args['WORKER_NODE_TYPE'].strip('"')
                },
                'masterConfig': {
                    'machineType': render_args['DRIVER_NODE_TYPE'].strip('"')
                }
            }
        }

    def load_cluster_by_prop(self, cluster_prop, is_inferred=False):
        self.loaded_cluster_prop = cluster_prop
        self.loaded_is_inferred = is_inferred
        return cluster_prop


class _DataprocCli:
    """Minimal Dataproc CLI implementation for cluster initialization tests."""

    def __init__(self):
        self.env_vars = {'zone': 'us-central1-a'}
        self.logger = logging.getLogger(__name__)

    def get_region(self):
        return 'us-central1'

    def get_zone(self):
        return self.env_vars['zone']


class _DataprocPlatform:
    """Minimal Dataproc platform implementation for cluster initialization tests."""

    def __init__(self):
        self.cli = _DataprocCli()


def _deprecated_prop_container_warnings(caught_warnings):
    return [
        warning for warning in caught_warnings
        if issubclass(warning.category, DeprecationWarning)
        and DEPRECATED_PROP_CONTAINER_MESSAGE in str(warning.message)
    ]


def test_cluster_inference_uses_abstract_prop_container():
    platform = _InferencePlatform()
    cluster_info_df = pd.DataFrame([{
        'App ID': 'app-1',
        'Num Worker Nodes': 2,
        'Cores Per Executor': 4,
        'Num Executors Per Node': 1,
        'Driver Node Type': 'n1-standard-4',
        'Worker Node Type': 'n1-standard-8'
    }])

    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter('always', DeprecationWarning)
        inferred_cluster = ClusterInference(platform=platform).infer_cluster(cluster_info_df)

    assert inferred_cluster is platform.loaded_cluster_prop
    assert platform.loaded_is_inferred is True
    assert isinstance(platform.loaded_cluster_prop, AbstractPropContainer)
    assert not _deprecated_prop_container_warnings(caught_warnings)


def test_dataproc_init_nodes_uses_abstract_prop_container(monkeypatch):
    def skip_fetching_hw_info(self, cli):
        del self, cli

    monkeypatch.setattr(DataprocNode, 'fetch_and_set_hw_info', skip_fetching_hw_info)
    cluster = DataprocCluster(_DataprocPlatform(), is_inferred=True)
    cluster.zone = 'us-central1-a'
    cluster.props = AbstractPropContainer(props={
        'config': {
            'masterConfig': {
                'instanceNames': ['master-0'],
                'machineType': 'n1-standard-4'
            },
            'workerConfig': {
                'numInstances': 1,
                'instanceNames': ['worker-0'],
                'machineType': 'n1-standard-8'
            }
        }
    })

    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter('always', DeprecationWarning)
        cluster._init_nodes()

    master_node = cluster.nodes[SparkNodeType.MASTER]
    worker_node = cluster.nodes[SparkNodeType.WORKER][0]
    assert isinstance(master_node.props, AbstractPropContainer)
    assert isinstance(worker_node.props, AbstractPropContainer)
    assert master_node.instance_type == 'n1-standard-4'
    assert worker_node.instance_type == 'n1-standard-8'
    assert not _deprecated_prop_container_warnings(caught_warnings)
