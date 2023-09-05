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

"""
Define some abstract cluster representation and some of the dataclasses that can provide
information about the hardware configurations
"""

import abc
from collections import defaultdict
from typing import Type, Union, Any, TypeVar, Dict, Callable, ClassVar, Optional

from ..exceptions import InvalidPropertiesSchema
from ..storagelib import CspPathT
from ..utils import AbstractPropContainer, PropValidatorSchema

CT = TypeVar('CT')
ClientClusterT = TypeVar('ClientClusterT', bound='ClientCluster')


class ClusterProxy:
    """
    Class holding metadata of the client implementation that interfaces with Cluster
    """
    name: str
    dependencies_loaded: bool = True
    _client_clzz: Type['ClientCluster']
    _prop_mgr_clzz: Type['ClusterPropMgr']

    @property
    def client_clzz(self) -> Type['ClientCluster']:
        return self._client_clzz

    @client_clzz.setter
    def client_clzz(self, clzz):
        self._client_clzz = clzz

    @property
    def prop_mgr_clzz(self) -> Type['ClusterPropMgr']:
        return self._prop_mgr_clzz

    @prop_mgr_clzz.setter
    def prop_mgr_clzz(self, clzz):
        self._prop_mgr_clzz = clzz


cluster_registry: Dict[str, ClusterProxy] = defaultdict(ClusterProxy)


def register_client_cluster(key: str) -> Callable[[Type[ClientClusterT]], Type[ClientClusterT]]:
    def decorator(cls: Type[ClientClusterT]) -> Type[ClientClusterT]:
        if not issubclass(cls, ClientCluster):
            raise TypeError('Only subclasses of ClusterSchema can be registered.')
        cluster_registry[key].client_clzz = cls
        cluster_registry[key].name = key
        cls._client_impl_meta = cluster_registry[key]  # pylint: disable=protected-access
        return cls
    return decorator


def register_cluster_prop_mgr(key: str) -> Callable:
    def decorator(cls: type) -> type:
        if not issubclass(cls, ClusterPropMgr):
            raise TypeError('Only subclasses of ClusterPropMgr can be registered.')
        cluster_registry[key].prop_mgr_clzz = cls
        return cls

    return decorator


class ClientClusterMeta(abc.ABCMeta):
    """
    Meta class representing client cluster
    """
    def __call__(cls: Type[CT], file_path: CspPathT, *args: Any, **kwargs: Any
                 ) -> Union[CT, ClientClusterT]:
        # cls is a class that is the instance of this metaclass, e.g., CloudPath
        if not issubclass(cls, ClientCluster):
            raise TypeError(
                f'Only subclasses of {ClientCluster.__name__} can be instantiated from its meta class.'
            )
        # Dispatch to subclass if base ClientCluster
        if cls is ClientCluster:
            for client_proxy in cluster_registry.values():
                client_clzz = client_proxy.client_clzz
                prop_mgr_clzz = client_proxy.prop_mgr_clzz
                if prop_mgr_clzz is not None and client_clzz is not None:
                    prop_mgr_obj = prop_mgr_clzz.load_from_file(file_path, False)
                    if prop_mgr_obj is not None:
                        client_obj = object.__new__(client_clzz)
                        client_clzz.__init__(client_obj, prop_mgr=prop_mgr_obj, *args, **kwargs)
                        return client_obj
            # no implementation matched the provided property file
            # raise an error
            raise InvalidPropertiesSchema(
                msg=f'Incorrect properties files: [{file_path}] '
                    'is incorrect or it does not match a valid Schema')
        prop_mgr_obj = cls._client_impl_meta.prop_mgr_clzz.load_from_file(file_path, True)
        client_obj = object.__new__(cls)
        cls.__init__(client_obj, prop_mgr=prop_mgr_obj, *args, **kwargs)
        return client_obj


class ClusterPropMgr(AbstractPropContainer):
    schema_clzz: ClassVar[Type['PropValidatorSchema']] = None


class ClientCluster(metaclass=ClientClusterMeta):   # pylint: disable=too-few-public-methods
    _client_impl_meta: ClusterProxy

    def __init__(self, prop_mgr: Optional['ClusterPropMgr']):
        self._prop_mgr = prop_mgr

    @property
    def platform_name(self) -> str:
        return self._client_impl_meta.name
