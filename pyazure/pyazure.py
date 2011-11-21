#!/usr/bin/env python
# encoding: utf-8
"""
Python wrapper around Windows Azure storage and management APIs

Authors:
    Sriram Krishnan <sriramk@microsoft.com>
    Steve Marx <steve.marx@microsoft.com>
    Tihomir Petkov <tpetkov@gmail.com>
    Blair Bethwaite <blair.bethwaite@gmail.com>

License:
    GNU General Public Licence (GPL)
    
    This file is part of pyazure.
    
    pyazure is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    pyazure is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with pyazure. If not, see <http://www.gnu.org/licenses/>.
"""

import sys

from util import *
from blob import BlobStorage
from queue import QueueStorage
from table import TableStorage

class PyAzure(object):
    """Class exposing Windows Azure storage and, if initialised appropriately,
    service management operations."""

    def __init__(self, storage_account_name=DEVSTORE_ACCOUNT,
            storage_account_key=None, use_path_style_uris=False,
            management_cert_path=None, subscription_id=None):
        self.storage_account = storage_account_name
        if storage_account_name == DEVSTORE_ACCOUNT:
            blob_host = DEVSTORE_BLOB_HOST
            queue_host = DEVSTORE_QUEUE_HOST
            table_host = DEVSTORE_TABLE_HOST
            if not storage_account_key:
                storage_account_key = DEVSTORE_SECRET_KEY
        else:
            blob_host = CLOUD_BLOB_HOST
            queue_host = CLOUD_QUEUE_HOST
            table_host = CLOUD_TABLE_HOST
        
        if management_cert_path and subscription_id:
            self.wasm = WASM(management_cert_path, subscription_id)
        else:
            self.wasm = None

        if not storage_account_key:
            if not self.wasm:
                raise WAError('Windows Azure Service Management API '
                    + 'not available.')
            storage_account_key, _, _ = self.wasm.get_storage_account_keys(
                storage_account_name)

        self.blobs = BlobStorage(blob_host, storage_account_name,
            storage_account_key, use_path_style_uris)
        self.tables = TableStorage(table_host, storage_account_name,
            storage_account_key, use_path_style_uris)
        self.queues = QueueStorage(queue_host, storage_account_name,
            storage_account_key, use_path_style_uris)
        self.WAError = WAError
        self.data_connection_string = create_data_connection_string(
            storage_account_name, storage_account_key)

    def set_storage_account(self, storage_account_name, create=False,
            location_or_affinity_group='Anywhere US'):
        """Set the storage account used by storage API objects.

        Setting to DEVSTORE_ACCOUNT will switch to the local storage
        emulator.
        For anything except switching to dev storage this requires the API
        to have been initialised with an appropriate management Windows
        Azure Service Mangement certificate.
        If the create flag is True and the storage account does not exist
        it will be created in the specified location or affinity group.
        May produce WAError exceptions due to authentication issues or when
        the storage account limit is reached.
        """
        if self.storage_account == storage_account_name:
            return
        storage_account_key = None
        if storage_account_name == DEVSTORE_ACCOUNT:
            self.blobs = BlobStorage(DEVSTORE_BLOB_HOST, DEVSTORE_ACCOUNT,
                DEVSTORE_SECRET_KEY)
            self.tables = TableStorage(DEVSTORE_BLOB_HOST, DEVSTORE_ACCOUNT,
                DEVSTORE_SECRET_KEY)
            self.queues = QueueStorage(DEVSTORE_BLOB_HOST, DEVSTORE_ACCOUNT,
                DEVSTORE_SECRET_KEY)
            self.data_connection_string = create_data_connection_string(
                DEVSTORE_ACCOUNT, DEVSTORE_SECRET_KEY)
            self.storage_account = storage_account_name
            return

        if not self.wasm:
            raise WAError('Windows Azure Service Management API not available.')
        if storage_account_name not in self.wasm.list_storage_accounts():
            if create:
                request = self.wasm.create_storage_account(
                    storage_account_name,
                    'PyAzure storage: %s' % get_azure_time(),
                    location_or_affinity_group,
                    'Storage account created by PyAzure')
                self.wasm.wait_for_request(request)
            else:
                raise WAError('Unknown storage account')
        storage_account_key, _, _ = self.wasm.get_storage_account_keys(
            storage_account_name)
        self.blobs = BlobStorage(CLOUD_BLOB_HOST, storage_account_name,
            storage_account_key)
        self.tables = TableStorage(CLOUD_TABLE_HOST, storage_account_name,
            storage_account_key)
        self.queues = QueueStorage(CLOUD_QUEUE_HOST, storage_account_name,
            storage_account_key)
        self.storage_account = storage_account_name
        self.data_connection_string = create_data_connection_string(
            storage_account_name, storage_account_key)


class WASM(object):
    """Class exposing Windows Azure Service Management operations."""

    def __init__(self, management_cert_path, subscription_id):
        from hostedservices import HostedServices, ServiceConfiguration
        from storageaccounts import StorageAccounts
        from locations import Locations
        self.service_api = HostedServices(management_cert_path,
            subscription_id)
        self.ServiceConfiguration = ServiceConfiguration
        self.storage_api = StorageAccounts(management_cert_path,
            subscription_id)
        self.location_api = Locations(management_cert_path,
            subscription_id)
        self._sme = ServiceManagementEndpoint(management_cert_path,
            subscription_id)
        self.WASMError = WASMError
        self.get_operation_status = self._sme.get_operation_status
        self.request_done = self._sme.request_done
        self.wait_for_request = self._sme.wait_for_request

        for op in self.service_api.get_wasm_ops():
            setattr(self, op.__name__, op)
        for op in self.storage_api.get_wasm_ops():
            setattr(self, op.__name__, op)
        for op in self.location_api.get_wasm_ops():
            setattr(self, op.__name__, op)

def main():
    pass

if __name__ == '__main__':
    sys.exit(main())
