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
    service management operations.    
    """

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
    """Class exposing Windows Azure Service Management operations.
    
    Using WASM
    ----------
    >>> import pyazure
    >>> pa = pyazure.PyAzure(management_cert_path=MANAGEMENT_CERT, 
    ... subscription_id=SUBSCRIPTION_ID)
    >>> 'Anywhere Asia' in pa.wasm.list_locations()
    True
    >>> request_id = pa.wasm.create_storage_account('pyazuretest','doctest',
    ... 'anywhere us', 'Here is my description, not great is it?')
    >>> pa.wasm.wait_for_request(request_id)
    True
    >>> (pa.wasm.get_operation_status(request_id) == 
    ... {'HttpStatusCode': '200', 'Status': 'Succeeded'})
    True
    >>> request_id = pa.wasm.create_storage_account(
    ... 'pyazuretestwithaverylongname','doctest','anywhere us')
    Traceback (most recent call last):
        ...
    ValueError: ('pyazuretestwithaverylongname', 'name must be between 3 and 24 characters in length and use numbers and lower-case letters only.')
    >>> 'pyazuretest' in pa.wasm.list_storage_accounts()
    True
    >>> pa.wasm.create_service('pyazuretest','create service doctest',
    ... 'anywhere europe')
    True
    >>> 'pyazuretest' in pa.wasm.list_services()
    True
    >>> pa.wasm.create_service('pyazuretest','create service doctest',
    ... 'anywhere europe')
    Traceback (most recent call last):
        ...
    WASMError: (409, 'ConflictError', 'The specified DNS name is already taken.')
    >>> pa.wasm.create_service('pyazuretest','create service doctest' * 10,
    ... 'anywhere europe') # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    ValueError: ('create service doctest...', 'label exceeds 100 char limit')
    >>> pa.wasm.get_service_properties('pyazuretest') # doctest: +ELLIPSIS
    ...                                   # doctest: +NORMALIZE_WHITESPACE
    OrderedDict([('Url', 'http...'),
                 ('ServiceName', 'pyazuretest'),
                 ('HostedServiceProperties',
                     OrderedDict([('Description', ''),
                                  ('Location', 'Anywhere Europe'),
                                  ('Label', 'create service doctest')]))])
    >>> pa.wasm.delete_service('pyazuretest')
    True
    >>> pa.wasm.delete_storage_account('pyazuretest')
    True
    >>> pa.wasm.delete_storage_account('pyazuretest')
    Traceback (most recent call last):
        ...
    WASMError: (404, 'ResourceNotFound', 'The requested storage account was not found.')
    """

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

def usage():
    print (
"""
This module is an API, and as such is not designed to be run directly.
However, there are embedded doctests which can executed by running pyazure.py
and providing a Windows Azure subscription id and management certificate path
as arguments, e.g., pyazure.py -s <subscription_id> -c <management_cert>

Available docstrings for the top-level API follow:
Service Management:
%s
Storage:
%s
""" % (PyAzure.__doc__, WASM.__doc__))
    print usage.func_doc

if __name__ == '__main__':
    import doctest
    import getopt
    try:
        opts, _ = getopt.getopt(sys.argv[1:],
            'hs:c:v',
            ['help','subscription_id','management_cert','verbose'])
    except getopt.GetoptError, e:
        print str(e)
        sys.exit(2)
    sub_id = None
    cert = None
    loud = False
    for opt, arg in opts:
        if opt in ('-h','--help'):
            usage()
            sys.exit()
        elif opt in ('-s','--subscription_id'):
            sub_id = arg
        elif opt in ('-c','--management_cert'):
            cert = arg
        elif opt in ('-v','--verbose'):
            loud = True
    if sub_id is None or cert is None:
        usage()
        sys.exit(2)
    doctest.testmod(
        extraglobs={'SUBSCRIPTION_ID':sub_id, 'MANAGEMENT_CERT':cert},
        verbose = loud)
    sys.exit()
