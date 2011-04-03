#!/usr/bin/env python
# encoding: utf-8
"""
Python wrapper around Windows Azure storage and management APIs

Authors:
    Sriram Krishnan <sriramk@microsoft.com>
    Steve Marx <steve.marx@microsoft.com>
    Tihomir Petkov <tpetkov@gmail.com>

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
    """Class exposing Windows Azure storage operations"""
    def __init__(self, account_name=DEVSTORE_ACCOUNT,
                 account_key=DEVSTORE_SECRET_KEY, use_path_style_uris=False):
        if account_name == DEVSTORE_ACCOUNT:
            blob_host = DEVSTORE_BLOB_HOST
            queue_host = DEVSTORE_QUEUE_HOST
            table_host = DEVSTORE_TABLE_HOST
        else:
            blob_host = CLOUD_BLOB_HOST
            queue_host = CLOUD_QUEUE_HOST
            table_host = CLOUD_TABLE_HOST
            
        self.blobs = BlobStorage(blob_host, account_name, account_key,
                                 use_path_style_uris)
        self.tables = TableStorage(table_host, account_name, account_key,
                                   use_path_style_uris)
        self.queues = QueueStorage(queue_host, account_name, account_key,
                                   use_path_style_uris)

def main():
    pass

if __name__ == '__main__':
    sys.exit(main())
