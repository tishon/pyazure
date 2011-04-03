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

import base64
import re
import time
import hmac
import hashlib
from urlparse import urlsplit
from datetime import datetime, timedelta
from urllib2 import Request, urlopen, URLError

# Constants
################################################################################
DEVSTORE_ACCOUNT = "devstoreaccount1"
DEVSTORE_SECRET_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

DEVSTORE_BLOB_HOST = "127.0.0.1:10000"
DEVSTORE_QUEUE_HOST = "127.0.0.1:10001"
DEVSTORE_TABLE_HOST = "127.0.0.1:10002"

CLOUD_QUEUE_HOST = "queue.core.windows.net"
CLOUD_BLOB_HOST = "blob.core.windows.net"
CLOUD_TABLE_HOST = "table.core.windows.net"

PREFIX_PROPERTIES = "x-ms-prop-"
PREFIX_METADATA = "x-ms-meta-"
PREFIX_STORAGE_HEADER = "x-ms-"

NEW_LINE = "\x0A"
TIME_FORMAT ="%a, %d %b %Y %H:%M:%S %Z"

# HTTP headers needed for the continuation tokens in the Table storage API
HEADERS_NEXTPARTITIONKEY = PREFIX_STORAGE_HEADER + "continuation-nextpartitionkey"
HEADERS_NEXTROWKEY = PREFIX_STORAGE_HEADER + "continuation-nextrowkey"

# Namespaces needed for parsing XML responses with lxml
NAMESPACE_M = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"
NAMESPACE_D = "http://schemas.microsoft.com/ado/2007/08/dataservices"
NAMESPACE_ATOM = "http://www.w3.org/2005/Atom"

# Tags with Clark notation for the namespaces. Needed for parsing XML responses
# with lxml
TAGS_ATOM_ENTRY = "{%s}entry" % NAMESPACE_ATOM
TAGS_ATOM_ID = "{%s}id" % NAMESPACE_ATOM
TAGS_ATOM_CONTENT = "{%s}content" % NAMESPACE_ATOM
TAGS_ATOM_ENTRY = "{%s}entry" % NAMESPACE_ATOM
TAGS_ATOM_QUEUEMESSAGE = "{%s}QueueMessage" % NAMESPACE_ATOM
TAGS_ATOM_MESSAGEID = "{%s}MessageId" % NAMESPACE_ATOM
TAGS_ATOM_POPRECEIPT = "{%s}PopReceipt" % NAMESPACE_ATOM
TAGS_ATOM_MESSAGETEXT = "{%s}MessageText" % NAMESPACE_ATOM
TAGS_M_PROPERTIES = "{%s}properties" % NAMESPACE_M
TAGS_D_TABLENAME = "{%s}TableName" % NAMESPACE_D

ATTRIBUTES_M_TYPE = "{%s}type" % NAMESPACE_M

# Helper functions
################################################################################
def add_url_parameter(request_string, key, value):
    separator = "&" if "?" in request_string else "?"
    return "%s%s%s=%s" % (request_string, separator, key, value)

def get_tag_name_without_namespace(tag):
    return tag.split("}")[-1] if "}" in tag else tag

def parse_edm_datetime(input):
    d = datetime.strptime(input[:input.find('.')], "%Y-%m-%dT%H:%M:%S")
    if input.find('.') != -1:
        d += timedelta(0, 0, int(round(float(input[input.index('.'):-1])*1000000)))
    return d

def parse_edm_int32(input):
    return int(input)

def parse_edm_double(input):
    return float(input)

def parse_edm_boolean(input):
    return input.lower() == "true"
    
# Windows Azure Storage APIs classes
################################################################################
class SharedKeyCredentials(object):
    def __init__(self, account_name, account_key, use_path_style_uris = None):
        self._account = account_name
        self._key = base64.decodestring(account_key)

    def _sign_request_impl(self, request, for_tables = False, use_path_style_uris = None):
        (scheme, host, path, query, fragment) = urlsplit(request.get_full_url())
        if use_path_style_uris:
            path = path[path.index('/'):]

        canonicalized_resource = "/" + self._account + path
        match = re.search(r'comp=[^&]*', query)
        if match is not None:
            canonicalized_resource += "?" + match.group(0)
            
        if use_path_style_uris is None:
            use_path_style_uris = re.match('^[\d.:]+$', host) is not None

        #RFC 1123
        request.add_header(PREFIX_STORAGE_HEADER + 'date', time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()))
        canonicalized_headers = NEW_LINE.join(('%s:%s' % (k.lower(), request.get_header(k).strip()) for k in sorted(request.headers.keys(), lambda x,y: cmp(x.lower(), y.lower())) if k.lower().startswith(PREFIX_STORAGE_HEADER)))

        # verb
        string_to_sign = request.get_method().upper() + NEW_LINE
        # MD5 not required
        string_to_sign += NEW_LINE
        # Content-Type
        if request.get_header('Content-type') is not None:
            string_to_sign += request.get_header('Content-type')
        string_to_sign += NEW_LINE
        if for_tables:
            string_to_sign += request.get_header(PREFIX_STORAGE_HEADER.capitalize() + 'date') + NEW_LINE
        else:
            # Date
            string_to_sign += NEW_LINE
        if not for_tables:
            # Canonicalized headers
            string_to_sign += canonicalized_headers + NEW_LINE
        # Canonicalized resource
        string_to_sign += canonicalized_resource
        
        request.add_header('Authorization', 'SharedKey ' + self._account + ':' + base64.encodestring(hmac.new(self._key, unicode(string_to_sign).encode("utf-8"), hashlib.sha256).digest()).strip())
        return request

    def sign_request(self, request, use_path_style_uris = None):
        return self._sign_request_impl(request, use_path_style_uris)

    def sign_table_request(self, request, use_path_style_uris = None):
        return self._sign_request_impl(request, for_tables = True, use_path_style_uris = use_path_style_uris)

class RequestWithMethod(Request):
    '''Subclass urllib2.Request to add the capability of using methods other than GET and POST.
       Thanks to http://benjamin.smedbergs.us/blog/2008-10-21/putting-and-deleteing-in-python-urllib2/'''
    def __init__(self, method, *args, **kwargs):
        self._method = method
        Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method

class Storage(object):
    def __init__(self, host, account_name, secret_key, use_path_style_uris):
        self._host = host
        self._account = account_name
        self._key = secret_key
        if use_path_style_uris is None:
            use_path_style_uris = re.match(r'^[^:]*[\d:]+$', self._host)
        self._use_path_style_uris = use_path_style_uris
        self._credentials = SharedKeyCredentials(self._account, self._key)

    def get_base_url(self):
        if self._use_path_style_uris:
            return "http://%s/%s" % (self._host, self._account)
        else:
            return "http://%s.%s" % (self._account, self._host)
