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

import base64
import re
import time
import inspect
import math
import hmac
import hashlib
import urllib2
import httplib
import os.path
from urlparse import urlsplit, urljoin
from datetime import datetime, timedelta
from StringIO import StringIO
import logging
logging.basicConfig(level=logging.DEBUG)
try:
    # new in Python2.7
    from collections import OrderedDict
    _builtin_odict = True
except ImportError:
    _builtin_odict = False
try:
    from lxml import etree
except ImportError:
    from xml.etree import ElementTree as etree


# Constants
################################################################################
DEVSTORE_ACCOUNT = "devstoreaccount1"
DEVSTORE_SECRET_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

DEVSTORE_BLOB_HOST = "127.0.0.1:10000"
DEVSTORE_QUEUE_HOST = "127.0.0.1:10001"
DEVSTORE_TABLE_HOST = "127.0.0.1:10002"
SERVICE_MANAGEMENT_HOST = "management.core.windows.net"

CLOUD_QUEUE_HOST = "queue.core.windows.net"
CLOUD_BLOB_HOST = "blob.core.windows.net"
CLOUD_TABLE_HOST = "table.core.windows.net"

PREFIX_PROPERTIES = "x-ms-prop-"
PREFIX_METADATA = "x-ms-meta-"
PREFIX_STORAGE_HEADER = "x-ms-"

MANAGEMENT_VERSION_HEADER = "x-ms-version"
MANAGEMENT_VERSION = "2011-10-01"

NEW_LINE = "\x0A"
TIME_FORMAT ="%a, %d %b %Y %H:%M:%S %Z"

# HTTP headers needed for the continuation tokens in the Table storage API
HEADERS_NEXTPARTITIONKEY = PREFIX_STORAGE_HEADER + "continuation-nextpartitionkey"
HEADERS_NEXTROWKEY = PREFIX_STORAGE_HEADER + "continuation-nextrowkey"
HEADERS_NEXTTABLENAME = PREFIX_STORAGE_HEADER + "continuation-nexttablename"

# Namespaces needed for parsing XML responses with lxml
NAMESPACE_M = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"
NAMESPACE_D = "http://schemas.microsoft.com/ado/2007/08/dataservices"
NAMESPACE_ATOM = "http://www.w3.org/2005/Atom"
NAMESPACE_MANAGEMENT = "http://schemas.microsoft.com/windowsazure"
NAMESPACE_SERVICECONFIG = \
    "http://schemas.microsoft.com/ServiceHosting/2008/10/ServiceConfiguration"

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
TAGS_WA_URL = "{%s}Url" % NAMESPACE_MANAGEMENT
TAGS_WA_SERVICENAME = "{%s}ServiceName" % NAMESPACE_MANAGEMENT

ATTRIBUTES_M_TYPE = "{%s}type" % NAMESPACE_M

# Exceptions
################################################################################
class WAError(Exception):
    pass

class WASMError(WAError):
    def __init__(self, http_status_code, error_code=None,
            user_message=None, httperror=None):
        if isinstance(httperror, urllib2.HTTPError):
            self.httperror = httperror
        else:
            self.httperror = None
        self.http_status_code = http_status_code
        self.error_code = error_code
        self.user_message = user_message
        super(WASMError, self).__init__(http_status_code, error_code,
            user_message)


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

def get_properties(obj):
    return [m for m in inspect.getmembers(obj) if not m[0].startswith("__")]
    
def get_azure_time():
    return time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())

def string_to_wasm_label(s):
    if re.match('^[a-z0-9]{3,24}$', s):
        l = s
    else:
        l = base64.b16encode(s)
        if not re.match('^[a-z0-9]{3,24}$', l):
            raise ValueError('label must be between 3 and 24 characters in '
                + 'length and use numbers and lower-case letters only')
    return l

def wasm_label_to_string(l):
    try:
        s = base64.b16decode(l)
    except TypeError:
        if re.match('^[a-z0-9]{3,24}$', l):
            s = l
        else:
            raise ValueError('label must be between 3 and 24 characters in '
                + 'length and use numbers and lower-case letters only')
    return s

if not _builtin_odict:
    # Copyright (c) 2009 Raymond Hettinger
    #
    # Permission is hereby granted, free of charge, to any person
    # obtaining a copy of this software and associated documentation files
    # (the "Software"), to deal in the Software without restriction,
    # including without limitation the rights to use, copy, modify, merge,
    # publish, distribute, sublicense, and/or sell copies of the Software,
    # and to permit persons to whom the Software is furnished to do so,
    # subject to the following conditions:
    #
    #     The above copyright notice and this permission notice shall be
    #     included in all copies or substantial portions of the Software.
    #
    #     THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
    #     EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
    #     OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    #     NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
    #     HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
    #     WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
    #     FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
    #     OTHER DEALINGS IN THE SOFTWARE.

    from UserDict import DictMixin

    class OrderedDict(dict, DictMixin):

        def __init__(self, *args, **kwds):
            if len(args) > 1:
                raise TypeError('expected at most 1 arguments, got %d'
                    % len(args))
            try:
                self.__end
            except AttributeError:
                self.clear()
            self.update(*args, **kwds)

        def clear(self):
            self.__end = end = []
            end += [None, end, end]    # sentinel node for doubly linked list
            self.__map = {}            # key --> [key, prev, next]
            dict.clear(self)

        def __setitem__(self, key, value):
            if key not in self:
                end = self.__end
                curr = end[1]
                curr[2] = end[1] = self.__map[key] = [key, curr, end]
            dict.__setitem__(self, key, value)

        def __delitem__(self, key):
            dict.__delitem__(self, key)
            key, prev, next = self.__map.pop(key)
            prev[2] = next
            next[1] = prev

        def __iter__(self):
            end = self.__end
            curr = end[2]
            while curr is not end:
                yield curr[0]
                curr = curr[2]

        def __reversed__(self):
            end = self.__end
            curr = end[1]
            while curr is not end:
                yield curr[0]
                curr = curr[1]

        def popitem(self, last=True):
            if not self:
                raise KeyError('dictionary is empty')
            if last:
                key = reversed(self).next()
            else:
                key = iter(self).next()
            value = self.pop(key)
            return key, value

        def __reduce__(self):
            items = [[k, self[k]] for k in self]
            tmp = self.__map, self.__end
            del self.__map, self.__end
            inst_dict = vars(self).copy()
            self.__map, self.__end = tmp
            if inst_dict:
                return (self.__class__, (items,), inst_dict)
            return self.__class__, (items,)

        def keys(self):
            return list(self)

        setdefault = DictMixin.setdefault
        update = DictMixin.update
        pop = DictMixin.pop
        values = DictMixin.values
        items = DictMixin.items
        iterkeys = DictMixin.iterkeys
        itervalues = DictMixin.itervalues
        iteritems = DictMixin.iteritems

        def __repr__(self):
            if not self:
                return '%s()' % (self.__class__.__name__,)
            return '%s(%r)' % (self.__class__.__name__, self.items())

        def copy(self):
            return self.__class__(self)

        @classmethod
        def fromkeys(cls, iterable, value=None):
            d = cls()
            for key in iterable:
                d[key] = value
            return d

        def __eq__(self, other):
            if isinstance(other, OrderedDict):
                if len(self) != len(other):
                    return False
                for p, q in  zip(self.items(), other.items()):
                    if p != q:
                        return False
                return True
            return dict.__eq__(self, other)

        def __ne__(self, other):
            return not self == other


# Retry decorator with exponential backoff
def retry(retries, delay=2, backoff=2, delay_ceiling=0, percolate_excs=()):
    """Retries a function or method until it returns something True.
    
    delay sets the initial delay, backoff sets by how many times delay should
    lengthen after each failure. delay_ceiling caps the delay period.
    percolate_excs is a tuple of exception types that should be re-raised
    immediately if caught during execution of the decorated function.
    
    Usage:
        @retry(3)
        def might_fail(...):
            ...
        or with lambda functions e.g.
        retry(3)(lambda: False)()"""

    if backoff <= 1:
        raise ValueError("backoff must be greater than 1")

    retries = math.floor(retries)
    if retries < 1:
        raise ValueError("retries must be 1 or greater")

    if delay <= 0:
        raise ValueError("delay must be greater than 0")

    if delay_ceiling < 0:
        raise ValueError("delay_ceiling must be >= 0")

    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = retries, delay # make mutable
            try:
                rv = f(*args, **kwargs) # 1st try
                if rv:
                    return rv
            except percolate_excs:
                raise
            except:
                mtries -= 1
            while mtries > 0:
                #print 'retry'
                mtries -= 1
                time.sleep(mdelay)
                mdelay *= backoff
                if delay_ceiling and mdelay > delay_ceiling:
                    mdelay = delay_ceiling
                try:
                    rv = f(*args, **kwargs)
                    if rv:
                        return rv
                except percolate_excs:
                    raise
                except:
                    if mtries == 0:
                        raise
                    else:
                        pass

            return False
        return f_retry # true decorator -> decorated function
    return deco_retry  # @retry(arg[, ...]) -> true decorator

def build_wasm_request_body(xml_as_odict, builder=None, root=True, indent=0):
    """Takes an OrderedDict and uses it to build an XML doc suitable for
    sending as a request body to the Windows Azure Management Service. This
    makes it possible for clients to compactly specify dynamic requests.
    Returns the doc as a string including the expected doctype declaration."""
    while len(xml_as_odict) > 0:
        k,v = xml_as_odict.popitem(last=False)
        #print ' ' * indent + '<%s>' % k
        if not builder:
            builder = etree.TreeBuilder()
            builder.start(k, {'xmlns':NAMESPACE_MANAGEMENT})
        else:
            builder.start(k, {})
        if isinstance(v,dict):
            # recurse
            build_wasm_request_body(v, builder, root=False, indent=indent+2)
        else:
            #print ' ' * indent + ' ' + v
            builder.data(v)
        #print ' ' * indent + '</%s>' % k
        builder.end(k)
    if root:
        body = u'<?xml version="1.0" encoding="utf-8"?>'
        body += etree.tostring(builder.close(), encoding='utf-8')
        body += NEW_LINE
        return body

# Windows Azure Storage APIs classes
################################################################################
class SharedKeyCredentials(object):
    def __init__(self, account_name, account_key, use_path_style_uris = None):
        self._account = account_name
        self._key = base64.decodestring(account_key)

    def _sign_request_impl(self, request, for_tables = False, use_path_style_uris = None):
        (scheme, host, path, query, fragment) = \
            urlsplit(request.get_full_url())
        if use_path_style_uris:
            path = path[path.index('/'):]

        canonicalized_resource = "/" + self._account + path
        match = re.search(r'comp=[^&]*', query)
        if match is not None:
            canonicalized_resource += "?" + match.group(0)
            
        if use_path_style_uris is None:
            use_path_style_uris = re.match('^[\d.:]+$', host) is not None

        #RFC 1123
        request.add_header(PREFIX_STORAGE_HEADER + 'date', get_azure_time())
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
        
        request.add_header('Authorization', 'SharedKey ' + self._account + ':'
            + base64.encodestring(hmac.new(self._key,
            unicode(string_to_sign).encode("utf-8"),
            hashlib.sha256).digest()).strip())
        return request

    def sign_request(self, request, use_path_style_uris = None):
        return self._sign_request_impl(request, use_path_style_uris)

    def sign_table_request(self, request, use_path_style_uris = None):
        return self._sign_request_impl(request, for_tables = True,
            use_path_style_uris = use_path_style_uris)

class RequestWithMethod(urllib2.Request):
    '''Subclass urllib2.Request to add the capability of using methods other
    than GET and POST.
    (Thanks to http://benjamin.smedbergs.us/blog/2008-10-21/putting-and-
        deleteing-in-python-urllib2/)'''

    def __init__(self, method, *args, **kwargs):
        self._method = method
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method

#class RequestWithMethod(urllib2.Request):
#  def __init__(self, *args, **kwargs):
#    self._method = kwargs.get('method')
#    if self._method:
#        del kwargs['method']
#    urllib2.Request.__init__(self, *args, **kwargs)
#
#  def get_method(self):
#    return self._method if self._method else super(RequestWithMethod, self).get_method()

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


# Windows Azure Management API
################################################################################

def create_data_connection_string(storage_account_name, storage_account_key):
    if storage_account_name != DEVSTORE_ACCOUNT:
        return u'DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s'\
            % (storage_account_name, storage_account_key)
    else:
        return u'UseDevelopmentStorage=true'

class ServiceManagementEndpoint(object):

    # static OpenerDirector for all sub-classes
    # NB: NOT thread-safe
    _cert_handler = None
    _opener = None
   
    def __init__(self, management_cert_path, subscription_id):
        if not os.path.isfile(management_cert_path):
            raise ValueError('Management certificate not readable or not '
                + 'a real file')
        self.cert = management_cert_path
        self.sub_id = subscription_id
        if not ServiceManagementEndpoint._cert_handler:
            ServiceManagementEndpoint._cert_handler = \
                HTTPSClientAuthHandler(self.cert)
            ServiceManagementEndpoint._opener = \
                urllib2.build_opener(self._cert_handler)
        #urllib2.install_opener(ServiceManagementEndpoint._opener)

    @property
    def base_url(self):
        return 'https://%s/%s' % (SERVICE_MANAGEMENT_HOST,self.sub_id)

    def urlopen(self, request):
        """Directs urlopen requests to the ServiceManagementEndpoint
        OpenDirector, which handles HTTPS client cert authn for the API.
        Expects a urllib2.Request object."""
        try:
            return ServiceManagementEndpoint._opener.open(request)
        except urllib2.HTTPError, e:
            logging.debug('HTTP Response: %s %s', e.code, e.msg)
            self._raise_wa_error(e)

    def get_operation_status(self, request_id):
        """The Get Operation Status operation returns the status of the
        specified operation. After calling an asynchronous operation, you
        can call Get Operation Status to determine whether the operation
        has succeeded, failed, or is still in progress."""

        req = RequestWithMethod('GET', 'https://%s/%s/operations/%s' %
            (SERVICE_MANAGEMENT_HOST, self.sub_id, request_id))
        res = self.urlopen(req)
        if not res.code == httplib.OK:
            self._raise_wa_error(res)
        ET = etree.parse(res)
        result = dict()
        result['Status'] = ET.findtext(
            './/{%s}Status' % NAMESPACE_MANAGEMENT)
        if result['Status'] == 'InProgress':
            return result
        # Succeeded or Failed...
        result['HttpStatusCode'] = ET.findtext(
            './/{%s}HttpStatusCode' % NAMESPACE_MANAGEMENT)
        if result['Status'] == 'Succeeded':
            return result
        # Status must be 'Failed', get additional error info
        error = ET.find('.//{%s}Error' % NAMESPACE_MANAGEMENT)
        if error is not None:
            result['Error'] = WASMError(*self._get_wa_error(ET))
        return result

    def request_done(self, request_id):
        """Poll asynchronous operation status and indicates whether the
        request has completed (True), is still in-progress (False), or failed
        (WASMError exception raised)."""
        op_status = self.get_operation_status(request_id)
        if op_status['Status'] == 'Succeeded':
            return True
        elif op_status['Status'] == 'InProgress':
            return False
        else:
            raise op_status['Error']

    @retry(float('infinity'),delay_ceiling=20, percolate_excs=(WASMError))
    def wait_for_request(self, request_id):
        """Example showing how to repeatedly poll asynchronous operation status
        with retry and backoff provided by retry decorator. Tries forever."""
        return self.request_done(request_id)

    def _get_wa_error(self, response):
        """Extracts error details from a urlopen response, including extended
        WA error details that might be included in the response body."""
        if not isinstance(response, etree.ElementTree):
            response_data = response.read()
            ET = etree.parse(StringIO(response_data))
        else:
            # assume response is a preparsed response body
            ET = response
        if 'Error' in ET.getroot().tag:
            error = ET.getroot()
        else:
            error = ET.find('.//{%s}Error' % NAMESPACE_MANAGEMENT)
        # if this is the failed response from an async operation it'll include 
        # the status code element
        http_status_code = ET.findtext(
            './/{%s}HttpStatusCode' % NAMESPACE_MANAGEMENT)
        if http_status_code is None:
            # just a regular synchronous response
            http_status_code = response.getcode()
        else:
            http_status_code = int(http_status_code)
        # NB: careful: bool(Element instance) returns False!
        if error is not None:
            # the service returned extended WA error info
            wa_code = error.findtext('{%s}Code' % NAMESPACE_MANAGEMENT)
            wa_message = error.findtext('{%s}Message' % NAMESPACE_MANAGEMENT)
        else:
            wa_code, wa_message = None, None
        return (http_status_code, wa_code, wa_message)

    def _raise_wa_error(self, response):
        """Raise a WASMError exception populated with values derived from
        _get_wa_error."""
        raise WASMError(*self._get_wa_error(response), httperror=response)

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
# thanks to: http://stackoverflow.com/questions/5896380/https-connection-
#   using-pem-certificate/5899320#5899320
    def __init__(self, cert):
        urllib2.HTTPSHandler.__init__(self, debuglevel=0)
        self.key = None # assume key & cert together in PEM encoded cert_file
        self.cert = cert
    
    def https_open(self, req):
        # Rather than pass in a reference to a connection class, we pass in
        # a reference to a function which, for all intents and purposes,
        # will behave as a constructor
        if not req.has_header(MANAGEMENT_VERSION_HEADER):
            req.headers[MANAGEMENT_VERSION_HEADER] = MANAGEMENT_VERSION
        if req.has_data() and not req.has_header('Content-Type'):
            req.headers['Content-Type'] = 'application/xml'
        if not req.has_header('Content-Type'):
            req.headers['Content-Type'] = ''
        logging.debug('Request: %s; %s; %s; %s;', req.get_method(),
            req.get_full_url(), req.headers, req.get_data())
        return self.do_open(self.getConnection, req)
    
    def getConnection(self, host, *args, **kargs):
        # Note that we are accepting all args but only passing the host
        # through at this point. In python>=2.6 a timeout parameter is given
        # too.
        return httplib.HTTPSConnection(host, key_file=self.key,
                cert_file=self.cert)

