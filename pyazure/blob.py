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

import time
from lxml import etree
from urllib2 import Request, urlopen, URLError

from util import *

class BlobStorage(Storage):
    def __init__(self, host, account_name, secret_key, use_path_style_uris):
        super(BlobStorage, self).__init__(host, account_name, secret_key,
                                          use_path_style_uris)

    def create_container(self, container_name, is_public = False):
        req = RequestWithMethod("PUT", "%s/%s" % (self.get_base_url(), container_name))
        req.add_header("Content-Length", "0")
        if is_public: req.add_header(PREFIX_PROPERTIES + "publicaccess", "true")
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def delete_container(self, container_name):
        req = RequestWithMethod("DELETE", "%s/%s" % (self.get_base_url(), container_name))
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def list_containers(self):
        req = Request("%s/?comp=list" % self.get_base_url())
        self._credentials.sign_request(req)
        dom = etree.fromstring(urlopen(req).read())
        containers = dom.findall("Container")
        for container in containers:
            container_name = container.find("Name").text
            etag = container.find("Etag").text
            last_modified = time.strptime(container.find("LastModified").text, TIME_FORMAT)
            yield (container_name, etag, last_modified)

    def list_blobs(self, container_name):
        req = Request("%s/$s?comp=list" % (self.get_base_url(), container_name))
        self._credentials.sign_request(req)
        dom = etree.fromstring(urlopen(req).read())
        containers = dom.findall("Blob")
        for container in containers:
            container_name = container.find("Name").text
            etag = container.find("Etag").text
            last_modified = time.strptime(container.find("LastModified").text, TIME_FORMAT)
            yield (container_name, etag, last_modified)

    def put_blob(self, container_name, blob_name, data, content_type = None):
        req = RequestWithMethod("PUT", "%s/%s/%s" % (self.get_base_url(), container_name, blob_name), data=data)
        req.add_header("Content-Length", "%d" % len(data))
        if content_type is not None:
            req.add_header("Content-Type", content_type)
        self._credentials.sign_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def get_blob(self, container_name, blob_name):
        req = Request("%s/%s/%s" % (self.get_base_url(), container_name, blob_name))
        self._credentials.sign_request(req)
        return urlopen(req).read()
    
    def delete_blob(self, container_name, blob_name):
        req = Request("DELETE", "%s/%s/%s" % (self.get_base_url(), container_name,
                      blob_name))
        self._credentials.sign_request(req)
        return urlopen(req).read()
