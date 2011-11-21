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
try:
    from lxml import etree
except ImportError:
    from xml.etree import ElementTree as etree
from urllib2 import Request, urlopen, URLError

from util import *

class Table(object):
    def __init__(self, url, name):
        self.url = url
        self.name = name

class TableEntity(object):
    def __init__(self, partition_key="", row_key="", timestamp=""):
        self.PartitionKey = partition_key
        self.RowKey = row_key
        self.Timestamp = timestamp

class TableStorage(Storage):
    '''Due to local development storage not supporting SharedKey authentication, this class
       will only work against cloud storage.'''
    def __init__(self, host, account_name, secret_key,
            use_path_style_uris=None):
        super(TableStorage, self).__init__(host, account_name, secret_key,
            use_path_style_uris)

    def create_table(self, name):
        data = """<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<entry xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns="http://www.w3.org/2005/Atom">
  <title />
  <updated>%s</updated>
  <author>
    <name />
  </author>
  <id />
  <content type="application/xml">
    <m:properties>
      <d:TableName>%s</d:TableName>
    </m:properties>
  </content>
</entry>""" % (time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()), name)
        req = RequestWithMethod("POST", "%s/Tables" % self.get_base_url(), data=data)
        req.add_header("Content-Length", "%d" % len(data))
        req.add_header("Content-Type", "application/atom+xml")
        self._credentials.sign_table_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def delete_table(self, name):
        req = RequestWithMethod("DELETE", "%s/Tables('%s')" % \
                                (self.get_base_url(), name))
        self._credentials.sign_table_request(req)
        try:
            response = urlopen(req)
            return response.code
        except URLError, e:
            return e.code

    def list_tables(self):
        request_necessary = True
        next_table_name = None
        request_string = "%s/Tables" % self.get_base_url()
        
        while request_necessary:
            request = self._get_tables_request(request_string, next_table_name)
            request_necessary, next_table_name =\
                self._get_tables_continuation_token(request)
            
            for table in self._get_tables(request):
                yield table

    def insert_entity(self, table_name, entity):
        raw_properties = get_properties(entity)
        # TODO:
        # convert properties to xml (mind namespaces)
        properties = []

        data = """<?xml version="1.0" encoding="utf-8" standalone="yes"?>
<entry xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns="http://www.w3.org/2005/Atom">
  <title />
  <updated>%s<updated/>
  <author>
    <name />
  </author>
  <id />
  <content type="application/xml">
    <m:properties>
      %s
    </m:properties>
  </content>
</entry>"""  % (time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()), properties)

    def get_entity(self, table_name, partition_key, row_key):
        request_object = Request("%s/%s(PartitionKey='%s',RowKey='%s')" % \
            (self.get_base_url(), table_name, partition_key, row_key))
        request = self._get_signed_request(request_object)
        response = request.read()
        dom = etree.fromstring(response)
        entity = self._parse_entity(dom.find(TAGS_ATOM_ENTRY))
        return entity
    
    def get_entities(self, table_name, partition_key=None, top=None, filters=None):
        """Get entities optionally filtered by partition key, number of results,
         or by a custom filter string."""
        request_string = self.get_base_url() + "/" + table_name + "()"
        if partition_key and "PartitionKey" not in filters:
           if filters:
               filters += "%20and%20"
           else:
               filters = ""
           filters += "PartitionKey%20eq%20'" + partition_key + "'"
        if filters:
           request_string = add_url_parameter(request_string, "$filter", filters)
        if top:
            request_string = add_url_parameter(request_string, "$top", top)
        
        # variables for handling continuation tokens
        request_necessary = True
        next_partition_key = None
        next_row_key = None
        
        while request_necessary:
            request = self._get_entities_request(request_string,
                        next_partition_key, next_row_key)
            request_necessary, next_partition_key, next_row_key =\
                self._get_entities_continuation_tokens(request)
            if "$top" in request_string:
                request_necessary = False
            
            for entity in self._get_entities(request):
                yield entity

    def delete_entity(self, table_name, partition_key, row_key):
        request_string = "%s/%s(PartitionKey='%s',RowKey='%s')" % \
            (self.get_base_url(), table_name, partition_key, row_key)
        request_object = Request("DELETE",request_string)
        request = self._get_signed_request(request_object)

    def _get_entities_request(self, request_string, next_partition_key,
                              next_row_key):
        if next_partition_key:
            request_string = add_url_parameter(request_string, "NextPartitionKey",
                next_partition_key)
        if next_row_key:
            request_string = add_url_parameter(request_string, "NextRowKey",
                next_row_key)
        request_object = Request(request_string)
        return self._get_signed_request(request_object)

    def _get_tables_request(self, request_string, next_table_name):
        if next_table_name:
            request_string = add_url_parameter(request_string, "NextTableName",
                                               next_table_name)
        request_object = Request(request_string)
        return self._get_signed_request(request_object)

    def _get_signed_request(self, request):
        return urlopen(self._credentials.sign_table_request(request))

    def _get_entities(self, request):
        response = request.read()
        dom = etree.fromstring(response)
        return [self._parse_entity(e) for e in dom.findall(TAGS_ATOM_ENTRY)]

    def _parse_entity(self, entry):
        entity = TableEntity()
        properties_element = entry.find('.//' + TAGS_M_PROPERTIES)
        properties = (p for p in properties_element.getchildren())
        for property in properties:
            self._parse_property(entity, property)
        return entity

    def _get_tables(self, request):
        response = request.read()
        dom = etree.fromstring(response)
        return [self._parse_table(t) for t in dom.findall(TAGS_ATOM_ENTRY)]
    
    def _parse_table(self, entry):
        table_url = entry.find(TAGS_ATOM_ID).text
        table_name = entry.find('.//' + TAGS_D_TABLENAME).text
        return Table(table_url, table_name)

    def _parse_property(self, entity, property):
        key = get_tag_name_without_namespace(property.tag)
        if property.get(ATTRIBUTES_M_TYPE):
            t = property.get(ATTRIBUTES_M_TYPE).lower()
            if t == 'edm.datetime': value = parse_edm_datetime(property.text)
            elif t == 'edm.int32': value = parse_edm_int32(property.text)
            elif t == 'edm.boolean': value = parse_edm_boolean(property.text)
            elif t == 'edm.double': value = parse_edm_double(property.text)
            else: raise Exception(t)
        else:
            value = property.text
        setattr(entity, key, value)
    
    def _get_entities_continuation_tokens(self, response):
        """Returns continuation tokens for table entity queries"""
        # set default values
        request_necessary, next_partition_key, next_row_key = True, None, None
        
        if HEADERS_NEXTPARTITIONKEY not in response.headers.keys():
            request_necessary = False
        else:
            next_partition_key = response.headers[HEADERS_NEXTPARTITIONKEY]
            # in some cases the NextRowKey header may be null
            if HEADERS_NEXTROWKEY in response.headers.keys():
                next_row_key = response.headers[HEADERS_NEXTROWKEY]
        return (request_necessary, next_partition_key, next_row_key)
    
    def _get_tables_continuation_token(self, response):
        """Returns continuation token for table queries"""
        request_necessary = True
        next_table_name = None
        
        if HEADERS_NEXTTABLENAME not in response.headers.keys():
            request_necessary = False
        else:
            next_table_name = request.headers[HEADERS_NEXTTABLENAME]
        return (request_necessary, next_table_name)
