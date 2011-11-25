#!/usr/bin/env python
# encoding: utf-8
"""
Python wrapper around Windows Azure storage and management APIs

Authors:
    Blair Bethwaite <blair.bethwaite@gmail.com>

License:
    GNU General Public Licence (GPL)
    
    This file is part of pyazure.
    Copyright (c) 2011 Blair Bethwaite <blair.bethwaite@gmail.com>
    
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

import httplib
try:
    from lxml import etree
except ImportError:
    from xml.etree import ElementTree as etree

from util import *
from locations import Locations


class StorageAccounts(ServiceManagementEndpoint):

    wasm_ops = []

    def __init__(self, management_cert_path, subscription_id):
        log.debug('init storage accounts')
        self.wasm_ops = self.get_wasm_ops()
        self._locations = None
        super(StorageAccounts, self).__init__(management_cert_path,
            subscription_id)

    @property
    def base_url(self):
        return super(StorageAccounts, self).base_url \
            + '/services/storageservices'

    @property
    def locations(self):
        # cached list of data center locations for deployments
        if not self._locations:
            self._locations = list(Locations(self.cert,
                self.sub_id).list_locations())
        return self._locations

    def get_wasm_ops(self):
        """Returns a list of bound methods for the Windows Azure Service
        Management operations that this class-instance wraps."""

        return \
        [
            self.list_storage_accounts,
            self.get_storage_account_properties,
            self.get_storage_account_keys,
            self.create_storage_account,
            self.delete_storage_account
        ]
            
    def list_storage_accounts(self, just_names=True):
        """The List Storage Accounts operation lists the storage accounts
        available under the current subscription."""

        log.debug('Getting storage accounts list')
        req = RequestWithMethod('GET', self.base_url)
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.OK:
            self._raise_wa_error(res)
        ET = etree.parse(res)
        storages = ET.findall('.//{%s}StorageService' % NAMESPACE_MANAGEMENT)
        for storage in storages:
            service_name = storage.findtext('{%s}ServiceName'
                % NAMESPACE_MANAGEMENT)
            if just_names:
                yield service_name
            else:
                url = storage.findtext('{%s}Url' % NAMESPACE_MANAGEMENT)
                yield {'Url':url, 'ServiceName':service_name}
    
    wasm_ops.append(list_storage_accounts)

    def get_storage_account_properties(self, name):
        """The Get Storage Account Properties operation returns the system
        properties for the specified storage account. These properties
        include: the address, description, and label of the storage account;
        and the name of the affinity group to which the service belongs, or its
        geo-location if it is not part of an affinity group."""
        
        log.debug('Getting storage account properties')
        req = RequestWithMethod('GET', '%s/%s' % (self.base_url, name))
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.OK:
            self._raise_wa_error(res)
        storage = etree.parse(res)
        props = OrderedDict()
        props['Url'] = storage.findtext('.//{%s}Url' % NAMESPACE_MANAGEMENT)
        props['ServiceName'] = storage.findtext('.//{%s}ServiceName'
            % NAMESPACE_MANAGEMENT)
        props['Description'] = storage.findtext('.//{%s}Description'
            % NAMESPACE_MANAGEMENT)
        affinitygroup = storage.find('.//{%s}AffinityGroup'
            % NAMESPACE_MANAGEMENT)
        if affinitygroup:
            props['AffinityGroup'] = affinitygroup.text
        else:
            props['Location'] = storage.findtext('.//{%s}Location'
                % NAMESPACE_MANAGEMENT)
        label = storage.findtext('.//{%s}Label' % NAMESPACE_MANAGEMENT)
        props['Label'] = base64.b64decode(label)
        props['Status'] = storage.findtext('.//{%s}Status'
            % NAMESPACE_MANAGEMENT)
        endpoints = storage.findall('.//{%s}Endpoint' % NAMESPACE_MANAGEMENT)
        props['Endpoints'] = [endpoint.text for endpoint in endpoints]
        return props

    wasm_ops.append(get_storage_account_properties)

    def get_storage_account_keys(self, name):
        """The Get Storage Keys operation returns the primary and secondary
        access keys for the specified storage account."""
 
        log.debug('Getting storage account keys')
        req = RequestWithMethod('GET', '%s/%s/keys' % (self.base_url, name))
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.OK:
            self._raise_wa_error(res)
        storage = etree.parse(res)
        url = storage.findtext('.//{%s}Url' % NAMESPACE_MANAGEMENT)
        primary = storage.findtext('.//{%s}Primary' % NAMESPACE_MANAGEMENT)
        secondary = storage.findtext('.//{%s}Secondary' % NAMESPACE_MANAGEMENT)
        return (primary,secondary,url)

    wasm_ops.append(get_storage_account_keys)

    def regenerate_storage_account_keys(self):
        pass

    def create_storage_account(self, name, label,
            location_or_affinity_group, description=''):
        """The Create Storage Account (async-)operation creates a new
        storage account in Windows Azure."""

        if not re.match('^[a-z0-9]{3,24}$', name):
            raise ValueError(name,
                'name must be between 3 and 24 characters in length and use '
                + 'numbers and lower-case letters only.')
        if not label:
            raise ValueError(label,'label must be set')
        if len(label) > 100:
            raise ValueError(label,'label must be <= 100 chars')
        if description and len(description) > 1024:
            raise ValueError(description,
                'description must be <= 1024 chars')
        log.debug('Creating storage account: %s', name)
        req = RequestWithMethod('POST', self.base_url)
        req_body = OrderedDict()
        req_body['ServiceName'] = name
        if description:
            req_body['Description'] = description
        req_body['Label'] = base64.b64encode(label)
        try:
            i = [l.lower() for l in self.locations].index(
                location_or_affinity_group.lower().strip())
            req_body['Location'] = self.locations[i]
        except ValueError:
            ##TODO validate AffinityGroup against API
            req_body['AffinityGroup'] = location_or_affinity_group
        req_body = OrderedDict([('CreateStorageServiceInput',req_body)])
        req.add_data(build_wasm_request_body(req_body))
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.ACCEPTED:
            self._raise_wa_error(res)
        request_id = res.headers.getheader('x-ms-request-id')
        log.debug('Request-Id: %s' % request_id)
        return request_id

    wasm_ops.append(create_storage_account)

    def delete_storage_account(self, name):
        """The Delete Storage Account operation deletes the specified storage
        account from Windows Azure."""

        log.debug('Deleting storage account: %s', name)
        req = RequestWithMethod('DELETE', '%s/%s' % (self.base_url,
            name))
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.OK:
            self._raise_wa_error(res)
        request_id = res.headers.getheader('x-ms-request-id')
        log.debug('Request-Id: %s' % request_id)
        return True

    wasm_ops.append(delete_storage_account)

    def update_storage_account(self):
        pass
