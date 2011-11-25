#!/usr/bin/env python
# encoding: utf-8
"""
Python wrapper around Windows Azure storage and management APIs

This management code is based in part on the winazureservice.py example
from drelu@github.com.

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
import base64
from xml.dom import minidom
import time
import os.path
from StringIO import StringIO
try:
    from lxml import etree
except ImportError:
    from xml.etree import ElementTree as etree

from util import *
from locations import Locations


class HostedServices(ServiceManagementEndpoint):
    
    def __init__(self, management_cert_path, subscription_id):
        log.debug("init hosted service")
        self.wasm_ops = self.get_wasm_ops()
        self._locations = None
        self.last_response_data = None
        super(HostedServices, self).__init__(management_cert_path,
            subscription_id)
    
    @property
    def base_url(self):
        return super(HostedServices, self).base_url \
            + '/services/hostedservices'

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
            self.list_services,
            self.delete_service,
            self.create_service,
            self.get_service_properties,
            self.create_deployment,
            self.get_deployment,
            self.delete_deployment,
            self.update_deployment_status,
            self.change_deployment_configuration,
        ]

    def list_services(self, just_names=True):
        """The List Hosted Services operation lists the hosted services
        available under the current subscription."""

        log.debug('Getting hosted services list')
        req = RequestWithMethod('GET', self.base_url)
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.OK:
            self._raise_wa_error(res)
        ET = etree.parse(res)
        services = ET.findall('.//{%s}HostedService' % NAMESPACE_MANAGEMENT)
        for service in services:
            service_name = service.findtext('{%s}ServiceName'
                % NAMESPACE_MANAGEMENT)
            if just_names:
                yield service_name
            else:
                url = service.findtext('{%s}Url' % NAMESPACE_MANAGEMENT)
                yield {'Url':url, 'ServiceName':service_name}

    def delete_service(self, service_name):
        """The Delete Hosted Service operation deletes the specified hosted
        service from Windows Azure."""

        log.debug('Deleting hosted service: %s', service_name)
        req = RequestWithMethod('DELETE', '%s/%s' % (self.base_url,
            service_name))
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.OK:
            self._raise_wa_error(res)
        request_id = res.headers.getheader('x-ms-request-id')
        log.debug('Request-Id: %s', request_id)
        return True

    def update_service(self):
        pass

    def create_service(self, service_name, label, location_or_affinity_group,
            description=''):
        """The Create Hosted Service operation creates a new hosted service
        in Windows Azure."""

        log.debug('Creating hosted service: %s', service_name)
        if not label:
            raise ValueError(label,'label must be set')
        if len(label) > 100:
            raise ValueError(label,'label exceeds 100 char limit')
        if len(description) > 1024:
            raise ValueError(description,'description exceeds 1024 char limit')
        req = RequestWithMethod('POST', self.base_url)
        req_body = OrderedDict()
        req_body['ServiceName'] = service_name
        req_body['Label'] = base64.b64encode(label)
        if description:
            req_body['Description'] = description
        try:
            i = [l.lower() for l in self.locations].index(
                location_or_affinity_group.lower().strip())
            req_body['Location'] = self.locations[i]
        except ValueError:
            ##TODO validate AffinityGroup against API
            req_body['AffinityGroup'] = location_or_affinity_group
        req_body = OrderedDict([('CreateHostedService',req_body)])
        req.add_data(build_wasm_request_body(req_body))
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.CREATED:
            self._raise_wa_error(res)
        request_id = res.headers.getheader('x-ms-request-id')
        log.debug('Request-Id: %s' % request_id)
        return True

    def get_service_properties(self, service_name, embed_detail=False):
        """The Get Hosted Service Properties operation retrieves system
        properties for the specified hosted service. These properties include
        the service name and service type; the name of the affinity group to
        which the service belongs, or its location if it is not part of an
        affinity group; and optionally, information on the service's
        deployments."""
        
        log.debug('Getting hosted service info: %s', service_name)
        if embed_detail:
            req = RequestWithMethod('GET', '%s/%s?embed-detail=true'
                % (self.base_url, service_name))
        else:
            req = RequestWithMethod('GET', '%s/%s'
                % (self.base_url, service_name))
        res = self.urlopen(req)
        if res.code != httplib.OK:
            self._raise_wa_error(res)
        service = etree.parse(res)
        info = OrderedDict()
        info['Url'] = service.findtext('.//{%s}Url' % NAMESPACE_MANAGEMENT)
        info['ServiceName'] = service.findtext('.//{%s}ServiceName'
            % NAMESPACE_MANAGEMENT)
        props = OrderedDict()
        info['HostedServiceProperties'] = props
        props['Description'] = service.findtext('.//{%s}Description'
            % NAMESPACE_MANAGEMENT)
        props['Location'] = service.findtext('.//{%s}Location'
            % NAMESPACE_MANAGEMENT)
        affinitygroup = service.findtext('.//{%s}AffinityGroup'
            % NAMESPACE_MANAGEMENT)
        if affinitygroup:
            props['AffinityGroup'] = affinitygroup
        else:
            props['Location'] = service.findtext('.//{%s}Location'
                % NAMESPACE_MANAGEMENT)
        props['Label'] = base64.b64decode(
            service.findtext('.//{%s}Label' % NAMESPACE_MANAGEMENT))
        if embed_detail:
            deployments = service.findall('.//{%s}Deployment'
                % NAMESPACE_MANAGEMENT)
            info['Deployments'] = []
            for deployment in deployments:
                info['Deployments'].append(self._parse_deployment(deployment))
        return info

    def create_deployment(self, service_name, deployment_slot, name,
            package_url, label, configuration, start_deployment=False,
            treat_warnings_as_error=False):
        """The Create Deployment (async-)operation uploads a new service
        package and creates a new deployment on staging or production."""

        if deployment_slot.lower() not in ('staging','production'):
            raise ValueError(deployment_slot,'deployment_slot must be '
                + '"staging" or "production"')
        if len(label) > 100:
            raise ValueError(label,'label exceeds 100 char limit')
        if isinstance(configuration, basestring):
            if os.path.isfile(configuration):
                config = open(configuration).read()
            else:
                config = configuration
        elif isinstance(configuration, file):
            config = configuration.read()
        else:
            config = configuration
        
        req = RequestWithMethod('POST', '%s/%s/deploymentslots/%s'
            % (self.base_url, service_name, deployment_slot))
        req_body = OrderedDict()
        req_body['Name'] = name
        req_body['PackageUrl'] = package_url
        req_body['Label'] = base64.b64encode(label)
        req_body['Configuration'] = base64.b64encode(`config`)
        if start_deployment:
            req_body['StartDeployment'] = u'true'
        if treat_warnings_as_error:
            req_body['TreatWarningsAsError'] = u'true'
        req_body = OrderedDict([('CreateDeployment',req_body)])
        req.add_data(build_wasm_request_body(req_body))
        res = self.urlopen(req)
        if res.code != httplib.ACCEPTED:
            self._raise_wa_error(res)
        request_id = res.headers.getheader('x-ms-request-id')
        log.debug('Request-Id: %s', request_id)
        return request_id
    
    def get_deployment(self, service_name, name):
        """The Get Deployment operation returns configuration information,
        status, and system properties for a deployment."""

        log.debug('Getting deployment info: %s - %s', service_name, name)
        req = RequestWithMethod('GET', '%s/%s/deployments/%s'
            % (self.base_url, service_name, name))
        res = self.urlopen(req)
        self.last_response_data = res.read()
        res.fp = StringIO(self.last_response_data)
        res.read = res.fp.read
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.OK:
            self._raise_wa_error(res)
        deployment = etree.parse(res)
        return self._parse_deployment(deployment.getroot())

    def _parse_deployment(self, deployment_element):
        """Parses an ElementTree.Element object to retrieve various values
        from sub-elements and return them as a dict."""

        de = deployment_element
        info = OrderedDict()
        info['Name'] = de.findtext('.//{%s}Name' % NAMESPACE_MANAGEMENT)
        info['DeploymentSlot'] = de.findtext('.//{%s}DeploymentSlot'
            % NAMESPACE_MANAGEMENT)
        info['PrivateID'] = de.findtext('.//{%s}PrivateID'
            % NAMESPACE_MANAGEMENT)
        info['Status'] = de.findtext('.//{%s}Status' % NAMESPACE_MANAGEMENT)
        info['Label'] = base64.b64decode(
            de.findtext('.//{%s}Label' % NAMESPACE_MANAGEMENT))
        info['Url'] = de.findtext('.//{%s}Url' % NAMESPACE_MANAGEMENT)
        info['Configuration'] = base64.b64decode(
            de.findtext('.//{%s}Configuration' % NAMESPACE_MANAGEMENT))
        role_instances = de.findall('.//{%s}RoleInstance'
            % NAMESPACE_MANAGEMENT)
        info['RoleInstanceList'] = []
        for ri in role_instances:
            ri_info = OrderedDict()
            ri_info['RoleName'] = ri.findtext('.//{%s}RoleName'
                % NAMESPACE_MANAGEMENT)
            ri_info['InstanceName'] = de.findtext('.//{%s}InstanceName'
                % NAMESPACE_MANAGEMENT)
            ri_info['InstanceStatus'] = de.findtext('.//{%s}InstanceStatus'
                % NAMESPACE_MANAGEMENT)
            ri_info['InstanceUpgradeDomain'] = de.findtext(
                './/{%s}InstanceUpgradeDomain' % NAMESPACE_MANAGEMENT)
            ri_info['InstanceFaultDomain'] = de.findtext(
                './/{%s}InstanceFaultDomain' % NAMESPACE_MANAGEMENT)
            ri_info['InstanceSize'] = de.findtext('.//{%s}InstanceSize'
                % NAMESPACE_MANAGEMENT)
            ri_info['InstanceStateDetails'] = de.findtext(
                './/{%s}InstanceStateDetails' % NAMESPACE_MANAGEMENT)
            ri_info['InstanceErrorCode'] = de.findtext(
                './/{%s}InstanceErrorCode' % NAMESPACE_MANAGEMENT)
            info['RoleInstanceList'].append(ri_info)
        upgrade_status = de.find('.//{%s}UpgradeStatus'
            % NAMESPACE_MANAGEMENT)
        if upgrade_status:
            info['UpgradeType'] = de.findtext('.//{%s}UpgradeType'
                % NAMESPACE_MANAGEMENT)
            info['CurrentUpgradeDomainState'] = de.findtext(
                './/{%s}CurrentUpgradeDomainState' % NAMESPACE_MANAGEMENT)
            info['CurrentUpgradeDomain'] = de.findtext(
                './/{%s}CurrentUpgradeDomain' % NAMESPACE_MANAGEMENT)
        info['UpgradeDomainCount'] = de.findtext(
            './/{%s}UpgradeDomainCount' % NAMESPACE_MANAGEMENT)
        roles = de.findall('.//{%s}Role' % MANAGEMENT_VERSION)
        info['RoleList'] = []
        for role in roles:
            role_info = OrderedDict()
            role_info['RoleName'] = role.findtext('.//{%s}RoleName'
                % NAMESPACE_MANAGEMENT)
            role_info['OsVersion'] = role.findtext('.//{%s}OsVersion'
                % NAMESPACE_MANAGEMENT)
            info['RoleList'].append(role_info)
        info['SdkVersion'] = de.findtext('.//{%s}SdkVersion'
            % NAMESPACE_MANAGEMENT)
        input_endpoints = de.findall('.//{%s}InputEndpoint'
            % MANAGEMENT_VERSION)
        info['InputEndpointList'] = []
        for endpoint in input_endpoints:
            endpoint_info = OrderedDict()
            endpoint_info['RoleName'] = endpoint.findtext('.//{%s}RoleName'
                % NAMESPACE_MANAGEMENT)
            endpoint_info['Vip'] = endpoint.findtext('.//{%s}Vip'
                % NAMESPACE_MANAGEMENT)
            endpoint_info['Port'] = endpoint.findtext('.//{%s}Port'
                % NAMESPACE_MANAGEMENT)
            info['InputEndpointList'].append(endpoint_info)
        info['Locked'] = de.findtext('.//{%s}Locked' % NAMESPACE_MANAGEMENT)
        info['RollbackAllowed'] = de.findtext('.//{%s}RollbackAllowed'
            % NAMESPACE_MANAGEMENT)
        return info

    def swap_deployment(self):
        pass

    def delete_deployment(self, service_name, deployment_slot_or_name):
        """The Delete Deployment (async-)operation deletes the specified
        deployment."""

        log.debug('Deleting deployment: %s - %s', service_name,
            deployment_slot_or_name)
        if deployment_slot_or_name in ('staging','production'):
            req = RequestWithMethod('DELETE', '%s/%s/deploymentslots/%s'
                % (self.base_url, service_name, deployment_slot_or_name))
        else:
            req = RequestWithMethod('DELETE', '%s/%s/deployments/%s'
                % (self.base_url, service_name, deployment_slot_or_name))
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.ACCEPTED:
            self._raise_wa_error(res)
        request_id = res.headers.getheader('x-ms-request-id')
        log.debug('Request-Id: %s', request_id)
        return request_id

    def change_deployment_configuration(self, service_name,
            deployment_slot_or_name, configuration,
            treat_warnings_as_error=False, mode='Auto'):
        """The Change Deployment Configuration (async-)operation initiates a
        change to the deployment configuration.
        
        The Change Deployment Configuration operation is an asynchronous
        operation. To determine whether the Management service has finished
        processing the request, call Get Operation Status."""

        log.debug('Changing deployment config: %s - %s',
            service_name, deployment_slot_or_name)
        if mode.lower() not in ('auto','manual'):
            raise ValueError(mode, 'mode must be "Auto" or "Manual"')
        if isinstance(configuration, basestring):
            if os.path.isfile(configuration):
                config = open(configuration).read()
            else:
                config = configuration
        elif isinstance(configuration, file):
            config = configuration.read()
        else:
            config = configuration
        if deployment_slot_or_name.lower() in ('staging','production'):
            req = RequestWithMethod('POST',
                '%s/%s/deploymentslots/%s/?comp=config' % (self.base_url,
                 service_name, deployment_slot_or_name.lower()))
        else:
            req = RequestWithMethod('POST',
                '%s/%s/deployments/%s/?comp=config' % (self.base_url,
                 service_name, deployment_slot_or_name))
        req_body = OrderedDict()      
        req_body['Configuration'] = base64.b64encode(`config`)
        if treat_warnings_as_error:
            req_body['TreatWarningsAsError'] = u'true'
        if mode.lower() == 'manual':
            req_body['Mode'] = u'Manual'
        req_body = OrderedDict([('ChangeConfiguration',req_body)])
        req.add_data(build_wasm_request_body(req_body))
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.ACCEPTED:
            self._raise_wa_error(res)
        request_id = res.headers.getheader('x-ms-request-id')
        log.debug('Request-Id: %s', request_id)
        return request_id

    def update_deployment_status(self, service_name, deployment_slot_or_name,
            status):
        """The Update Deployment Status (async-)operation initiates a change
        in deployment status."""

        if status.lower() == 'running':
            status = 'Running'
        elif status.lower() == 'suspended':
            status = 'Suspended'
        else: # invalid status
            raise ValueError(status, 'status must be "Running" or "Suspended"')
        log.debug('Updating deployment: %s - %s to %s',
            service_name, deployment_slot_or_name, status)
        if deployment_slot_or_name.lower() in ('staging','production'):
            req = RequestWithMethod('POST',
                '%s/%s/deploymentslots/%s/?comp=status' % (self.base_url,
                 service_name, deployment_slot_or_name.lower()))
        else:
            req = RequestWithMethod('POST',
                '%s/%s/deployments/%s/?comp=status' % (self.base_url,
                 service_name, deployment_slot_or_name))
        req_body = OrderedDict()
        req_body['Status'] = status
        req_body = OrderedDict([('UpdateDeploymentStatus',req_body)])
        req.add_data(build_wasm_request_body(req_body))
        res = self.urlopen(req)
        log.debug('HTTP Response: %s %s', res.code, res.msg)
        if res.code != httplib.ACCEPTED:
            self._raise_wa_error(res)
        request_id = res.headers.getheader('x-ms-request-id')
        log.debug('Request-Id: %s', request_id)
        return request_id

    def upgrade_deployment(self):
        pass

    def walk_upgrade_domain(self):
        pass

    def reboot_role_instance(self):
        pass

    def reimage_role_instance(self):
        pass

    def rollback_update_or_upgrade(self):
        pass


class ServiceConfiguration(object):
    
    def __init__(self, cscfg):
        if isinstance(cscfg, basestring):
            if os.path.isfile(cscfg):
                self.cscfg = etree.parse(cscfg).getroot()
            else:
                self.cscfg = etree.fromstring(cscfg)
        else:
            raise ValueError('cscfg must be a basestring')
        self.xmlns = NAMESPACE_SERVICECONFIG

    def __str__(self):
        return etree.tostring(self.cscfg)

    def __repr__(self):
        return etree.tostring(self.cscfg)

    def __iadd__(self, other):
        ins = self.cscfg.find('.//{%s}Instances' % self.xmlns)
        ins.attrib['count'] = unicode(int(ins.attrib['count']) + other)
        return self

    def __isub__(self, other):
        ins = self.cscfg.find('.//{%s}Instances' % self.xmlns)
        ins.attrib['count'] = unicode(int(ins.attrib['count']) - other)
        return self

    def update_setting(self, name, value):
        """Alter the value of a Configuration Setting"""
        settings = self.cscfg.findall('.//{%s}Setting' % self.xmlns)
        for setting in settings:
            if setting.attrib['name'] == name:
                setting.attrib['value'] = unicode(value)
                return True
        return False

    def update_connections(self, data_connection_string):
        """Update well-known connection strings commonly needed in the
        Azure Cloud Service Config"""
        self.update_setting('DataConnectionString', data_connection_string)
        self.update_setting('DiagnosticsConnectionString',
            data_connection_string)
        self.update_setting(
            'Microsoft.WindowsAzure.Plugins.Diagnostics.ConnectionString',
            data_connection_string)

    def update_instances(self, count):
        """Alter the value Instances 'count' attribute"""
        instances = self.cscfg.find('.//{%s}Instances' % self.xmlns)
        instances.attrib['count'] = unicode(count)

