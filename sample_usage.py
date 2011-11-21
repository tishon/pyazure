#!/usr/bin/env python
# encoding: utf-8

import sys, os
sys.path.append('/path/to/pyazure')
from pyazure import pyazure

ACCOUNT_NAME = '<Azure_storage_account_name>'
ACCOUNT_KEY = '<Azure_storage_account_key>'

pa = pyazure.PyAzure(ACCOUNT_NAME, ACCOUNT_KEY)

tables = pa.tables.list_tables()
for t in tables:
    print(e.name)


MANAGEMENT_CERT = \
    '<path to Azure management certificate (PEM with embedded key)>'
SUBSCRIPTION_ID = '<Azure subscription ID>'

print 'Initialising Azure management interface'
pa = pyazure.PyAzure(management_cert_path=MANAGEMENT_CERT,
    subscription_id=SUBSCRIPTION_ID)

print 'Listing existing hosted services...'
for svc in pa.wasm.list_services():
    print svc

print 'Listing existing storage accounts...'
for store in pa.wasm.list_storage_accounts():
    print store

print "Switching pyazure to 'pyazuretest' storage account..."
pa.set_storage_account('pyazuretest', create=True)

print 'Uploading azure cspkg...'
cspkg_file = open('/path/to/MyAzureService.cspkg', 'rb')
status = self.azure.blobs.create_container('my-azure-service')
print '...create container status: %s' % status
status = self.azure.blobs.put_blob('my-azure-service', 'MyAzureService.cspkg',
    cspkg_file.read())
print '...put blob status: %s' % status
cspkg_url = pa.blobs.get_base_url() + '/my-azure-service/MyAzureService.cspkg'

print "Creating hosted service 'myservice1'"
pa.wasm.create_service('myservice1', 'This is version 1 of my-service',
    'anywhere us')

print 'Loading hosted service configuration from file'
cscfg = pa.wasm.ServiceConfiguration('/path/to/MyAzureService.cscfg')

print "Editing hosted service configuration to update connection strings for the 'pyazuretest' storage account"
cscfg.update_connections(pa.data_connection_string)

print "Editing hosted service configuration to update service settings"
cscfg.update_setting('WORKER_CONCURRENCY', 4)
cscfg.update_setting('WORKER_CLOUD_STORE', 'my-azure-service')

print 'Deploying to myservice1...'
request = pa.wasm.create_deployment('myservice1', 'staging', 'mydeployment1',
    cspkg_url, 'Staging deployment of my-service v1', cscfg,
    start_deployment=True)
pa.wasm.wait_for_request(request)

print 'Retrieving hosted service properties...'
s = pa.wasm.get_service_properties('myservice1', embed_detail=True)
print '%d roles instances in deployment' % \
    len(s['Deployments'][0]['RoleInstanceList'])

print 'Updating deployment, increasing instances...'
cscfg += 1
request = pa.wasm.change_deployment_configuration('myservice1',
    'mydeployment1', cscfg)
pa.wasm.wait_for_request(request)

print "Tearing down 'myservice1'"
pa.wasm.wait_for_request(pa.wasm.delete_deployment('myservice1',
    'mydeployment1'))
pa.wasm.delete_service('myservice1')
pa.wasm.delete_storage_account('pyazuretest')
