#!/usr/bin/env python
# encoding: utf-8

import sys
sys.path.append('/path/to/pyazure')
from pyazure import pyazure

ACCOUNT_NAME = '<account_name>'
ACCOUNT_KEY = '<account_key>'

pa = pyazure.PyAzure(ACCOUNT_NAME, ACCOUNT_KEY, False)

tables = pa.tables.list_tables()
for t in tables:
    print(e.name)

