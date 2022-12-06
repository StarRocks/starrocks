#! /usr/bin/python3
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
from sqlalchemy.dialects.mysql import DECIMAL
from sqlalchemy.dialects.mysql.mysqldb import MySQLDialect_mysqldb

class StarRocksDialect(MySQLDialect_mysqldb):

    name = 'starrocks'

    def __init__(self, *args, **kw):
        super(StarRocksDialect, self).__init__(*args, **kw)
        self.ischema_names = self.ischema_names.copy()
        self.ischema_names['largeint'] = DECIMAL

    @classmethod
    def dbapi(cls):
        return __import__("MySQLdb")
