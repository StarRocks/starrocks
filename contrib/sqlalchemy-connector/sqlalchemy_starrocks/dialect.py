#! /usr/bin/python3
# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy.dialects.mysql.mysqldb import MySQLDialect_mysqldb

class SRDialect(MySQLDialect_mysqldb):

    name = 'sr'

    def __init__(self, *args, **kw):
        super(SRDialect, self).__init__(*args, **kw)
        self.ischema_names = self.ischema_names.copy()
        self.ischema_names['largeint']=BIGINT

    @classmethod
    def dbapi(cls):
        return __import__("MySQLdb")
