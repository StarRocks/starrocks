#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from contextlib import contextmanager

import mysql.connector

import dbt.exceptions
from dataclasses import dataclass

from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.contracts.connection import Connection
from dbt.events import AdapterLogger
from typing import Optional

logger = AdapterLogger("starrocks")

@dataclass
class StarRocksCredentials(Credentials):
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    charset: Optional[str] = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __post_init__(self):
        # starrocks classifies database and schema as the same thing
        if (
            self.database is not None and
            self.database != self.schema
        ):
            raise dbt.exceptions.RuntimeException(
                f"    schema: {self.schema} \n"
                f"    database: {self.database} \n"
                f"On StarRocks, database must be omitted or have the same value as"
                f" schema."
            )

    @property
    def type(self):
        return 'starrocks'

    @property
    def unique_field(self):
        return self.schema

    def _connection_keys(self):
        """
        Returns an iterator of keys to pretty-print in 'dbt debug'
        """
        return (
            "host",
            "port",
            "database",
            "schema",
            "username",
        )

class StarRocksConnectionManager(SQLConnectionManager):
    TYPE = 'starrocks'

    @classmethod
    def open(cls, connection):
        if connection.state == 'open':
            logger.debug('Connection is already open, skipping open.')
            return connection

        credentials = cls.get_credentials(connection.credentials)
        kwargs = {}

        kwargs["host"] = credentials.host
        kwargs["username"] = credentials.username
        kwargs["password"] = credentials.password
        kwargs["database"] = credentials.schema

        if credentials.port:
            kwargs["port"] = credentials.port

        try:
            connection.handle = mysql.connector.connect(**kwargs)
            connection.state = 'open'
        except mysql.connector.Error:

            try:
                logger.debug("Failed connection without supplying the `database`. "
                             "Trying again with `database` included.")

                # Try again with the database included
                kwargs["database"] = "information_schema"

                connection.handle = mysql.connector.connect(**kwargs)
                connection.state = 'open'
            except mysql.connector.Error as e:

                logger.debug("Got an error when attempting to open a StarRocks "
                             "connection: '{}'"
                             .format(e))

                connection.handle = None
                connection.state = 'fail'

                raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    @classmethod
    def get_credentials(cls, credentials):
        return credentials

    def cancel(self, connection: Connection):
        connection.handle.close()

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except mysql.connector.DatabaseError as e:
            logger.debug('StarRocks error: {}'.format(str(e)))

            try:
                self.rollback_if_open()
            except mysql.connector.Error:
                logger.debug("Failed to release connection!")
                pass

            raise dbt.exceptions.DatabaseException(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            logger.debug("Rolling back transaction.")
            self.rollback_if_open()
            if isinstance(e, dbt.exceptions.RuntimeException):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise

            raise dbt.exceptions.RuntimeException(e) from e

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        code = "SUCCESS"
        num_rows = 0

        if cursor is not None and cursor.rowcount is not None:
            num_rows = cursor.rowcount

        # There's no real way to get the status from the mysql-connector-python driver.
        # So just return the default value.
        return AdapterResponse(
            _message="{} {}".format(code, num_rows),
            rows_affected=num_rows,
            code=code
        )
