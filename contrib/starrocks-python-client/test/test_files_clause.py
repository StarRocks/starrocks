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


from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL
from sqlalchemy import (
    Table,
    Column,
    Integer,
    func,
    MetaData,
    literal_column,
)

from starrocks import (
    InsertIntoFiles,
    FilesTarget,
    FilesTargetOptions,
    InsertFromFiles,
    FilesSource,
    # FilesSourceOptions,
    CSVFormat,
    ParquetFormat,
    GoogleCloudStorage,
    Compression,
)


class CompileStarrocksInsertIntoFilesTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = "starrocks"

    def test_insert_into_files(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            schema="test_schema",
        )
        insert_into_files = InsertIntoFiles(
            target=FilesTarget(
                storage=GoogleCloudStorage(
                    uri='gs://starrocks/atable',
                    service_account_email='x@y.z',
                    service_account_private_key_id='mykey',
                    service_account_private_key='some_private_key',
                ),
                format=CSVFormat(
                    column_separator=',',
                    line_delimiter='\n',
                    enclose='"',
                    skip_header=1,
                ),
                options=FilesTargetOptions(
                    single=True,
                )
            ),
            from_=tbl.select(),
        )

        self.assert_compile(
            insert_into_files,
            (
                "INSERT INTO FILES("
                "'path' = 'gs://starrocks/atable',"
                "'gcp.gcs.service_account_email' = 'x@y.z',"
                "'gcp.gcs.service_account_private_key_id' = 'mykey',"
                "'gcp.gcs.service_account_private_key' = 'some_private_key',"
                "'format' = 'csv',"
                "'csv.line_delimiter' = '\\n',"
                "'csv.column_separator' = ',',"
                "'csv.enclose' = '\"',"
                "'csv.skip_header' = '1',"
                "'single' = 'true'"
                ")"
                " SELECT test_schema.atable.id FROM test_schema.atable"
            ),
        )


class CompileStarrocksInsertFromFilesTest(fixtures.TestBase, AssertsCompiledSQL):

    __only_on__ = "starrocks"

    def test_insert_from_files_csv(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            schema="test_schema",
        )
        insert_from_files = InsertFromFiles(
            target=tbl,
            from_=FilesSource(
                storage=GoogleCloudStorage(
                    uri='gs://starrocks/atable',
                    service_account_email='x@y.z',
                    service_account_private_key_id='mykey',
                    service_account_private_key='some_private_key',
                ),
                format=CSVFormat(
                    column_separator=',',
                    row_delimiter='\n',
                    enclose='"',
                ),
                # options=FilesSourceOptions(
                # )
            ),
        )

        self.assert_compile(
            insert_from_files,
            (
                "INSERT INTO test_schema.atable "
                "SELECT * FROM FILES("
                "'path' = 'gs://starrocks/atable',"
                "'gcp.gcs.service_account_email' = 'x@y.z',"
                "'gcp.gcs.service_account_private_key_id' = 'mykey',"
                "'gcp.gcs.service_account_private_key' = 'some_private_key',"
                "'format' = 'csv',"
                "'csv.row_delimiter' = '\\n',"
                "'csv.column_separator' = ',',"
                "'csv.enclose' = '\"'"
                ")"
            ),
        )

    def test_insert_from_files_parquet(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            schema="test_schema",
        )
        insert_from_files = InsertFromFiles(
            target=tbl,
            from_=FilesSource(
                storage=GoogleCloudStorage(
                    uri='gs://starrocks/atable.parquet',
                    service_account_email='x@y.z',
                    service_account_private_key_id='mykey',
                    service_account_private_key='some_private_key',
                ),
                format=ParquetFormat(
                    compression=Compression.SNAPPY
                ),
            ),
        )

        self.assert_compile(
            insert_from_files,
            (
                "INSERT INTO test_schema.atable "
                "SELECT * FROM FILES("
                "'path' = 'gs://starrocks/atable.parquet',"
                "'gcp.gcs.service_account_email' = 'x@y.z',"
                "'gcp.gcs.service_account_private_key_id' = 'mykey',"
                "'gcp.gcs.service_account_private_key' = 'some_private_key',"
                "'format' = 'parquet',"
                "'compression' = 'snappy'"
                ")"
            ),
        )

    def test_insert_from_files_column_str(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            schema="test_schema",
        )
        insert_from_files = InsertFromFiles(
            target=tbl,
            from_=FilesSource(
                storage=GoogleCloudStorage(
                    uri='gs://starrocks/atable.parquet',
                    service_account_email='x@y.z',
                    service_account_private_key_id='mykey',
                    service_account_private_key='some_private_key',
                ),
                format=ParquetFormat(
                    compression=Compression.SNAPPY
                ),
            ),
            columns='$1, $2, $3'
        )

        self.assert_compile(
            insert_from_files,
            (
                "INSERT INTO test_schema.atable "
                "SELECT $1, $2, $3"
                " FROM FILES("
                "'path' = 'gs://starrocks/atable.parquet',"
                "'gcp.gcs.service_account_email' = 'x@y.z',"
                "'gcp.gcs.service_account_private_key_id' = 'mykey',"
                "'gcp.gcs.service_account_private_key' = 'some_private_key',"
                "'format' = 'parquet',"
                "'compression' = 'snappy'"
                ")"
            ),
        )

    def test_insert_from_files_column_expr(self):
        m = MetaData()
        tbl = Table(
            "atable",
            m,
            Column("id", Integer),
            schema="test_schema",
        )
        insert_from_files = InsertFromFiles(
            target=tbl,
            from_=FilesSource(
                storage=GoogleCloudStorage(
                    uri='gs://starrocks/atable.parquet',
                    service_account_email='x@y.z',
                    service_account_private_key_id='mykey',
                    service_account_private_key='some_private_key',
                ),
                format=ParquetFormat(
                    compression=Compression.SNAPPY
                ),
            ),
            columns=[func.IF(literal_column("$1") == "xyz", "NULL", "NOTNULL")]
        )

        self.assert_compile(
            insert_from_files,
            (
                "INSERT INTO test_schema.atable "
                "SELECT IF($1 = %(1_1)s, %(IF_1)s, %(IF_2)s)"
                " FROM FILES("
                "'path' = 'gs://starrocks/atable.parquet',"
                "'gcp.gcs.service_account_email' = 'x@y.z',"
                "'gcp.gcs.service_account_private_key_id' = 'mykey',"
                "'gcp.gcs.service_account_private_key' = 'some_private_key',"
                "'format' = 'parquet',"
                "'compression' = 'snappy'"
                ")"
            ),
            checkparams={"1_1": "xyz", "IF_1": "NULL", "IF_2": "NOTNULL"},
        )

