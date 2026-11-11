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


from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    cast,
    exc,
    func,
    literal_column,
    select,
    testing,
)
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.assertions import AssertsCompiledSQL, expect_raises_message

from starrocks.sql.dml import (
    AzureBlobStorage,
    AzureDataLakeStorage1,
    AzureDataLakeStorage2,
    Compression,
    CSVFormat,
    FilesSource,
    FilesSourceOptions,
    FilesTarget,
    FilesTargetOptions,
    GoogleCloudStorage,
    InsertFromFiles,
    InsertIntoFiles,
    ParquetFormat,
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
                "'csv.row_delimiter' = '\\n',"
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


class CompileStarrocksSelectFromFilesTableTest(fixtures.TestBase, AssertsCompiledSQL):
    __only_on__ = "starrocks"

    def test_select_from_files_column_expr(self):
        t_files = FilesSource(
            storage=GoogleCloudStorage(
                uri='gs://starrocks/atable.parquet',
                service_account_email='x@y.z',
                service_account_private_key_id='mykey',
                service_account_private_key='some_private_key',
            ),
            format=ParquetFormat(
                compression=Compression.SNAPPY
            ),
        ).table_valued(literal_column("$1"))


        self.assert_compile(
            select(
                cast(t_files.c['$1'], String).label('Col1'),
                func.IF(t_files.c['$1'] == "xyz", "NULL", t_files.c['$1']).label('Col2'),
            ),
            (
                "SELECT CAST(anon_1.$1 AS CHAR) AS `Col1`, IF(anon_1.$1 = %(1_1)s, %(IF_1)s, anon_1.$1) AS `Col2`"
                " FROM FILES("
                "'path' = 'gs://starrocks/atable.parquet',"
                "'gcp.gcs.service_account_email' = 'x@y.z',"
                "'gcp.gcs.service_account_private_key_id' = 'mykey',"
                "'gcp.gcs.service_account_private_key' = 'some_private_key',"
                "'format' = 'parquet',"
                "'compression' = 'snappy'"
                ") AS anon_1"
            ),
            checkparams={"1_1": "xyz", "IF_1": "NULL"},
        )


BLOB_PATH = "wasbs://box@acct.blob.core.windows.net/data/"
ADLS1_PATH = "adl://acct.azuredatalakestore.net/data/"
ADLS2_PATH = "abfss://box@acct.dfs.core.windows.net/data/"
SAS_TOKEN = "sv=2022-11-02&ss=b&srt=co&sp=rwl&se=2026-12-31T00%3A00%3A00Z&sig=c2ln%2Bc2ln%3D"
SAS_TOKEN_DOUBLED_FOR_PYFORMAT = "sv=2022-11-02&ss=b&srt=co&sp=rwl&se=2026-12-31T00%%3A00%%3A00Z&sig=c2ln%%2Bc2ln%%3D"


class CompileStarrocksAzureStorageTest(fixtures.TestBase, AssertsCompiledSQL):
    """The Azure storage clauses use the path schemes, property prefixes and property names that StarRocks' FE
    reads: CloudConfigurationConstants, AzureCloudConfigurationProvider and HdfsFsManager."""

    __only_on__ = "starrocks"

    def _assert_properties(self, storage, *properties):
        self.assert_compile(
            FilesSource(storage=storage, format=ParquetFormat()),
            "FILES(" + ",".join([f"'path' = '{storage.options['path']}'", *properties, "'format' = 'parquet'"]) + ")",
        )

    @testing.combinations(
        (AzureBlobStorage, "wasb://box@acct.blob.core.windows.net/data/"),
        (AzureBlobStorage, "wasbs://box@acct.blob.core.windows.net/data/"),
        (AzureDataLakeStorage1, "adl://acct.azuredatalakestore.net/data/"),
        (AzureDataLakeStorage2, "abfs://box@acct.dfs.core.windows.net/data/"),
        (AzureDataLakeStorage2, "abfss://box@acct.dfs.core.windows.net/data/"),
        argnames="storage_cls, uri",
    )
    def test_accepts_path_scheme(self, storage_cls, uri):
        self._assert_properties(storage_cls(uri=uri))

    @testing.combinations(
        (AzureBlobStorage, "azure://box@acct.blob.core.windows.net/data/", "wasb:// or wasbs://", "azure"),
        (AzureBlobStorage, "abfss://box@acct.dfs.core.windows.net/data/", "wasb:// or wasbs://", "abfss"),
        (AzureBlobStorage, "https://acct.blob.core.windows.net/box/data/", "wasb:// or wasbs://", "https"),
        (AzureDataLakeStorage1, "wasb://box@acct.blob.core.windows.net/data/", "adl://", "wasb"),
        (AzureDataLakeStorage1, "abfss://box@acct.dfs.core.windows.net/data/", "adl://", "abfss"),
        (AzureDataLakeStorage2, "wasb://box@acct.blob.core.windows.net/data/", "abfs:// or abfss://", "wasb"),
        (AzureDataLakeStorage2, "adl://acct.azuredatalakestore.net/data/", "abfs:// or abfss://", "adl"),
        (AzureDataLakeStorage2, "/data/", "abfs:// or abfss://", ""),
        argnames="storage_cls, uri, accepted, given",
    )
    def test_rejects_path_scheme(self, storage_cls, uri, accepted, given):
        with expect_raises_message(ValueError, f"expected {accepted}, got scheme '{given}'"):
            storage_cls(uri=uri)

    def test_insert_into_files_on_blob_with_shared_key(self):
        tbl = Table("atable", MetaData(), Column("id", Integer), schema="test_schema")
        insert_into_files = InsertIntoFiles(
            target=FilesTarget(
                storage=AzureBlobStorage(uri=BLOB_PATH, shared_key="c2hhcmVk+a2V5/=="),
                format=ParquetFormat(),
                options=FilesTargetOptions(single=True),
            ),
            from_=tbl.select(),
        )

        self.assert_compile(
            insert_into_files,
            (
                "INSERT INTO FILES("
                f"'path' = '{BLOB_PATH}',"
                "'azure.blob.shared_key' = 'c2hhcmVk+a2V5/==',"
                "'format' = 'parquet',"
                "'single' = 'true'"
                ")"
                " SELECT test_schema.atable.id FROM test_schema.atable"
            ),
        )

    def test_insert_from_files_on_blob_with_sas_token_doubles_percent_signs(self):
        tbl = Table("atable", MetaData(), Column("id", Integer), schema="test_schema")
        insert_from_files = InsertFromFiles(
            target=tbl,
            from_=FilesSource(
                storage=AzureBlobStorage(uri=BLOB_PATH, sas_token=SAS_TOKEN),
                format=ParquetFormat(),
            ),
        )

        self.assert_compile(
            insert_from_files,
            (
                "INSERT INTO test_schema.atable "
                "SELECT * FROM FILES("
                f"'path' = '{BLOB_PATH}',"
                f"'azure.blob.sas_token' = '{SAS_TOKEN_DOUBLED_FOR_PYFORMAT}',"
                "'format' = 'parquet'"
                ")"
            ),
        )

    def test_blob_managed_identity(self):
        self._assert_properties(
            AzureBlobStorage(uri=BLOB_PATH, oauth2_use_managed_identity=True, oauth2_client_id="cid"),
            "'azure.blob.oauth2_use_managed_identity' = 'true'",
            "'azure.blob.oauth2_client_id' = 'cid'",
        )

    def test_blob_service_principal(self):
        self._assert_properties(
            AzureBlobStorage(
                uri=BLOB_PATH, oauth2_client_id="cid", oauth2_client_secret="secret", oauth2_tenant_id="tid"
            ),
            "'azure.blob.oauth2_client_id' = 'cid'",
            "'azure.blob.oauth2_client_secret' = 'secret'",
            "'azure.blob.oauth2_tenant_id' = 'tid'",
        )

    def test_blob_endpoint_storage_account_and_container(self):
        self._assert_properties(
            AzureBlobStorage(
                uri=BLOB_PATH,
                shared_key="key",
                endpoint="https://acct.blob.core.windows.net",
                storage_account="acct",
                container="box",
            ),
            "'azure.blob.shared_key' = 'key'",
            "'azure.blob.endpoint' = 'https://acct.blob.core.windows.net'",
            "'azure.blob.storage_account' = 'acct'",
            "'azure.blob.container' = 'box'",
        )

    def test_adls1_managed_service_identity(self):
        self._assert_properties(
            AzureDataLakeStorage1(uri=ADLS1_PATH, use_managed_service_identity=True),
            "'azure.adls1.use_managed_service_identity' = 'true'",
        )

    def test_adls1_service_principal(self):
        self._assert_properties(
            AzureDataLakeStorage1(
                uri=ADLS1_PATH,
                oauth2_client_id="cid",
                oauth2_credential="secret",
                oauth2_endpoint="https://login.microsoftonline.com/tid/oauth2/token",
            ),
            "'azure.adls1.oauth2_client_id' = 'cid'",
            "'azure.adls1.oauth2_credential' = 'secret'",
            "'azure.adls1.oauth2_endpoint' = 'https://login.microsoftonline.com/tid/oauth2/token'",
        )

    def test_adls1_old_argument_names_write_the_properties_starrocks_reads(self):
        self._assert_properties(
            AzureDataLakeStorage1(
                uri=ADLS1_PATH,
                oauth2_client_id="cid",
                oauth2_client_credential="secret",
                oauth2_client_endpoint="https://login.microsoftonline.com/tid/oauth2/token",
            ),
            "'azure.adls1.oauth2_client_id' = 'cid'",
            "'azure.adls1.oauth2_credential' = 'secret'",
            "'azure.adls1.oauth2_endpoint' = 'https://login.microsoftonline.com/tid/oauth2/token'",
        )

    @testing.combinations(
        (dict(oauth2_credential="a", oauth2_client_credential="a"), "oauth2_credential or its old name"),
        (dict(oauth2_endpoint="a", oauth2_client_endpoint="a"), "oauth2_endpoint or its old name"),
        argnames="kwargs, message",
    )
    def test_adls1_new_and_old_name_together_raise(self, kwargs, message):
        with expect_raises_message(TypeError, message):
            AzureDataLakeStorage1(uri=ADLS1_PATH, **kwargs)

    def test_adls2_managed_identity(self):
        self._assert_properties(
            AzureDataLakeStorage2(
                uri=ADLS2_PATH, oauth2_use_managed_identity=True, oauth2_tenant_id="tid", oauth2_client_id="cid"
            ),
            "'azure.adls2.oauth2_use_managed_identity' = 'true'",
            "'azure.adls2.oauth2_tenant_id' = 'tid'",
            "'azure.adls2.oauth2_client_id' = 'cid'",
        )

    def test_adls2_shared_key(self):
        self._assert_properties(
            AzureDataLakeStorage2(uri=ADLS2_PATH, shared_key="key", storage_account="acct"),
            "'azure.adls2.shared_key' = 'key'",
            "'azure.adls2.storage_account' = 'acct'",
        )

    def test_adls2_sas_token(self):
        self._assert_properties(
            AzureDataLakeStorage2(uri=ADLS2_PATH, sas_token=SAS_TOKEN),
            f"'azure.adls2.sas_token' = '{SAS_TOKEN_DOUBLED_FOR_PYFORMAT}'",
        )

    def test_adls2_service_principal(self):
        self._assert_properties(
            AzureDataLakeStorage2(
                uri=ADLS2_PATH,
                oauth2_client_id="cid",
                oauth2_client_secret="secret",
                oauth2_client_endpoint="https://login.microsoftonline.com/tid/oauth2/token",
            ),
            "'azure.adls2.oauth2_client_id' = 'cid'",
            "'azure.adls2.oauth2_client_secret' = 'secret'",
            "'azure.adls2.oauth2_client_endpoint' = 'https://login.microsoftonline.com/tid/oauth2/token'",
        )

    def test_adls2_workload_identity(self):
        self._assert_properties(
            AzureDataLakeStorage2(
                uri=ADLS2_PATH,
                oauth2_tenant_id="tid",
                oauth2_client_id="cid",
                oauth2_token_file="/var/run/secrets/azure/tokens/azure-identity-token",
            ),
            "'azure.adls2.oauth2_tenant_id' = 'tid'",
            "'azure.adls2.oauth2_client_id' = 'cid'",
            "'azure.adls2.oauth2_token_file' = '/var/run/secrets/azure/tokens/azure-identity-token'",
        )

    def test_adls2_endpoint(self):
        self._assert_properties(
            AzureDataLakeStorage2(uri=ADLS2_PATH, shared_key="key", endpoint="https://acct.dfs.core.windows.net"),
            "'azure.adls2.shared_key' = 'key'",
            "'azure.adls2.endpoint' = 'https://acct.dfs.core.windows.net'",
        )


class CompileStarrocksFilesTargetOptionsTest(fixtures.TestBase, AssertsCompiledSQL):
    """FilesTargetOptions writes the unload properties StarRocks reads, as strings, also under their old names."""

    __only_on__ = "starrocks"

    def _assert_unload_options(self, options, *properties):
        tbl = Table("atable", MetaData(), Column("id", Integer), schema="test_schema")
        insert_into_files = InsertIntoFiles(
            target=FilesTarget(
                storage=GoogleCloudStorage(uri="gs://starrocks/atable/"),
                format=ParquetFormat(),
                options=options,
            ),
            from_=tbl.select(),
        )
        self.assert_compile(
            insert_into_files,
            "INSERT INTO FILES("
            + ",".join(["'path' = 'gs://starrocks/atable/'", "'format' = 'parquet'", *properties])
            + ") SELECT test_schema.atable.id FROM test_schema.atable",
        )

    def test_target_max_file_size_and_partition_by(self):
        self._assert_unload_options(
            FilesTargetOptions(target_max_file_size=1073741824, partition_by="id"),
            "'target_max_file_size' = '1073741824'",
            "'partition_by' = 'id'",
        )

    def test_old_names_write_the_properties_starrocks_reads(self):
        self._assert_unload_options(
            FilesTargetOptions(target_max_files_size=1073741824, partitioned_by="id"),
            "'target_max_file_size' = '1073741824'",
            "'partition_by' = 'id'",
        )

    @testing.combinations((True, "true"), (False, "false"), ("false", "false"), argnames="single, written")
    def test_single(self, single, written):
        self._assert_unload_options(FilesTargetOptions(single=single), f"'single' = '{written}'")

    def test_empty_options_leave_no_trailing_comma(self):
        self._assert_unload_options(FilesTargetOptions())

    @testing.combinations(
        (dict(target_max_file_size=1, target_max_files_size=1), "target_max_file_size or its old name"),
        (dict(partition_by="id", partitioned_by="id"), "partition_by or its old name"),
        argnames="kwargs, message",
    )
    def test_new_and_old_name_together_raise(self, kwargs, message):
        with expect_raises_message(TypeError, message):
            FilesTargetOptions(**kwargs)


class CompileStarrocksFilesFormatTest(fixtures.TestBase, AssertsCompiledSQL):
    """The format options use the property names StarRocks reads, and every value is written as a string."""

    __only_on__ = "starrocks"

    def _assert_files(self, file_format, *properties, options=None):
        self.assert_compile(
            FilesSource(storage=GoogleCloudStorage(uri="gs://starrocks/atable/"), format=file_format, options=options),
            "FILES(" + ",".join(["'path' = 'gs://starrocks/atable/'", *properties]) + ")",
        )

    def test_csv_unload_header_and_row_delimiter(self):
        self._assert_files(
            CSVFormat(column_separator="|", row_delimiter="\r\n", include_header=True),
            "'format' = 'csv'",
            "'csv.row_delimiter' = '\\r\\n'",
            "'csv.column_separator' = '|'",
            "'csv.include_header' = 'true'",
        )

    def test_line_delimiter_writes_row_delimiter(self):
        self._assert_files(CSVFormat(line_delimiter="\n"), "'format' = 'csv'", "'csv.row_delimiter' = '\\n'")

    def test_row_delimiter_and_line_delimiter_together_raise(self):
        with expect_raises_message(TypeError, "row_delimiter or its old name line_delimiter"):
            CSVFormat(row_delimiter="\n", line_delimiter="\n")

    @testing.combinations((True, "true"), (False, "false"), argnames="trim_space, written")
    def test_csv_trim_space_is_written_as_a_string(self, trim_space, written):
        self._assert_files(CSVFormat(trim_space=trim_space), "'format' = 'csv'", f"'csv.trim_space' = '{written}'")

    def test_parquet_use_legacy_encoding_and_version(self):
        self._assert_files(
            ParquetFormat(use_legacy_encoding=True, version="2.6"),
            "'format' = 'parquet'",
            "'parquet.use_legacy_encoding' = 'true'",
            "'parquet.version' = '2.6'",
        )

    def test_non_string_option_is_written_as_a_string(self):
        self._assert_files(
            ParquetFormat(),
            "'format' = 'parquet'",
            "'auto_detect_sample_files' = '2'",
            options=FilesSourceOptions(auto_detect_sample_files=2),
        )

    def test_option_without_a_value_raises(self):
        files = FilesSource(
            storage=GoogleCloudStorage(uri="gs://starrocks/atable/"),
            format=ParquetFormat(),
            options=FilesSourceOptions(strict_mode=None),
        )
        with expect_raises_message(exc.CompileError, "property 'strict_mode' has no value"):
            files.compile(dialect=testing.db.dialect)

    @testing.combinations((b"gzip", "bytes"), (Compression.GZIP, "Compression"), argnames="value, type_name")
    def test_option_of_another_type_raises(self, value, type_name):
        files = FilesSource(
            storage=GoogleCloudStorage(uri="gs://starrocks/atable/"),
            format=ParquetFormat(),
            options=FilesSourceOptions(compression=value),
        )
        with expect_raises_message(exc.CompileError, f"property 'compression' is a {type_name}"):
            files.compile(dialect=testing.db.dialect)


UNRESOLVABLE_ACCOUNT = "nosuchaccount0starrocks"
BLOB_UNLOAD_PATH = f"wasbs://box@{UNRESOLVABLE_ACCOUNT}.blob.core.windows.net/unload/"


class FilesUnloadPlanTest(fixtures.TestBase):
    """StarRocks plans each INSERT INTO FILES statement the clauses compile, so its parser and analyser accept the
    properties. Planning reads no storage, and the account names don't resolve, so the fake credentials go nowhere.
    """

    __backend__ = True
    __only_on__ = "starrocks"

    @testing.combinations(
        (
            "blob_shared_key",
            AzureBlobStorage(uri=BLOB_UNLOAD_PATH, shared_key="c2hhcmVka2V5"),
            ParquetFormat(),
            FilesTargetOptions(single=True),
        ),
        ("blob_sas_token", AzureBlobStorage(uri=BLOB_UNLOAD_PATH, sas_token=SAS_TOKEN), ParquetFormat(), None),
        (
            "adls2_shared_key",
            AzureDataLakeStorage2(
                uri=f"abfss://box@{UNRESOLVABLE_ACCOUNT}.dfs.core.windows.net/unload/",
                shared_key="c2hhcmVka2V5",
                storage_account=UNRESOLVABLE_ACCOUNT,
            ),
            ParquetFormat(),
            None,
        ),
        (
            "adls1_service_principal",
            AzureDataLakeStorage1(
                uri=f"adl://{UNRESOLVABLE_ACCOUNT}.azuredatalakestore.net/unload/",
                oauth2_client_id="cid",
                oauth2_credential="secret",
                oauth2_endpoint="http://127.0.0.1:9/oauth2/token",
            ),
            ParquetFormat(),
            None,
        ),
        (
            "csv_options",
            AzureBlobStorage(uri=BLOB_UNLOAD_PATH, shared_key="c2hhcmVka2V5"),
            CSVFormat(column_separator="|", line_delimiter="\r\n", include_header=True, enclose='"', escape="\\"),
            FilesTargetOptions(single=True),
        ),
        (
            "parquet_options",
            AzureBlobStorage(uri=BLOB_UNLOAD_PATH, shared_key="c2hhcmVka2V5"),
            ParquetFormat(use_legacy_encoding=True, version="2.6", compression=Compression.ZSTD),
            FilesTargetOptions(target_max_file_size=1048576),
        ),
        (
            "partition_by",
            AzureBlobStorage(uri=BLOB_UNLOAD_PATH, shared_key="c2hhcmVka2V5"),
            ParquetFormat(),
            FilesTargetOptions(partition_by="region"),
        ),
        (
            "empty_options",
            AzureBlobStorage(uri=BLOB_UNLOAD_PATH, shared_key="c2hhcmVka2V5"),
            ParquetFormat(),
            FilesTargetOptions(),
        ),
        id_="iaaa",
        argnames="storage, file_format, options",
    )
    def test_starrocks_plans_the_unload(self, connection, storage, file_format, options):
        unload = InsertIntoFiles(
            target=FilesTarget(storage=storage, format=file_format, options=options),
            from_=select(literal_column("1").label("id"), literal_column("'emea'").label("region")),
        )
        plan = connection.exec_driver_sql("EXPLAIN " + str(unload.compile(dialect=connection.dialect))).fetchall()
        assert any("TABLE FUNCTION TABLE SINK" in row[0] for row in plan)
