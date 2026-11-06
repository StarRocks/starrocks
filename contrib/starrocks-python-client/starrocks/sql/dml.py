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

from enum import Enum
from typing import Any
from urllib.parse import urlparse

from sqlalchemy.schema import Table
from sqlalchemy.sql import ClauseElement, TableClause
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.functions import FunctionElement


class FilesClause(ClauseElement):
    """FILES Base Class

    This class represents the base class for a FILES <https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/files/> table function in Starrocks

    Usage is through descending classes FilesSource and FilesTarget.

    """
    __visit_name__ = 'files'

    def __init__(self, format: "FilesFormat", options: "_FilesOptions" = None):
        super().__init__()
        self.format = format
        self.options = options

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in base.py
        """

        return f"FILES(\n{repr(self.format)}\n{repr(self.options)}\n)"


class _FilesOptions(ClauseElement):
    __visit_name__ = "files_options"

    def __init__(self, **kwargs):
        self.options = dict()
        self.options.update(kwargs)  # Allow use of any other parameters

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in dialect.py
        """
        return "\n".join([f"{k} = {v}" for k, v in self.options.items()])


class InsertIntoFiles(UpdateBase):
    inherit_cache = False
    __visit_name__ = "insert_into_files"
    _bind = None

    def __init__(
        self,
        *,
        target: "FilesTarget",
        from_,
    ):
        self.target = target
        self.from_ = from_

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in dialect.py
        """
        return f"INSERT INTO {repr(self.target)} FROM {repr(self.from_)}"

    def bind(self):
        return None


class InsertFromFiles(UpdateBase):
    inherit_cache = False
    __visit_name__ = "insert_from_files"
    _bind = None

    def __init__(
        self,
        *,
        target: Table|TableClause,
        from_: "FilesSource",
        columns: str|list = '*',
    ):
        # columns need to be a list of expressions of column index, e.g. ["$1", "IF($1 =='t', True, False)"]
        # or a string of these expressions that we just use
        self.target = target
        self.from_ = from_
        self.columns = columns

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in dialect.py
        """
        return (
            f"INSERT INTO {self.target}"
            f" SELECT {','.join(self.columns) if isinstance(self.columns, list) else self.columns}"
            f" FROM {repr(self.from_)}"
        )

    def bind(self):
        return None


class FilesTarget(FilesClause):
    """Represents a FILES table function in Starrocks when used for unloading data

    Example Usage, used with InsertIntoFiles class:

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

    """

    __visit_name__ = 'files_target'

    def __init__(
        self,
        *,
        storage: "_StorageClause",
        format: "FilesFormat",
        options: "FilesTargetOptions" = None,
    ):
        super().__init__(format, options)
        self.storage = storage

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in dialect.py
        """

        return f"FILES(\n{repr(self.storage)}\n{repr(self.format)}\n{repr(self.options)}\n)"

def _argument_or_its_old_name(name: str, value: Any, old_name: str, old_value: Any) -> Any:
    """Return the argument passed as ``name`` or under its old name ``old_name``, refusing both at once."""
    if value is not None and old_value is not None:
        raise TypeError(f"Pass {name} or its old name {old_name}, not both")
    return old_value if value is None else value


class FilesTargetOptions(_FilesOptions):
    """Unload options of a FILES() table function, for INSERT INTO FILES.

    ``single`` writes one file, ``target_max_file_size`` caps each file at a best-effort number of bytes, and
    ``partition_by`` names the comma-separated columns whose values partition the files into directories.
    ``target_max_files_size`` and ``partitioned_by`` are their old names: earlier releases wrote those names into
    the statement, where StarRocks ignored them.
    """

    def __init__(
        self,
        *,
        single: bool | None = None,
        target_max_file_size: int | None = None,
        partition_by: str | None = None,
        target_max_files_size: int | None = None,
        partitioned_by: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        target_max_file_size = _argument_or_its_old_name(
            "target_max_file_size", target_max_file_size, "target_max_files_size", target_max_files_size
        )
        partition_by = _argument_or_its_old_name("partition_by", partition_by, "partitioned_by", partitioned_by)
        if single is not None:
            self.options["single"] = single
        if target_max_file_size is not None:
            self.options["target_max_file_size"] = target_max_file_size
        if partition_by is not None:
            self.options["partition_by"] = partition_by


class FilesSource(FilesClause, FunctionElement):
    """Represents a FILES table function in Starrocks when used for loading data

    Example Usage, used with InsertFromFiles class::

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

    Example Usage, selecting with table_valued method::

        m = MetaData()
        tbl_files = FilesSource(
            storage=GoogleCloudStorage(
                uri='gs://starrocks/atable.parquet',
                service_account_email='x@y.z',
                service_account_private_key_id='mykey',
                service_account_private_key='some_private_key',
            ),
            format=ParquetFormat(
                compression=Compression.SNAPPY
            ),
        ).table_valued(*[literal_column(f"${i}") for i in range(10)])
        sel_cols = sqlalchemy.select(
            cast(tbl_files.c['$1'], String).label('Col1'),
            func.IF(tbl_files.c['$2'] == "xyz", "NULL", t_files.c['$2']).label('Col2'),
        )

    """
    __visit_name__ = 'files_source'
    inherit_cache = False

    def __init__(
        self,
        *,
        storage: "_StorageClause",
        format: "FilesFormat",
        options: "FilesSourceOptions" = None,
    ):
        super().__init__(format, options)
        self.storage = storage

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in dialect.py
        """

        return f"FILES(\n{repr(self.storage)}\n{repr(self.format)}\n{repr(self.options)}\n)"

class FilesSourceOptions(_FilesOptions):
    def __init__(
        self,
        *,
        list_files_only: bool = None,
        list_recursively: bool = None,
        columns_from_path: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if isinstance(list_files_only, bool):
            self.options["list_files_only"] = str(list_files_only).lower()
        if isinstance(list_recursively, bool):
            self.options["list_recursively"] = str(list_recursively).lower()
        if columns_from_path is not None:
            self.options["columns_from_path"] = columns_from_path

class Compression(Enum):
    NONE = "uncompressed"
    DEFLATE = "deflate"
    BZIP2 = "bzip2"
    GZIP = "gzip"
    LZ4 = "lz4"
    LZ4_FRAME = "lz4_frame"
    ZSTD = "zstd"
    SNAPPY = "snappy"
    ZLIB = "zlib"

class FilesFormat(ClauseElement):
    """
    Base class for file format specifications inside a FILES statement.
    """

    __visit_name__ = "files_format"
    __format_type__ = None

    def __init__(self, compression: Compression|str = None, **kwargs):
        self.options = dict()
        if self.__format_type__:
            self.options['format'] = self.__format_type__
        if compression:
            if isinstance(compression, Compression):
                self.options["compression"] = compression.value
            else:
                self.options["compression"] = compression

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the respective visitor in the dialect
        """
        return f"{self.options}"

    def add_option(self, name, value):
        if self.__format_type__ is None:
            raise Exception(f'Format type is not set for format {self.__class__}')
        self.options[f'{self.__format_type__}.{name}'] = value


class CSVFormat(FilesFormat):
    """CSV format of a FILES() table function.

    Loading reads ``column_separator``, ``row_delimiter``, ``enclose``, ``escape``, ``skip_header`` and
    ``trim_space``; unloading reads ``column_separator``, ``row_delimiter``, ``include_header``, ``enclose`` and
    ``escape``. ``line_delimiter`` is an old name for ``row_delimiter``, which StarRocks reads for both.
    """
    __format_type__ = "csv"

    def __init__(
        self,
        *,
        column_separator: str | None = None,
        row_delimiter: str | None = None,
        line_delimiter: str | None = None,
        enclose: str | None = None,
        escape: str | None = None,
        skip_header: int | None = None,
        trim_space: bool | None = None,
        include_header: bool | None = None,
        compression: Compression | str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(compression, **kwargs)
        row_delimiter = _argument_or_its_old_name("row_delimiter", row_delimiter, "line_delimiter", line_delimiter)
        if row_delimiter:
            if (
                len(str(row_delimiter).encode().decode("unicode_escape")) != 1
                and row_delimiter != "\r\n"
            ):
                raise TypeError("Record Delimiter should be a single character.")
            self.add_option("row_delimiter", row_delimiter)
        if column_separator:
            if len(str(column_separator).encode().decode("unicode_escape")) != 1:
                raise TypeError("Column Separator should be a single character")
            self.add_option("column_separator", column_separator)
        if enclose:
            if enclose not in ["'", '"', "`"]:
                raise TypeError("Enclose character must be one of [', \", `].")
            self.add_option("enclose", enclose)
        if escape:
            if escape not in ["\\", ""]:
                raise TypeError('Escape character must be "\\" or "".')
            self.add_option("escape", escape)
        if skip_header is not None:
            if skip_header < 0:
                raise TypeError("Skip header must be positive integer.")
            self.add_option("skip_header", str(skip_header))
        if isinstance(trim_space, bool):
            self.add_option("trim_space", trim_space)
        if isinstance(include_header, bool):
            self.add_option("include_header", include_header)


class ParquetFormat(FilesFormat):
    __format_type__ = "parquet"

    def __init__(
        self,
        *,
        use_legacy_encoding: bool = None, # for unloading only
        version: str = None, # for unloading only
        compression: Compression|str = None,
        **kwargs,
    ):
        super().__init__(compression, **kwargs)
        if use_legacy_encoding is not None:
            self.add_option("use_legacy_encoding", use_legacy_encoding)
        if version:
            self.add_option("version", version)

class AVROFormat(FilesFormat):
    __format_type__ = "avro"

    def __init__(
        self, **kwargs,
    ):
        super().__init__(**kwargs)

class ORCFormat(FilesFormat):
    __format_type__ = "orc"

    def __init__(
        self,
        *,
        compression: Compression|str = None,
        **kwargs,
    ):
        super().__init__(compression, **kwargs)

# ToDo - Not yet supported
# class JSONFormat(FilesFormat):
#     format_type = "json"
#
#     def __init__(
#         self,
#         *,
#         jsonpaths,
#         strip_outer_array,
#         json_root: str = None,
#         ignore_json_size: bool = None,
#     ):
#         super().__init__()
#
# class ProtoBufFormat(FilesFormat):
#     format_type = "protobuf"
#
#     def __init__(
#         self,
#     ):
#         super().__init__()
#
# class ThriftFormat(FilesFormat):
#     format_type = "thrift"
#
#     def __init__(
#         self,
#     ):
#         super().__init__()


class _StorageClause(ClauseElement):
    """Base class for the storage properties of a FILES() table function.

    ``__uri_schemes__`` lists the path schemes StarRocks reads the storage system from, and
    ``__option_prefix__`` the prefix of its credential properties. A path with any other scheme raises
    ``ValueError``, naming only the scheme so that a credential carried in the path is not echoed.
    """
    __visit_name__ = "cloud_storage"
    __uri_schemes__: tuple[str, ...] = ()
    __option_prefix__ = ''

    def __init__(
        self,
        *,
        uri: str,
        **kwargs: Any,
    ) -> None:
        self.options = dict()
        scheme = urlparse(uri).scheme
        if scheme not in self.__uri_schemes__:
            accepted = " or ".join(f"{accepted_scheme}://" for accepted_scheme in self.__uri_schemes__)
            raise ValueError(
                f"Invalid path prefix for storage clause {self.__class__.__name__}: "
                f"expected {accepted}, got scheme {scheme!r}"
            )
        self.options['path'] = uri
        self.options.update(kwargs)  # Allow use of any other parameters

    def add_option(self, option_name, value):
        self.options[f'{self.__option_prefix__}.{option_name}'] = value

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in dialect.py
        """
        return ",\n".join([f"{k} = {v}" for k, v in self.options.items()])

class AmazonS3(_StorageClause):
    """Amazon S3"""
    __uri_schemes__ = ("s3",)
    __option_prefix__ = 'aws.s3'

    def __init__(
        self,
        uri: str,
        use_instance_profile: bool = None,
        iam_role_arn: str = None,
        region: str = None,
        access_key: str = None,
        secret_key: str = None,
        **kwargs,
    ):
        super().__init__(uri=uri, **kwargs)

        if isinstance(use_instance_profile, bool):
            self.add_option('use_instance_profile', str(use_instance_profile).lower())
        if iam_role_arn:
            self.add_option('iam_role_arn', iam_role_arn)
        if region:
            self.add_option('region', region)
        if access_key:
            self.add_option('access_key', access_key)
        if secret_key:
            self.add_option('secret_key', secret_key)

class OtherS3Compatibile(_StorageClause):
    __uri_schemes__ = ("s3",)
    __option_prefix__ = 'aws.s3'

    def __init__(
        self,
        uri: str,
        enable_ssl: bool = None,
        enable_path_style_access: bool = None,
        access_key: str = None,
        secret_key: str = None,
        endpoint: str = None,
        **kwargs,
    ):
        super().__init__(uri=uri, **kwargs)

        if isinstance(enable_ssl, bool):
            self.add_option('enable_ssl', str(enable_ssl).lower())
        if isinstance(enable_path_style_access, bool):
            self.add_option('enable_path_style_access', str(enable_path_style_access).lower())
        if access_key:
            self.add_option('access_key', access_key)
        if secret_key:
            self.add_option('secret_key', secret_key)
        if endpoint:
            self.add_option('endpoint', endpoint)

class AzureBlobStorage(_StorageClause):
    """Microsoft Azure Blob Storage.

    The path is ``wasb[s]://<container>@<storage_account>.blob.core.windows.net/<path>``. Authenticate with
    ``shared_key``, ``sas_token``, a user-assigned managed identity (``oauth2_use_managed_identity`` and
    ``oauth2_client_id``) or a service principal (``oauth2_client_id``, ``oauth2_client_secret`` and
    ``oauth2_tenant_id``). StarRocks' native Azure client, used unless the FE's ``azure_use_native_sdk`` is false,
    takes the storage account and container from the path; ``endpoint``, ``storage_account`` and ``container``
    are read only by the Hadoop client used when it is false.
    """
    __uri_schemes__ = ("wasb", "wasbs")
    __option_prefix__ = 'azure.blob'

    def __init__(
        self, *,
        uri: str,
        shared_key: str | None = None,
        sas_token: str | None = None,
        oauth2_use_managed_identity: bool | None = None,
        oauth2_client_id: str | None = None,
        oauth2_client_secret: str | None = None,
        oauth2_tenant_id: str | None = None,
        endpoint: str | None = None,
        storage_account: str | None = None,
        container: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(uri=uri, **kwargs)

        if shared_key:
            self.add_option('shared_key', shared_key)
        if sas_token:
            self.add_option('sas_token', sas_token)
        if isinstance(oauth2_use_managed_identity, bool):
            self.add_option('oauth2_use_managed_identity', str(oauth2_use_managed_identity).lower())
        if oauth2_client_id:
            self.add_option('oauth2_client_id', oauth2_client_id)
        if oauth2_client_secret:
            self.add_option('oauth2_client_secret', oauth2_client_secret)
        if oauth2_tenant_id:
            self.add_option('oauth2_tenant_id', oauth2_tenant_id)
        if endpoint:
            self.add_option('endpoint', endpoint)
        if storage_account:
            self.add_option('storage_account', storage_account)
        if container:
            self.add_option('container', container)

class AzureDataLakeStorage1(_StorageClause):
    """Microsoft Azure Data Lake Storage Gen1.

    The path is ``adl://<account>.azuredatalakestore.net/<path>``. Authenticate with the managed service identity
    (``use_managed_service_identity``) or a service principal (``oauth2_client_id``, ``oauth2_credential`` and
    ``oauth2_endpoint``). ``oauth2_client_credential`` and ``oauth2_client_endpoint`` are the old names of
    ``oauth2_credential`` and ``oauth2_endpoint``.
    """
    __uri_schemes__ = ("adl",)
    __option_prefix__ = 'azure.adls1'

    def __init__(
        self,
        *,
        uri: str,
        use_managed_service_identity: bool | None = None,
        oauth2_client_id: str | None = None,
        oauth2_credential: str | None = None,
        oauth2_endpoint: str | None = None,
        oauth2_client_credential: str | None = None,
        oauth2_client_endpoint: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(uri=uri, **kwargs)
        oauth2_credential = _argument_or_its_old_name(
            "oauth2_credential", oauth2_credential, "oauth2_client_credential", oauth2_client_credential
        )
        oauth2_endpoint = _argument_or_its_old_name(
            "oauth2_endpoint", oauth2_endpoint, "oauth2_client_endpoint", oauth2_client_endpoint
        )

        if isinstance(use_managed_service_identity, bool):
            self.add_option('use_managed_service_identity', str(use_managed_service_identity).lower())
        if oauth2_client_id:
            self.add_option('oauth2_client_id', oauth2_client_id)
        if oauth2_credential:
            self.add_option('oauth2_credential', oauth2_credential)
        if oauth2_endpoint:
            self.add_option('oauth2_endpoint', oauth2_endpoint)

class AzureDataLakeStorage2(_StorageClause):
    """Microsoft Azure Data Lake Storage Gen2.

    The path is ``abfs[s]://<container>@<storage_account>.dfs.core.windows.net/<path>``. Authenticate with a
    managed identity (``oauth2_use_managed_identity``, ``oauth2_tenant_id`` and ``oauth2_client_id``),
    ``shared_key``, ``sas_token``, a service principal (``oauth2_client_id``, ``oauth2_client_secret`` and
    ``oauth2_client_endpoint``) or a workload identity (``oauth2_token_file``, ``oauth2_tenant_id`` and
    ``oauth2_client_id``). Give ``storage_account`` (or ``endpoint``) with ``shared_key`` or ``sas_token``:
    StarRocks takes the account from the path when loading, but not when unloading with INSERT INTO FILES.
    """
    __uri_schemes__ = ("abfs", "abfss")
    __option_prefix__ = 'azure.adls2'

    def __init__(
        self,
        *,
        uri: str,
        shared_key: str | None = None,
        storage_account: str | None = None,
        oauth2_use_managed_identity: bool | None = None,
        oauth2_tenant_id: str | None = None,
        oauth2_client_id: str | None = None,
        oauth2_client_secret: str | None = None,
        oauth2_client_endpoint: str | None = None,
        sas_token: str | None = None,
        endpoint: str | None = None,
        oauth2_token_file: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(uri=uri, **kwargs)

        if shared_key:
            self.add_option('shared_key', shared_key)
        if storage_account:
            self.add_option('storage_account', storage_account)
        if isinstance(oauth2_use_managed_identity, bool):
            self.add_option('oauth2_use_managed_identity', str(oauth2_use_managed_identity).lower())
        if oauth2_tenant_id:
            self.add_option('oauth2_tenant_id', oauth2_tenant_id)
        if oauth2_client_id:
            self.add_option('oauth2_client_id', oauth2_client_id)
        if oauth2_client_secret:
            self.add_option('oauth2_client_secret', oauth2_client_secret)
        if oauth2_client_endpoint:
            self.add_option('oauth2_client_endpoint', oauth2_client_endpoint)
        if sas_token:
            self.add_option('sas_token', sas_token)
        if endpoint:
            self.add_option('endpoint', endpoint)
        if oauth2_token_file:
            self.add_option('oauth2_token_file', oauth2_token_file)

class GoogleCloudStorage(_StorageClause):
    """Google Cloud Storage"""
    __uri_schemes__ = ("gs",)
    __option_prefix__ = "gcp.gcs"

    def __init__(
        self,
        *,
        uri: str,
        use_compute_engine_service_account: bool = None,
        service_account_email: str = None,
        service_account_private_key_id: str = None,
        service_account_private_key: str = None,
        impersonation_service_account: str = None,
        **kwargs,
    ):
        super().__init__(uri=uri, **kwargs)
        if isinstance(use_compute_engine_service_account, bool):
            self.add_option('use_compute_engine_service_account', str(use_compute_engine_service_account).lower())
        if service_account_email:
            self.add_option('service_account_email', service_account_email)
        if service_account_private_key_id:
            self.add_option('service_account_private_key_id', service_account_private_key_id)
        if service_account_private_key:
            self.add_option('service_account_private_key', service_account_private_key)
        if impersonation_service_account:
            self.add_option('impersonation_service_account', impersonation_service_account)

class HadoopHDFSStorage(_StorageClause):
    """Hadoop HDFS"""
    __uri_schemes__ = ("hdfs",)
    __option_prefix__ = 'hadoop'

    def __init__(
        self,
        *,
        uri: str,
        username: str = None,
        password: str = None,
        **kwargs,
    ):
        super().__init__(uri=uri, **kwargs)
        # N.B. Not using add_option in this case because the option_prefix is not particularly obvious
        self.options['hadoop.security.authentication'] = 'true'
        if username:
            self.options['username'] = username
        if password:
            self.options['password'] = password
