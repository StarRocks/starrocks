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
from urllib.parse import urlparse

from sqlalchemy.schema import Table
from sqlalchemy.sql import TableClause, ClauseElement
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.roles import FromClauseRole


class FilesClause(ClauseElement, FromClauseRole):
    __visit_name__ = 'files'

    def __init__(self, format: "FilesFormat", options: "_FilesOptions" = None):
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

class FilesTargetOptions(_FilesOptions):
    def __init__(
        self,
        *,
        single: bool = None,
        target_max_files_size: int = None,
        partitioned_by: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if single is not None:
            self.options["single"] = "true" if single else "false"
        if target_max_files_size is not None:
            self.options["target_max_files_size"] = target_max_files_size
        if partitioned_by is not None:
            self.options["partitioned_by"] = partitioned_by


class FilesSource(FilesClause):
    __visit_name__ = 'files_source'

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

    def __init__(self, compression: Compression = None, **kwargs):
        self.options = dict()
        if self.__format_type__:
            self.options['format'] = self.__format_type__
        if compression:
            self.options["compression"] = compression.value

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
    __format_type__ = "csv"

    def __init__(
        self,
        *,
        column_separator: str = None,
        row_delimiter: str = None,
        line_delimiter: str = None,
        enclose: str = None,
        escape: str = None,
        skip_header: int = None,
        trim_space: bool = None,
        compression: Compression = None,
        **kwargs,
    ):
        super().__init__(compression, **kwargs)
        if row_delimiter is not None and line_delimiter is not None:
            raise Exception('Only specify one of row_delimiter (for load) or line_delimiter (for unload)')
        if row_delimiter:
            if (
                len(str(row_delimiter).encode().decode("unicode_escape")) != 1
                and row_delimiter != "\r\n"
            ):
                raise TypeError("Record Delimiter should be a single character.")
            self.add_option("row_delimiter", row_delimiter)
        if line_delimiter:
            if (
                len(str(line_delimiter).encode().decode("unicode_escape")) != 1
                and line_delimiter != "\r\n"
            ):
                raise TypeError("Record Delimiter should be a single character.")
            self.add_option("line_delimiter", line_delimiter)
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


class ParquetFormat(FilesFormat):
    __format_type__ = "parquet"

    def __init__(
        self,
        *,
        use_legacy_encoding: bool = None, # for unloading only
        version: str = None, # for unloading only
        compression: Compression = None,
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
        compression: Compression = None,
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
    __visit_name__ = "cloud_storage"
    __uri_scheme__ = None
    __option_prefix__ = ''

    def __init__(
        self,
        *,
        uri: str,
        **kwargs,
    ):
        self.options = dict()
        url_components = urlparse(uri)
        if url_components.scheme != self.__uri_scheme__:
            raise Exception(f'Invalid path prefix for storage clause {self.__class__.__name__}')
        self.options['path'] = uri
        self.options.update(kwargs)  # Allow use of any other parameters

    def add_option(self, option_name, value):
        self.options[f'{self.__option_prefix__}.{option_name}'] = value

    def __repr__(self):
        return ",\n".join([f"{k} = {v}" for k, v in self.options.items()])

class AmazonS3(_StorageClause):
    """Amazon S3"""
    __uri_scheme__ = "s3"
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
    __uri_scheme__ = "s3"
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
    """Microsoft Azure Blob Storage"""
    __uri_scheme__ = "azure"
    __option_prefix__ = 'azure.blob'

    def __init__(
        self, *,
        uri: str,
        shared_key: str = None,
        sas_token: str = None,
        oauth2_use_managed_identity: bool = None,
        oauth2_client_id: str = None,
        oauth2_client_secret: str = None,
        oauth2_tenant_id: str = None,
        **kwargs,
    ):
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

class AzureDataLakeStorage1(_StorageClause):
    """Microsoft Azure Data Lake Storage Gen1"""
    __uri_scheme__ = "wasb"
    __option_prefix__ = 'azure.adls2'

    def __init__(
        self,
        *,
        uri: str,
        use_managed_service_identity: bool = None,
        oauth2_client_id: str = None,
        oauth2_client_credential: str = None,
        oauth2_client_endpoint: str = None,
        **kwargs,
    ):
        super().__init__(uri=uri, **kwargs)

        if isinstance(use_managed_service_identity, bool):
            self.add_option('use_managed_service_identity', str(use_managed_service_identity).lower())
        if oauth2_client_id:
            self.add_option('oauth2_client_id', oauth2_client_id)
        if oauth2_client_credential:
            self.add_option('oauth2_client_credential', oauth2_client_credential)
        if oauth2_client_endpoint:
            self.add_option('oauth2_client_endpoint', oauth2_client_endpoint)

class AzureDataLakeStorage2(_StorageClause):
    """Microsoft Azure Data Lake Storage2"""
    __uri_scheme__ = "wasb"
    __option_prefix__ = 'azure.adls2'

    def __init__(
        self,
        *,
        uri: str,
        shared_key: str = None,
        storage_account: str = None,
        oauth2_use_managed_identity: bool = None,
        oauth2_tenant_id: str = None,
        oauth2_client_id: str = None,
        oauth2_client_secret: str = None,
        oauth2_client_endpoint: str = None,
        **kwargs,
    ):
        super().__init__(uri=uri, **kwargs)

        if shared_key:
            self.add_option('shared_key', shared_key)
        if storage_account:
            self.add_option('storage_account', storage_account)
        if isinstance(oauth2_use_managed_identity, bool):
            self.add_option('oauth2_managed_identity', str(oauth2_use_managed_identity).lower())
        if oauth2_tenant_id:
            self.add_option('oauth2_tenant_id', oauth2_tenant_id)
        if oauth2_client_id:
            self.add_option('oauth2_client_id', oauth2_client_id)
        if oauth2_client_secret:
            self.add_option('oauth2_client_secret', oauth2_client_secret)
        if oauth2_client_endpoint:
            self.add_option('oauth2_client_endpoint', oauth2_client_endpoint)

class GoogleCloudStorage(_StorageClause):
    """Google Cloud Storage"""
    __uri_scheme__ = "gs"
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
    __uri_scheme__ = "hdfs"
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
