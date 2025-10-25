// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.staros.util;

import com.staros.proto.FileStoreType;

public class Constant {
    public static final long DEFAULT_ID = 0L;
    public static final String EMPTY_SERVICE_ID = "";
    public static final long DEFAULT_PROTOBUF_INTEGER = 0L;
    public static final String DEFAULT_DIGEST_ALGORITHM = "MD5";
    public static final FileStoreType FS_NOT_SET = FileStoreType.INVALID;
    public static String S3_FSKEY_FOR_CONFIG = "s3_fskey_for_filestore_from_configuration";
    public static String S3_FSNAME_FOR_CONFIG = "s3_fsname_for_filestore_from_configuration";
    public static String HDFS_FSKEY_FOR_CONFIG = "hdfs_fskey_for_filestore_from_configuration";
    public static String HDFS_FSNAME_FOR_CONFIG = "hdfs_fsname_for_filestore_from_configuration";
    public static String AZURE_BLOB_FSKEY_FOR_CONFIG = "azblob_fskey_for_filestore_from_configuration";
    public static String AZURE_BLOB_FSNAME_FOR_CONFIG = "azblob_fsname_for_filestore_from_configuration";
    public static String AZURE_ADLS2_FSKEY_FOR_CONFIG = "adls2_fskey_for_filestore_from_configuration";
    public static String AZURE_ADLS2_FSNAME_FOR_CONFIG = "adls2_fsname_for_filestore_from_configuration";
    public static String GS_FSKEY_FOR_CONFIG = "gs_fskey_for_filestore_from_configuration";
    public static String GS_FSNAME_FOR_CONFIG = "gs_fsname_for_filestore_from_configuration";
    public static final String S3_PREFIX = "s3://";
    public static final String AZURE_BLOB_PREFIX = "azblob://";
    public static final String AZURE_ADLS2_PREFIX = "adls2://";
    public static final String GS_PREFIX = "gs://";
}
