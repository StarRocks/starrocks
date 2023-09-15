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

package com.starrocks.credential;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FileSystemConstants {
    // For s3 scheme, we have to set impl 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    public static String[] S3_SCHEMES = new String[] {"s3", "s3a", "oss", "ks3", "obs", "tos", "cosn", "oss"};
    // For those schemes, we don't need to set explictly.
    public static String[] REST_SCHEMES = new String[] {"wasb", "wasbs", "adl", "abfs", "abfss", "gs", "hdfs", "viewfs", "har"};

    public static List<String> ALL_SCHEMES = new ArrayList<String>();

    static {
        ALL_SCHEMES.addAll(Arrays.asList(S3_SCHEMES));
        ALL_SCHEMES.addAll(Arrays.asList(REST_SCHEMES));
    }

    public static String FS_IMPL_FMT = "fs.%s.impl";
    public static String FS_IMPL_DISABLE_CACHE_FMT = "fs.%s.impl.disable.cache";
    public static String FS_IMPL_STARROCKS_CACHE_FILESYSTEM = "com.starrocks.fs.CacheFileSystem";
}
