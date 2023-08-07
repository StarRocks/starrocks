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

package com.starrocks.connector.hive;

import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Map;

import static com.starrocks.connector.hive.HiveMetastoreOperations.EXTERNAL_LOCATION_PROPERTY;
import static com.starrocks.connector.hive.HiveMetastoreOperations.LOCATION_PROPERTY;

public class HiveWriteUtils {
    private static final Logger LOG = LogManager.getLogger(HiveWriteUtils.class);
    public static boolean isS3Url(String prefix) {
        return prefix.startsWith("oss://") || prefix.startsWith("s3n://") || prefix.startsWith("s3a://") ||
                prefix.startsWith("s3://") || prefix.startsWith("cos://") || prefix.startsWith("cosn://") ||
                prefix.startsWith("obs://") || prefix.startsWith("ks3://") || prefix.startsWith("tos://");
    }

    public static void checkLocationProperties(Map<String, String> properties) throws DdlException {
        if (properties.containsKey(EXTERNAL_LOCATION_PROPERTY) || properties.containsKey(LOCATION_PROPERTY)) {
            throw new DdlException("Can't create non-managed Hive table. " +
                    "Only supports creating hive table under Database location. " +
                    "You could execute command without location properties");
        }
    }

    public static boolean pathExists(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return fileSystem.exists(path);
        } catch (Exception e) {
            LOG.error("Failed checking path {}", path, e);
            throw new StarRocksConnectorException("Failed checking path: " + path + " msg:" + e.getMessage());
        }
    }

    public static boolean isDirectory(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return fileSystem.getFileStatus(path).isDirectory();
        } catch (IOException e) {
            LOG.error("Failed checking path {}", path, e);
            throw new StarRocksConnectorException("Failed checking path: " + path);
        }
    }

    public static void createDirectory(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            if (!fileSystem.mkdirs(path)) {
                LOG.error("Mkdir {} returned false", path);
                throw new IOException("mkdirs returned false");
            }
        } catch (IOException e) {
            LOG.error("Failed to create directory: {}", path);
            throw new StarRocksConnectorException("Failed to create directory: " + path, e);
        }
    }
}
