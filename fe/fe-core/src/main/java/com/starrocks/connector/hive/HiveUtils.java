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

import com.google.common.base.Preconditions;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.ExprCastFunction;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.UUID;

import static com.starrocks.connector.hive.HiveMetastoreOperations.EXTERNAL_LOCATION_PROPERTY;

public class HiveUtils {
    private static final Logger LOG = LogManager.getLogger(HiveUtils.class);
    public static boolean isS3Url(String prefix) {
        return prefix.startsWith("oss://") || prefix.startsWith("s3n://") || prefix.startsWith("s3a://") ||
                prefix.startsWith("s3://") || prefix.startsWith("cos://") || prefix.startsWith("cosn://") ||
                prefix.startsWith("obs://") || prefix.startsWith("ks3://") || prefix.startsWith("tos://");
    }

    public static void checkLocationProperties(Map<String, String> properties) throws DdlException {
        if (properties.containsKey(EXTERNAL_LOCATION_PROPERTY)) {
            throw new DdlException("Can't create non-managed Hive table. " +
                    "Only supports creating hive table under Database location. " +
                    "You could execute command without external_location properties");
        }
    }

    public static void checkExternalLocationProperties(Map<String, String> properties) throws DdlException {
        if (!properties.containsKey(EXTERNAL_LOCATION_PROPERTY)) {
            throw new DdlException("Can't create external Hive table without external_location property.");
        }
    }

    public static boolean pathExists(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return fileSystem.exists(path);
        } catch (Exception e) {
            LOG.error("Failed to check path {}", path, e);
            throw new StarRocksConnectorException("Failed to check path: " + path + ". msg: " + e.getMessage());
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

    public static boolean isEmpty(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            return !fileSystem.listFiles(path, false).hasNext();
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

    public static String getStagingDir(HiveTable table, String tempStagingDir) {
        String stagingDir;
        String location = table.getTableLocation();
        if (isS3Url(location)) {
            stagingDir = location;
        } else {
            Path tempRoot = new Path(location, tempStagingDir);
            Path tempStagingPath = new Path(tempRoot, UUID.randomUUID().toString());
            stagingDir = tempStagingPath.toString();
        }
        return stagingDir.endsWith("/") ? stagingDir : stagingDir + "/";
    }

    public static boolean fileCreatedByQuery(String fileName, String queryId) {
        Preconditions.checkState(queryId.length() > 8, "file name or query id is invalid");
        if (fileName.length() <= queryId.length()) {
            // file is created by other engine like hive
            return false;
        }
        String checkQueryId = queryId.substring(0, queryId.length() - 8);
        return fileName.startsWith(checkQueryId) || fileName.endsWith(checkQueryId);
    }

    public static void checkedDelete(FileSystem fileSystem, Path file, boolean recursive) throws IOException {
        try {
            if (!fileSystem.delete(file, recursive)) {
                if (fileSystem.exists(file)) {
                    throw new IOException("Failed to delete " + file);
                }
            }
        } catch (FileNotFoundException ignored) {
            // ignore
        }
    }

    public static boolean deleteIfExists(Path path, boolean recursive, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            if (fileSystem.delete(path, recursive)) {
                return true;
            }

            return !fileSystem.exists(path);
        } catch (FileNotFoundException ignored) {
            return true;
        } catch (IOException ignored) {
            LOG.error("Failed to delete remote path {}", path);
        }

        return false;
    }

    public static boolean createDirectoryIfNotExists(Path path, Configuration conf) {
        try {
            FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
            if (fileSystem.exists(path)) {
                return false;
            }
            return fileSystem.mkdirs(path);
        } catch (IOException e) {
            LOG.error("Failed to create remote path {}", path, e);
            throw new StarRocksConnectorException("Failed to create remote path: " + path);
        }
    }

    public static LiteralExpr normalizeKey(LiteralExpr key, Type targetType) {
        if (key instanceof DecimalLiteral) {
            DecimalLiteral decimalKey = (DecimalLiteral) key;
            ScalarType type = (ScalarType) targetType;
            int scale = type.decimalScale();
    
            BigDecimal scaled = decimalKey.getValue()
                    .setScale(scale, RoundingMode.HALF_UP);
    
            key = new DecimalLiteral(scaled);
        }

        if (!key.getType().equals(targetType)) {
            try {
                key = (LiteralExpr) ExprCastFunction.castTo(key, targetType);
            } catch (AnalysisException e) {
                throw new StarRocksConnectorException(
                        String.format("Failed to cast partition column literal %s to %s",
                                key.getStringValue(), targetType), e);
            }
        }
        return key;
    }

}
