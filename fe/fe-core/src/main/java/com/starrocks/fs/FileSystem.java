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

package com.starrocks.fs;

import com.google.common.base.Strings;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.azure.AzBlobFileSystem;
import com.starrocks.fs.hdfs.HdfsFileSystemWrap;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.fs.hdfs.WildcardURI;
import com.starrocks.thrift.TCloudConfiguration;
import org.apache.hadoop.fs.FileStatus;

import java.util.List;
import java.util.Map;

/**
 * A unified abstraction for different types of file systems (e.g., HDFS, Azure Blob).
 * This interface defines common operations like glob listing and retrieving cloud configuration used in the backend.
 */
public interface FileSystem {

    /**
     * Factory method to obtain a concrete FileSystem implementation based on the URI scheme.
     *
     * If the path scheme is 'wasb' or 'wasbs' and the configuration enables Azure native SDK usage,
     * it returns an instance of {@link AzBlobFileSystem}. Otherwise, it falls back to {@link HdfsFileSystemWrap}.
     *
     * @param path       The full URI of the path (e.g., wasbs://..., hdfs://...)
     * @param properties Configuration properties for the underlying file system
     * @return A concrete implementation of FileSystem
     * @throws StarRocksException If the scheme is missing or invalid
     */
    public static FileSystem getFileSystem(String path, Map<String, String> properties) throws StarRocksException {
        WildcardURI pathUri = new WildcardURI(path);
        String scheme = pathUri.getUri().getScheme();
        if (Strings.isNullOrEmpty(scheme)) {
            throw new StarRocksException("Invalid path. scheme is null");
        }

        if (scheme.equalsIgnoreCase(HdfsFsManager.WASB_SCHEME) || scheme.equalsIgnoreCase(HdfsFsManager.WASBS_SCHEME)) {
            // Use native Azure SDK if enabled and the scheme is wasb/wasbs
            return new AzBlobFileSystem(properties);
        } else {
            // HDFS-compatible implementation
            return new HdfsFileSystemWrap(properties);
        }
    }

    /**
     * Perform a glob-like listing of the given path.
     * The implementation may translate glob patterns and return matching files or directories.
     *
     * @param path    The glob path (may include wildcards)
     * @param skipDir Whether to skip directories in the result
     * @return A list of matched {@link FileStatus} objects
     * @throws StarRocksException On list or resolution errors
     */
    public List<FileStatus> globList(String path, boolean skipDir) throws StarRocksException;

    /**
     * Get cloud configuration in thrift format used in the backend for the given path.
     *
     * @param path Path for which properties are requested
     * @return TCloudConfiguration used in the backend
     * @throws StarRocksException On resolution errors
     */
    public TCloudConfiguration getCloudConfiguration(String path) throws StarRocksException;

}
