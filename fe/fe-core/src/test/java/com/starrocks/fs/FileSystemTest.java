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

import com.google.common.collect.Maps;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.azure.AzBlobFileSystem;
import com.starrocks.fs.hdfs.HdfsFileSystemWrap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class FileSystemTest {

    @Test
    public void testGetFileSystem() throws StarRocksException {
        Map<String, String> properties = Maps.newHashMap();

        {
            // wasbs
            String path = "wasbs://container_name@account_name.blob.core.windows.net/blob_name";
            FileSystem fs = FileSystem.getFileSystem(path, properties);
            Assertions.assertTrue(fs instanceof AzBlobFileSystem);
        }

        {
            // abfs
            String path = "abfs://container_name@account_name.blob.core.windows.net/blob_name";
            FileSystem fs = FileSystem.getFileSystem(path, properties);
            Assertions.assertTrue(fs instanceof HdfsFileSystemWrap);
        }

        {
            // hdfs
            String path = "hdfs://host:port/file_name";
            FileSystem fs = FileSystem.getFileSystem(path, properties);
            Assertions.assertTrue(fs instanceof HdfsFileSystemWrap);
        }

        {
            // invalid scheme
            String path = "file_name";
            ExceptionChecker.expectThrows(StarRocksException.class, () -> FileSystem.getFileSystem(path, properties));
        }
    }
}
