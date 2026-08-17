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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class PreSplitHadoopAccessTest {

    @Test
    void hadoopConfigurationDisablesFileSystemCacheForCredentialedSchemes() {
        // Hadoop's FileSystem.CACHE ignores the passed Configuration on a cache
        // hit; without disable-cache the second concurrent load with different
        // credentials would silently reuse the first load's filesystem. Pin
        // the contract for every scheme StarRocks's HdfsFsManager supports.
        Configuration hadoopConfig = PreSplitHadoopAccess.buildHadoopConfiguration(Collections.emptyMap());
        for (String scheme : PreSplitHadoopAccess.SCHEMES_TO_BUILD_FRESH_FILESYSTEM) {
            Assertions.assertTrue(
                    hadoopConfig.getBoolean("fs." + scheme + ".impl.disable.cache", /*defaultValue=*/ false),
                    "fs." + scheme + ".impl.disable.cache must be true");
        }
    }

    @Test
    void hadoopFileStatusPreservesPathAndSize() {
        TBrokerFileStatus brokerFileStatus = new TBrokerFileStatus(
                "oss://bucket/load/data.parquet", /*isDir=*/ false, /*size=*/ 1024L, /*isSplitable=*/ true);

        FileStatus hadoopFileStatus = PreSplitHadoopAccess.toHadoopFileStatus(brokerFileStatus);

        Assertions.assertEquals(1024L, hadoopFileStatus.getLen());
        Assertions.assertFalse(hadoopFileStatus.isDirectory());
        Assertions.assertEquals("oss://bucket/load/data.parquet", hadoopFileStatus.getPath().toString());
    }
}
