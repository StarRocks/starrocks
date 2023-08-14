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

package com.starrocks.pseudocluster;

import com.staros.proto.AwsCredentialInfo;
import com.staros.proto.AwsDefaultCredentialInfo;
import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import com.staros.proto.S3FileStoreInfo;
import com.starrocks.common.AnalysisException;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.SharedNothingStorageVolumeMgr;
import com.starrocks.storagevolume.StorageVolume;
import mockit.Mock;
import mockit.MockUp;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;

public class PseudoClusterTest {
    @BeforeClass
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testCreateTabletAndInsert() throws Exception {
        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            stmt.execute("create database test");
            stmt.execute("use test");
            stmt.execute("create table test ( pk bigint NOT NULL, v0 string not null, v1 int not null ) " + 
                    "primary KEY (pk) DISTRIBUTED BY HASH(pk) BUCKETS 7 " +
                    "PROPERTIES(\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\")");
            Assert.assertFalse(stmt.execute("insert into test values (1,\"1\", 1), (2,\"2\",2), (3,\"3\",3)"));
            System.out.printf("updated %d rows\n", stmt.getUpdateCount());
            stmt.execute("select * from test");
        } finally {
            stmt.close();
            connection.close();
        }
    }

    @Test
    public void testCreateLakeTable() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        FileStoreInfo.Builder fsBuilder = builder.getFsInfoBuilder();

        S3FileStoreInfo.Builder s3FsBuilder = fsBuilder.getS3FsInfoBuilder();
        s3FsBuilder.setBucket("test-bucket");
        s3FsBuilder.setRegion("test-region");
        s3FsBuilder.setCredential(AwsCredentialInfo.newBuilder()
                .setDefaultCredential(AwsDefaultCredentialInfo.newBuilder().build()));
        S3FileStoreInfo s3FsInfo = s3FsBuilder.build();

        fsBuilder.setFsType(FileStoreType.S3);
        fsBuilder.setFsKey("test-bucket");
        fsBuilder.setFsName("test-fsname");
        fsBuilder.setS3FsInfo(s3FsInfo);
        FileStoreInfo fsInfo = fsBuilder.build();

        builder.setFsInfo(fsInfo);
        builder.setFullPath("s3://test-bucket/1/");
        FilePathInfo pathInfo = builder.build();

        new MockUp<StarOSAgent>() {
            @Mock
            public long getPrimaryComputeNodeIdByShard(long shardId, long workerGroupId) {
                return GlobalStateMgr.getCurrentSystemInfo().getBackendIds(true).get(0);
            }

            @Mock
            public FilePathInfo allocateFilePath(String storageVolumeId, long tableId) {
                return pathInfo;
            }
        };

        new MockUp<SharedNothingStorageVolumeMgr>() {
            @Mock
            public StorageVolume getStorageVolume(String svKey) throws AnalysisException {
                return StorageVolume.fromFileStoreInfo(fsInfo);
            }

            @Mock
            public String getStorageVolumeIdOfTable(long tableId) {
                return fsInfo.getFsKey();
            }
        };

        Connection connection = PseudoCluster.getInstance().getQueryConnection();
        Statement stmt = connection.createStatement();
        try {
            stmt.execute("create database db_test_lake");
            stmt.execute("use db_test_lake");
            stmt.execute("create table test (k0 bigint NOT NULL, v0 string not null, v1 int not null ) " +
                    "duplicate KEY(k0) DISTRIBUTED BY HASH(k0) BUCKETS 7");
        } finally {
            stmt.close();
            connection.close();
        }
    }
}
