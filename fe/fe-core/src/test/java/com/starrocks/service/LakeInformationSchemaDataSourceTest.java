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

package com.starrocks.service;

import com.google.gson.Gson;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetPartitionsMetaRequest;
import com.starrocks.thrift.TGetPartitionsMetaResponse;
import com.starrocks.thrift.TGetTablesConfigRequest;
import com.starrocks.thrift.TGetTablesConfigResponse;
import com.starrocks.thrift.TPartitionMetaInfo;
import com.starrocks.thrift.TTableConfigInfo;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class LakeInformationSchemaDataSourceTest {

    @Mocked
    ExecuteEnv exeEnv;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
    }

    @Test
    public void testGetLakeTablesConfig() throws Exception {
        starRocksAssert.withEnableMV().withDatabase("db1").useDatabase("db1");

        String createTblStmtStr = "CREATE TABLE db1.tbl1 (`k1` int,`k2` int,`k3` int,`v1` int,`v2` int,`v3` int) " +
                "ENGINE=OLAP " + "PRIMARY KEY(`k1`, `k2`, `k3`) " +
                "COMMENT \"OLAP\" " +
                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3 " +
                "ORDER BY(`v2`, `v3`) " +
                "PROPERTIES ('replication_num' = '1');";
        starRocksAssert.withTable(createTblStmtStr);

        String createMvStmtStr = "CREATE MATERIALIZED VIEW db1.mv1 " +
                "DISTRIBUTED BY HASH(k1) BUCKETS 10 " +
                "REFRESH ASYNC " +
                "AS SELECT k1, k2 " +
                "FROM db1.tbl1 ";

        starRocksAssert.withMaterializedView(createMvStmtStr);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesConfigRequest req = new TGetTablesConfigRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("db1");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetTablesConfigResponse response = impl.getTablesConfig(req);
        TTableConfigInfo tableConfig = response.getTables_config_infos().stream()
                .filter(t -> t.getTable_name().equals("tbl1")).findFirst()
                .orElseGet(null);
        Assertions.assertEquals("db1", tableConfig.getTable_schema());
        Assertions.assertEquals("tbl1", tableConfig.getTable_name());
        Assertions.assertEquals("CLOUD_NATIVE", tableConfig.getTable_engine());
        Assertions.assertEquals("PRIMARY_KEYS", tableConfig.getTable_model());
        Assertions.assertEquals("`k1`, `k2`, `k3`", tableConfig.getPrimary_key());
        Assertions.assertEquals("", tableConfig.getPartition_key());
        Assertions.assertEquals("`k1`, `k2`, `k3`", tableConfig.getDistribute_key());
        Assertions.assertEquals("HASH", tableConfig.getDistribute_type());
        Assertions.assertEquals(3, tableConfig.getDistribute_bucket());
        Assertions.assertEquals("`v2`, `v3`", tableConfig.getSort_key());
        Map<String, String> propsMap = new HashMap<>();
        propsMap = new Gson().fromJson(tableConfig.getProperties(), propsMap.getClass());
        Assertions.assertEquals("builtin_storage_volume", propsMap.get("storage_volume"));


        TTableConfigInfo mvConfig = response.getTables_config_infos().stream()
                .filter(t -> t.getTable_engine().equals("CLOUD_NATIVE_MATERIALIZED_VIEW")).findFirst()
                .orElseGet(null);
        Assertions.assertEquals("CLOUD_NATIVE_MATERIALIZED_VIEW", mvConfig.getTable_engine());
        propsMap = new HashMap<>();
        propsMap = new Gson().fromJson(mvConfig.getProperties(), propsMap.getClass());
        Assertions.assertEquals("1", propsMap.get("replication_num"));
        Assertions.assertEquals("HDD", propsMap.get("storage_medium"));
        Assertions.assertEquals("builtin_storage_volume", propsMap.get("storage_volume"));
    }

    /**
     * Test getPartitionsMeta for cloud native tables.
     * This covers InformationSchemaDataSource.java lines 480-484:
     * - setCompact_version
     * - setEnable_datacache
     */
    @Test
    public void testGetLakePartitionsMeta() throws Exception {
        starRocksAssert.withEnableMV().withDatabase("db_partition_meta").useDatabase("db_partition_meta");

        String createTblStmtStr = "CREATE TABLE db_partition_meta.lake_table " +
                "(`k1` int,`k2` int,`v1` int) " +
                "PRIMARY KEY(`k1`, `k2`) " +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 2 " +
                "PROPERTIES ('replication_num' = '1', 'datacache.enable' = 'true');";
        starRocksAssert.withTable(createTblStmtStr);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetPartitionsMetaRequest req = new TGetPartitionsMetaRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("db_partition_meta");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetPartitionsMetaResponse response = impl.getPartitionsMeta(req);

        TPartitionMetaInfo partitionMeta = response.getPartitions_meta_infos().stream()
                .filter(t -> t.getTable_name().equals("lake_table")).findFirst().orElse(null);
        Assertions.assertNotNull(partitionMeta);
        Assertions.assertEquals("db_partition_meta", partitionMeta.getDb_name());
        Assertions.assertEquals("lake_table", partitionMeta.getTable_name());
        // Verify cloud-native specific fields are set (lines 480-484)
        // compact_version is set (defaults to 0 if no compaction statistics)
        Assertions.assertTrue(partitionMeta.getCompact_version() >= 0);
        // enable_datacache should be true since we set 'datacache.enable' = 'true'
        Assertions.assertTrue(partitionMeta.isEnable_datacache());
    }

    @Test
    public void testGetLakePartitionsMetaWithoutDatacache() throws Exception {
        starRocksAssert.withEnableMV().withDatabase("db_partition_meta2").useDatabase("db_partition_meta2");

        String createTblStmtStr = "CREATE TABLE db_partition_meta2.lake_table_no_cache " +
                "(`k1` int,`k2` int,`v1` int) " +
                "PRIMARY KEY(`k1`, `k2`) " +
                "DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 2 " +
                "PROPERTIES ('replication_num' = '1', 'datacache.enable' = 'false');";
        starRocksAssert.withTable(createTblStmtStr);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetPartitionsMetaRequest req = new TGetPartitionsMetaRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("db_partition_meta2");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetPartitionsMetaResponse response = impl.getPartitionsMeta(req);

        TPartitionMetaInfo partitionMeta = response.getPartitions_meta_infos().stream()
                .filter(t -> t.getTable_name().equals("lake_table_no_cache")).findFirst().orElse(null);
        Assertions.assertNotNull(partitionMeta);
        Assertions.assertEquals("db_partition_meta2", partitionMeta.getDb_name());
        // compact_version is set (defaults to 0 if no compaction statistics)
        Assertions.assertTrue(partitionMeta.getCompact_version() >= 0);
        // enable_datacache should be false
        Assertions.assertFalse(partitionMeta.isEnable_datacache());
    }
}
