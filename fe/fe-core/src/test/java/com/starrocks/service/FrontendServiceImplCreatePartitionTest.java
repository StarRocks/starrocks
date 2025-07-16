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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FrontendServiceImplCreatePartitionTest {
    @Mocked
    ExecuteEnv exeEnv;

    private static ConnectContext connectContext;

    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        UtFrameUtils.addMockComputeNode(50001);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                    .withTable("CREATE TABLE test_table (\n" +
                                "    event_day DATE,\n" +
                                "    site_id INT DEFAULT '10',\n" +
                                "    city_code VARCHAR(100),\n" +
                                "    user_name VARCHAR(32) DEFAULT '',\n" +
                                "    pv BIGINT DEFAULT '0'\n" +
                                ")\n" +
                                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                                "PARTITION BY date_trunc('day', event_day)\n" +
                                "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\"\n" +
                                ");");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists test.test_table";
        try {
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        } catch (Exception ex) {

        }
    }

    @Test
    public void testCreatePartition() throws TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        new MockUp<WarehouseManager>() {
            int count = 0;
            @Mock
            public Long getAliveComputeNodeId(ComputeResource computeResource, long tabletId) {
                if (count < 1) {
                    count++;
                    return 50001L;
                }
                return null;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_table");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("2025-07-10");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
        Assertions.assertTrue(partition.getStatus().getError_msgs().get(0)
                .contains("No alive compute node found for tablet. " + "Check if any backend is down or not. tablet_id:"));
    }
}
