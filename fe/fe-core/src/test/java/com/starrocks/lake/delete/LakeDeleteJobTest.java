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

package com.starrocks.lake.delete;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.proto.BinaryPredicatePB;
import com.starrocks.proto.DeleteDataRequest;
import com.starrocks.proto.DeleteDataResponse;
import com.starrocks.proto.DeletePredicatePB;
import com.starrocks.proto.InPredicatePB;
import com.starrocks.proto.IsNullPredicatePB;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.BrpcProxy;
import com.starrocks.rpc.LakeService;
import com.starrocks.rpc.LakeServiceWithMetrics;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

public class LakeDeleteJobTest {

    @BeforeAll
    public static void beforeClass() {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster(true, RunMode.SHARED_DATA);
        GlobalStateMgr.getCurrentState().getTabletStatMgr().setStop();
        GlobalStateMgr.getCurrentState().getStarMgrMetaSyncer().setStop();
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        for (ComputeNode node : systemInfoService.getBackends()) {
            node.setAlive(true);
        }
        for (ComputeNode node : systemInfoService.getComputeNodes()) {
            node.setAlive(true);
        }
    }

    @Test
    public void testDeleteConditionsColumnIdAfterRename(@Mocked LakeService lakeService) {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        TableName tblName = new TableName("test", "dup_table");
        String createTableSql = "CREATE TABLE " + tblName + " (colName String) DUPLICATE KEY(colName) " +
                "DISTRIBUTED BY HASH(colName) BUCKETS 1 " + "PROPERTIES('replication_num' = '1');";

        // create test.dup_table
        Assertions.assertDoesNotThrow(() -> starRocksAssert.withDatabase(tblName.getDb()).withTable(createTableSql));
        // rename column colName to colNameNew
        Assertions.assertDoesNotThrow(
                () -> starRocksAssert.alterTable("ALTER TABLE " + tblName + " RENAME COLUMN colName TO colNameNew;"));

        // A single DELETE statement to cover all the possible predicates, may not be reasonable in reality.
        // Expect the delete condition to use columnId instead of logical name
        String deleteSql =
                "DELETE FROM " + tblName + " WHERE colNameNew = 'a' AND colNameNew IS NOT NULL AND colNameNew IN ('a');";
        DeleteStmt deleteStmt;
        try {
            deleteStmt = (DeleteStmt) UtFrameUtils.parseStmtWithNewParser(deleteSql, ctx);
        } catch (Exception e) {
            Assertions.fail("Don't expect exception: " + e.getMessage());
            return;
        }

        Table table = starRocksAssert.getTable(tblName.getDb(), tblName.getTbl());
        Assertions.assertNotNull(table);
        Column col = table.getColumn("colNameNew");
        Assertions.assertNotNull(col);
        Assertions.assertEquals("colNameNew", col.getName());
        Assertions.assertEquals("colName", col.getColumnId().getId());

        // Simulate one tablet failed, our purpose is to verify the request only.
        // return a failed response to fail the job so that the following commit() will be skipped.
        DeleteDataResponse response = new DeleteDataResponse();
        response.failedTablets = List.of(1002L);

        AtomicReference<DeleteDataRequest> capturedRequest = new AtomicReference<>();
        LakeServiceWithMetrics wrappedLakeService = new LakeServiceWithMetrics(lakeService);
        // Avoid directly mocking the LakeService, which is implemented by BrpcProxy reflection proxy.
        new MockUp<LakeServiceWithMetrics>() {
            @Mock
            Future<DeleteDataResponse> deleteData(DeleteDataRequest request) {
                capturedRequest.set(request);
                return CompletableFuture.completedFuture(response);
            }
        };

        new MockUp<BrpcProxy>() {
            @Mock
            public LakeService getLakeService(String host, int port) throws RpcException {
                return wrappedLakeService;
            }
        };
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> GlobalStateMgr.getCurrentState().getDeleteMgr().process(deleteStmt));
        Assertions.assertEquals("Failed to execute delete. failed tablet num: 1", exception.getMessage());

        Assertions.assertNotNull(capturedRequest.get());
        DeletePredicatePB deletePredicate = capturedRequest.get().getDeletePredicate();

        // colNameNew = 'a'
        Assertions.assertEquals(1, deletePredicate.getBinaryPredicates().size());
        BinaryPredicatePB binaryPredicatePB = deletePredicate.getBinaryPredicates().get(0);
        Assertions.assertEquals("colName", binaryPredicatePB.getColumnName());
        Assertions.assertEquals("=", binaryPredicatePB.getOp());
        Assertions.assertEquals("a", binaryPredicatePB.getValue());

        // colNameNew IS NOT NULL
        Assertions.assertEquals(1, deletePredicate.getIsNullPredicates().size());
        IsNullPredicatePB isNullPredicatePB = deletePredicate.getIsNullPredicates().get(0);
        Assertions.assertEquals("colName", isNullPredicatePB.getColumnName());
        Assertions.assertTrue(isNullPredicatePB.isIsNotNull());

        // colNameNew IN ('a')
        Assertions.assertEquals(1, deletePredicate.getInPredicates().size());
        InPredicatePB inPredicatePB = deletePredicate.getInPredicates().get(0);
        Assertions.assertEquals("colName", inPredicatePB.getColumnName());
        Assertions.assertFalse(inPredicatePB.isIsNotIn());
        Assertions.assertEquals(List.of("a"), inPredicatePB.getValues());
    }
}
