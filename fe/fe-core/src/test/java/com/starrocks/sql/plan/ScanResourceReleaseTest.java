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

package com.starrocks.sql.plan;

import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.Status;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.planner.DeltaLakeScanNode;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.qe.scheduler.Coordinator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.type.PrimitiveType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScanResourceReleaseTest extends ConnectorPlanTestBase {

    private static final String HIVE_QUERY = "select * from hive0.partitioned_db.t1";

    @AfterEach
    public void restoreHiveMetadata() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, new MockedHiveMetadata());
    }

    @Test
    public void testAPlanThatFailsToAssembleClearsItsScans() {
        AtomicInteger cleared = new AtomicInteger();
        new MockUp<ScanNode>() {
            @Mock
            public void clear() {
                cleared.incrementAndGet();
            }
        };
        new MockUp<ExecPlan>() {
            @Mock
            public void setPlanCount(int planCount) {
                throw new IllegalStateException("planning failed after the scan was registered");
            }
        };

        assertThrows(Exception.class, () -> getExecPlan(HIVE_QUERY));
        assertEquals(1, cleared.get());
    }

    @Test
    public void testAnInternalQueryClearsExternalResources() throws Exception {
        ExecPlan plan = getExecPlan(HIVE_QUERY);
        AtomicInteger cleared = new AtomicInteger();
        new MockUp<DefaultCoordinator>() {
            @Mock
            public void exec() throws StarRocksException {
                throw new StarRocksException("backend unavailable");
            }

            @Mock
            public void clearExternalResources() {
                cleared.incrementAndGet();
            }
        };

        StmtExecutor executor = new StmtExecutor(connectContext,
                UtFrameUtils.parseStmtWithNewParser(HIVE_QUERY, connectContext));
        executor.executeStmtWithExecPlan(connectContext, plan);

        assertEquals(1, cleared.get());
    }

    @Test
    public void testADmlWithProfileClearsExternalResources() throws Exception {
        ExecPlan plan = getExecPlan(HIVE_QUERY);
        Coordinator coord = Mockito.mock(Coordinator.class);
        new MockUp<StmtExecutor>() {
            @Mock
            public void handleDMLStmt(ExecPlan execPlan, DmlStmt stmt) {
            }
        };
        StmtExecutor executor = new StmtExecutor(connectContext,
                UtFrameUtils.parseStmtWithNewParser(HIVE_QUERY, connectContext));
        Deencapsulation.setField(executor, "coord", coord);

        executor.handleDMLStmtWithProfile(plan, null);

        Mockito.verify(coord).clearExternalResources();
    }

    /** Failing before a coordinator exists reports the cause instead of an NPE, and still clears the scans. */
    @Test
    public void testAnInternalQueryFailingBeforeSchedulingKeepsItsCause() throws Exception {
        ExecPlan plan = getExecPlan(HIVE_QUERY);
        AtomicInteger cleared = new AtomicInteger();
        new MockUp<DescriptorTable>() {
            @Mock
            public TDescriptorTable toThrift() {
                throw new StarRocksConnectorException("metastore unavailable");
            }
        };
        new MockUp<ScanNode>() {
            @Mock
            public void clear() {
                cleared.incrementAndGet();
            }
        };
        StmtExecutor executor = new StmtExecutor(connectContext,
                UtFrameUtils.parseStmtWithNewParser(HIVE_QUERY, connectContext));

        Status status = executor.executeStmtWithExecPlan(connectContext, plan).second;

        assertTrue(status.getErrorMsg().contains("metastore unavailable"), status.getErrorMsg());
        assertEquals(1, cleared.get());
    }

    /** An Iceberg scan that fails after setting up its scan ranges, before it is registered, is cleared. */
    @Test
    public void testAnIcebergScanFailingBeforeRegistrationIsCleared() {
        AtomicInteger cleared = new AtomicInteger();
        new MockUp<IcebergScanNode>() {
            @Mock
            public HDFSScanNodePredicates getScanNodePredicates() {
                throw new IllegalStateException("failed after setup");
            }

            @Mock
            public void clear() {
                cleared.incrementAndGet();
            }
        };

        assertThrows(Exception.class, () -> getExecPlan("select * from iceberg0.unpartitioned_db.t0"));
        assertEquals(1, cleared.get());
    }

    /** The same for a Delta Lake scan. */
    @Test
    public void testADeltaLakeScanFailingBeforeRegistrationIsCleared() {
        AtomicInteger cleared = new AtomicInteger();
        // The mocked Delta table carries no snapshot, which the analyzer would otherwise dereference.
        new MockUp<DeltaLakeTable>() {
            @Mock
            public String getTableIdentifier() {
                return "deltalake_catalog.deltalake_db.tbl";
            }
        };
        new MockUp<DeltaLakeScanNode>() {
            @Mock
            public void setupScanRangeSource(boolean enableIncrementalScanRanges) {
            }

            @Mock
            public HDFSScanNodePredicates getScanNodePredicates() {
                throw new IllegalStateException("failed after setup");
            }

            @Mock
            public void clear() {
                cleared.incrementAndGet();
            }
        };

        assertThrows(Exception.class, () -> getExecPlan("select * from deltalake_catalog.deltalake_db.tbl"));
        assertEquals(1, cleared.get());
    }

    /** A partition lookup failure keeps its cause instead of turning into a null descriptor. */
    @Test
    public void testAPartitionLookupFailureIsRethrown() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, new MockedHiveMetadata() {
            @Override
            public List<PartitionInfo> getPartitions(Table table, List<String> partitionNames) {
                throw new StarRocksConnectorException("metastore unavailable");
            }
        });
        HiveTable table = (HiveTable) metadataMgr.getTable(connectContext, "hive0", "partitioned_db", "duplicate_partition");
        assertTrue(!table.getPartitionColumnNames().isEmpty());
        PartitionKey key = new PartitionKey();
        for (int i = 0; i < table.getPartitionColumnNames().size(); i++) {
            key.pushColumn(new StringLiteral("1"), PrimitiveType.VARCHAR);
        }

        StarRocksConnectorException e = assertThrows(StarRocksConnectorException.class,
                () -> table.toThrift(List.of(new DescriptorTable.ReferencedPartitionInfo(1L, key))));
        assertTrue(e.getMessage().contains("metastore unavailable"), e.getMessage());
    }
}
