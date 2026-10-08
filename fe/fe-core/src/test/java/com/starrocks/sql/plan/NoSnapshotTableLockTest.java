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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.FileTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.type.IntegerType.INT;

/**
 * Tables that live in an internal database but keep their data elsewhere -- FILE, and HIVE / ICEBERG / HUDI
 * created from a resource -- are not meta lock targets (Table.isMetaLockTarget). They used to be: with no
 * snapshot to plan against, they held the lock for the whole planning phase, so a FileTable's file listing
 * (row-count estimate and scan ranges) and a resource-mapping table's metastore calls ran under it.
 */
public class NoSnapshotTableLockTest extends PlanTestBase {

    private volatile Thread testThread;
    private final AtomicBoolean listedUnderLock = new AtomicBoolean();
    private final AtomicInteger listCalls = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withTable("create external table test.file_tbl (v1 bigint, v2 bigint) engine=file " +
                "properties (\"path\"=\"hdfs://127.0.0.1:10000/file_tbl/\", \"format\"=\"parquet\")");
    }

    @AfterEach
    public void tearDown() {
        testThread = null;
    }

    /** Samples the lock on every file listing the test thread makes; the listing itself returns one file. */
    private void probeFileListing() {
        testThread = Thread.currentThread();
        listedUnderLock.set(false);
        listCalls.set(0);
        new MockUp<FileTable>() {
            @Mock
            public List<RemoteFileDesc> getFileDescsFromHdfs() {
                if (Thread.currentThread() == testThread) {
                    listCalls.incrementAndGet();
                    if (LockHoldDepth.isUnderLock()) {
                        listedUnderLock.set(true);
                    }
                }
                return Lists.newArrayList(new RemoteFileDesc("f0.parquet", "snappy", 0, 0, ImmutableList.of()));
            }
        };
    }

    @Test
    public void testFileTableIsNotALockTarget() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "file_tbl");
        Assertions.assertInstanceOf(FileTable.class, table);
        Assertions.assertFalse(table.isMetaLockTarget());
    }

    @Test
    public void testFileTableListsFilesOutsideTheLock() throws Exception {
        probeFileListing();
        getFragmentPlan("select * from test.file_tbl");
        Assertions.assertTrue(listCalls.get() > 0, "the probe never saw a file listing");
        Assertions.assertFalse(listedUnderLock.get(), "a FileTable listed its files under the meta lock");
    }

    /**
     * LogicalPlanPrinter had no visitor for a FILE scan, and the base visitPhysicalScan dispatched straight back to
     * visit(), recursing until the stack overflowed. The fallback names such a scan after its operator type.
     */
    @Test
    public void testPhysicalPlanOfAFileScanCanBePrinted() throws Exception {
        probeFileListing();
        String physicalPlan = UtFrameUtils.getPlanAndFragment(connectContext, "select * from test.file_tbl").first;
        assertContains(physicalPlan, "FILE SCAN");
    }

    /**
     * Joined with an OLAP table, the FileTable used to keep that table's lock for the whole planning phase:
     * the listing ran while an internal table's READ lock was held.
     */
    @Test
    public void testFileTableJoinedWithOlapTableListsFilesOutsideTheLock() throws Exception {
        probeFileListing();
        getFragmentPlan("select * from test.t0 join test.file_tbl on t0.v1 = file_tbl.v1");
        Assertions.assertTrue(listCalls.get() > 0, "the probe never saw a file listing");
        Assertions.assertFalse(listedUnderLock.get(), "a FileTable listed its files under the meta lock");
    }

    /**
     * A resource-mapping Iceberg table is one object shared by every query, and a query writes to the table it
     * plans on (metrics reporter, partition caches, clearMetadata when it ends). The analyzer hands planning a
     * private copy, so none of that reaches the published object.
     */
    @Test
    public void testResourceMappingIcebergTableIsPlannedOnAPrivateCopy() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        org.apache.iceberg.Table nativeTable = Mockito.mock(org.apache.iceberg.Table.class);
        Schema schema = new Schema(Types.NestedField.optional(1, "k", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Mockito.when(nativeTable.schema()).thenReturn(schema);
        Mockito.when(nativeTable.spec()).thenReturn(spec);
        Mockito.when(nativeTable.specs()).thenReturn(Map.of(spec.specId(), spec));
        IcebergTable published = new IcebergTable(GlobalStateMgr.getCurrentState().getNextId(), "ice_rm", null,
                "iceberg_rm_resource", "remote_db", "remote_tbl", "", Lists.newArrayList(new Column("k", INT)),
                nativeTable, Maps.newHashMap());
        Assertions.assertFalse(published.isMetaLockTarget());
        db.registerTableUnlocked(published);
        try {
            QueryStatement stmt = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "select k from test.ice_rm", connectContext);
            Table planned = ((TableRelation) ((SelectRelation) stmt.getQueryRelation()).getRelation()).getTable();
            Assertions.assertInstanceOf(IcebergTable.class, planned);
            Assertions.assertNotSame(published, planned);
            Assertions.assertEquals(published.getId(), planned.getId());
            Assertions.assertEquals(published.getName(), planned.getName());
            Assertions.assertEquals(published.getCatalogName(), planned.getCatalogName());

            // What a query does to its table stays on the copy.
            ((IcebergTable) planned).clearMetadata();
            Assertions.assertSame(nativeTable, published.getNativeTable());
        } finally {
            db.unRegisterTableUnlocked(published);
        }
    }
}
