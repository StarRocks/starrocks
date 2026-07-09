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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.connector.iceberg.IcebergTableMORParams;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.sql.IcebergPlannerUtils;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.apache.iceberg.BaseTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Verifies that the Iceberg DML target-scan lookup pulls the conflict-detection filter and
 * base snapshot id from the scan node that actually feeds the DML (the one producing the
 * output row-locator slots), not from an arbitrary same-table scan elsewhere in the plan.
 */
public class IcebergDmlTargetScanTest extends PlanTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    // One shared native table: within a query, every resolution of the target reuses the same
    // native Iceberg table (query-level IcebergMetadata cache), so all wrappers share it and
    // IcebergTable.equals() (catalog + db + table identifier incl. UUID) sees one identity.
    private static final org.apache.iceberg.Table SHARED_NATIVE_TABLE =
            Mockito.mock(BaseTable.class, Mockito.RETURNS_DEEP_STUBS);

    private static IcebergTable icebergTable(long syntheticId) {
        // Same catalog identity, distinct synthetic ids: models the target table and a
        // same-table scan resolved independently (CONNECTOR_ID_GENERATOR mints fresh ids).
        return new IcebergTable(syntheticId, "t0_v2", "iceberg0", "resource", "unpartitioned_db",
                "t0_v2", "", Lists.newArrayList(), SHARED_NATIVE_TABLE, Maps.newHashMap());
    }

    /**
     * Deterministic guard for the scan order the SQL-level tests cannot force: when several
     * scans share the target's catalog identity and the NON-target scan comes first in
     * {@code getScanNodes()} (e.g. after a semi-join commutation), the lookup must still pick
     * the scan producing the output row-locator slots.
     */
    @Test
    public void testFindScanNodeForBindsRowLocatorProducingScan() throws Exception {
        ExecPlan execPlan = new ExecPlan();
        DescriptorTable descTbl = execPlan.getDescTbl();

        // Subquery scan over the same physical table: plain data slots, no row locators.
        TupleDescriptor subqueryTuple = descTbl.createTupleDescriptor("same-table-subquery-scan");
        subqueryTuple.setTable(icebergTable(2L));
        SlotDescriptor idSlot = descTbl.addSlotDescriptor(subqueryTuple);
        idSlot.setColumn(new Column("id", IntegerType.INT));
        idSlot.setIsMaterialized(true);
        subqueryTuple.computeMemLayout();

        // Target scan: produces the _file/_pos slots consumed by the DML sink.
        TupleDescriptor targetTuple = descTbl.createTupleDescriptor("dml-target-scan");
        targetTuple.setTable(icebergTable(3L));
        SlotDescriptor fileSlot = descTbl.addSlotDescriptor(targetTuple);
        fileSlot.setColumn(new Column(IcebergTable.FILE_PATH, VarcharType.VARCHAR));
        fileSlot.setIsMaterialized(true);
        SlotDescriptor posSlot = descTbl.addSlotDescriptor(targetTuple);
        posSlot.setColumn(new Column(IcebergTable.ROW_POSITION, IntegerType.BIGINT));
        posSlot.setIsMaterialized(true);
        targetTuple.computeMemLayout();

        IcebergScanNode subqueryScan = new IcebergScanNode(new PlanNodeId(0), subqueryTuple, "IcebergScanNode",
                IcebergTableMORParams.EMPTY, IcebergMORParams.EMPTY, null);
        IcebergScanNode targetScan = new IcebergScanNode(new PlanNodeId(1), targetTuple, "IcebergScanNode",
                IcebergTableMORParams.EMPTY, IcebergMORParams.EMPTY, null);

        // Adverse order: the non-target scan first.
        execPlan.getScanNodes().add(subqueryScan);
        execPlan.getScanNodes().add(targetScan);
        execPlan.getOutputExprs().add(new SlotRef(fileSlot));
        execPlan.getOutputExprs().add(new SlotRef(posSlot));

        Method method = IcebergPlannerUtils.class.getDeclaredMethod(
                "findScanNodeFor", ExecPlan.class, IcebergTable.class);
        method.setAccessible(true);
        Object found = method.invoke(null, execPlan, icebergTable(1L));
        assertSame(targetScan, found,
                "lookup must bind to the scan producing the output row-locator slots, not the first "
                        + "same-identity scan in plan order");
    }
}
