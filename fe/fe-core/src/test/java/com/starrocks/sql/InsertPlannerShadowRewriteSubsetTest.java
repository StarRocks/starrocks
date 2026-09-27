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

package com.starrocks.sql;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TStorageType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Part 1 of the issue #76521 fix: the internal shadow-rewrite INSERT that materializes a column-subset
 * range rollup writes ONLY the target rollup index. Base columns the target index omits are carried in
 * the sink tuple solely to satisfy the sink's base distribution/partition plumbing (BE validates their
 * presence); they are never persisted nor used for routing. {@link InsertPlanner} must therefore relax
 * the nullability of those omitted-slot columns, so an omitted NOT-NULL base column that gets
 * default-filled with NULL is not rejected by the BE non-nullable data validation.
 *
 * <p>The relaxation is distribution-agnostic (it keys off the shadow-rewrite flags + the target write
 * index's column set), so this planner test uses a plain hash-distributed DUP table with a real subset
 * rollup index registered in every physical partition; the range-distribution runtime is validated
 * end-to-end on a cluster.
 */
public class InsertPlannerShadowRewriteSubsetTest extends PlanTestBase {

    @BeforeAll
    public static void beforeAll() throws Exception {
        PlanTestBase.beforeClass();
    }

    @AfterAll
    public static void afterAll() {
        PlanTestBase.afterClass();
    }

    /** Registers a NORMAL rollup index carrying {@code schema} in every physical partition of {@code table}. */
    private static long registerSubsetRollupIndex(OlapTable table, String name, List<Column> schema) {
        long metaId = GlobalStateMgr.getCurrentState().getNextId();
        table.setIndexMeta(metaId, name, schema, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, List.of(0));
        for (PhysicalPartition pp : table.getPhysicalPartitions()) {
            long physId = GlobalStateMgr.getCurrentState().getNextId();
            pp.createRollupIndex(new MaterializedIndex(physId, metaId,
                    MaterializedIndex.IndexState.NORMAL, PhysicalPartition.INVALID_SHARD_GROUP_ID));
        }
        return metaId;
    }

    /** Finds the sink (output) tuple: the one carrying a slot for the omitted base column {@code k1}. */
    private static TupleDescriptor findSinkTuple(ExecPlan plan, String omittedColumn) {
        for (TupleDescriptor tuple : plan.getDescTbl().getTupleDescs()) {
            for (SlotDescriptor slot : tuple.getSlots()) {
                if (slot.getColumn() != null && omittedColumn.equalsIgnoreCase(slot.getColumn().getName())) {
                    return tuple;
                }
            }
        }
        return null;
    }

    private static SlotDescriptor slotOf(TupleDescriptor tuple, String columnName) {
        for (SlotDescriptor slot : tuple.getSlots()) {
            if (slot.getColumn() != null && columnName.equalsIgnoreCase(slot.getColumn().getName())) {
                return slot;
            }
        }
        return null;
    }

    private static ExecPlan planShadowRewriteSubsetInsert(OlapTable table, String sql, long targetWriteIndexId)
            throws Exception {
        List<StatementBase> stmts = SqlParser.parse(sql, new SessionVariable());
        InsertStmt insertStmt = (InsertStmt) stmts.get(0);
        // The rewrite job sets these BEFORE analysis; InsertAnalyzer's shadow-rewrite relaxation depends on them.
        insertStmt.setShadowRewrite(true);
        insertStmt.setTargetWriteIndexId(targetWriteIndexId);
        Analyzer.analyze(insertStmt, starRocksAssert.getCtx());
        AnalyzerUtils.collectAllDatabase(starRocksAssert.getCtx(), insertStmt);

        PlannerMetaLocker locker = new PlannerMetaLocker(starRocksAssert.getCtx(), insertStmt);
        StatementPlanner.lock(locker);
        try {
            return new InsertPlanner(locker, true).plan(insertStmt, starRocksAssert.getCtx());
        } finally {
            StatementPlanner.unLock(locker);
        }
    }

    @Test
    public void testShadowRewriteSubsetRelaxesOmittedSlotNullability() throws Exception {
        starRocksAssert.withTable("create table d_shadow_subset (k1 int not null, k2 int not null, v int)\n"
                + "duplicate key(k1) distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1')");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState()
                .getLocalMetastore().getTable("test", "d_shadow_subset");
        List<Column> base = table.getSchemaByIndexMetaId(table.getBaseIndexMetaId());
        // Subset rollup index = [k2, v] (omits the NOT-NULL base key k1).
        List<Column> subset = List.of(base.get(1), base.get(2));
        long shadowMetaId = registerSubsetRollupIndex(table, "r_sub", subset);

        ExecPlan plan = planShadowRewriteSubsetInsert(table,
                "insert into d_shadow_subset (k2, v) select k2, v from d_shadow_subset", shadowMetaId);

        TupleDescriptor sinkTuple = findSinkTuple(plan, "k1");
        Assertions.assertNotNull(sinkTuple, "omitted base column k1 must still be present in the sink tuple");

        SlotDescriptor k1Slot = slotOf(sinkTuple, "k1");
        SlotDescriptor k2Slot = slotOf(sinkTuple, "k2");
        SlotDescriptor vSlot = slotOf(sinkTuple, "v");
        Assertions.assertNotNull(k1Slot);
        Assertions.assertNotNull(k2Slot);
        Assertions.assertNotNull(vSlot);

        // k1 is omitted from the target index -> its slot nullability is relaxed so its NULL fill passes
        // the BE non-nullable validation, even though the base column is declared NOT NULL.
        Assertions.assertTrue(k1Slot.getIsNullable(),
                "omitted NOT-NULL base column k1 must have its slot relaxed to nullable");
        // k2 is in the target index and declared NOT NULL -> keeps its declared (non-null) nullability.
        Assertions.assertFalse(k2Slot.getIsNullable(),
                "target-index NOT-NULL column k2 must keep its NOT-NULL slot");
        // v is in the target index and nullable -> nullable.
        Assertions.assertTrue(vSlot.getIsNullable(), "nullable target-index column v stays nullable");

        // k1 is genuinely absent from the target write index's persisted column set.
        List<String> targetIndexColumns = table.getIndexMetaByMetaId(shadowMetaId).getSchema()
                .stream().map(Column::getName).toList();
        Assertions.assertFalse(targetIndexColumns.contains("k1"), "k1 must not be in the target rollup index");
    }
}
