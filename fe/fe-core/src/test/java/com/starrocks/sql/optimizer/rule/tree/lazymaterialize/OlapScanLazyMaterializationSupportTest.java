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

package com.starrocks.sql.optimizer.rule.tree.lazymaterialize;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.VectorSearchOptions;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Focused unit tests for {@link OlapScanLazyMaterializationSupport#predicateUsedColumns}.
 *
 * The PR that introduced these tests narrowed the embedding-column eager-materialization
 * carve-out from {@code isEnableUseANN()} to {@code isUseIVFPQ()}: HNSW queries flow through
 * the global lazy-materialize rule, IVFPQ keeps the original eager path. These tests pin
 * that contract directly, independent of the rest of the planner pipeline.
 */
public class OlapScanLazyMaterializationSupportTest {

    private static OlapTable buildOlapTableWithVectorIndex(String vectorColumnName) {
        OlapTable table = new OlapTable();
        table.setId(1L);
        table.setName("t");
        Index vectorIndex = new Index(
                "vidx",
                Collections.singletonList(ColumnId.create(vectorColumnName)),
                IndexDef.IndexType.VECTOR,
                "");
        table.setIndexes(List.of(vectorIndex));
        return table;
    }

    private static PhysicalOlapScanOperator buildScan(OlapTable table,
                                                      Map<ColumnRefOperator, Column> colRefToMeta,
                                                      ScalarOperator predicate,
                                                      VectorSearchOptions opts) {
        return new PhysicalOlapScanOperator(
                table,
                colRefToMeta,
                null,
                Operator.DEFAULT_LIMIT,
                predicate,
                0L,
                List.of(),
                List.of(),
                List.of(),
                List.of(),
                null,
                false,
                opts);
    }

    private static ColumnRefOperator vectorColumnRef() {
        return new ColumnRefOperator(10, new ArrayType(FloatType.FLOAT), "v", false);
    }

    private static Column vectorColumnMeta() {
        return new Column("v", new ArrayType(FloatType.FLOAT), false);
    }

    private static ColumnRefOperator idColumnRef() {
        return new ColumnRefOperator(11, IntegerType.BIGINT, "id", false);
    }

    private static Column idColumnMeta() {
        return new Column("id", IntegerType.BIGINT, false);
    }

    private static Map<ColumnRefOperator, Column> twoColumnMeta(ColumnRefOperator vRef,
                                                                Column vMeta,
                                                                ColumnRefOperator idRef,
                                                                Column idMeta) {
        // LinkedHashMap so iteration order is deterministic if the rule ever relies on it.
        Map<ColumnRefOperator, Column> m = new LinkedHashMap<>();
        m.put(idRef, idMeta);
        m.put(vRef, vMeta);
        return m;
    }

    /**
     * HNSW (isEnableUseANN=true, isUseIVFPQ=false): the rule's carve-out is skipped,
     * so with no predicate, predicateUsedColumns must be empty. This is the CHANGED
     * behavior — before the PR, isEnableUseANN alone would have added v.
     */
    @Test
    public void testHnswDoesNotForceEmbeddingEager() {
        OlapTable table = buildOlapTableWithVectorIndex("v");
        ColumnRefOperator vRef = vectorColumnRef();
        ColumnRefOperator idRef = idColumnRef();
        Map<ColumnRefOperator, Column> meta = twoColumnMeta(vRef, vectorColumnMeta(), idRef, idColumnMeta());

        VectorSearchOptions hnsw = new VectorSearchOptions();
        hnsw.setEnableUseANN(true);
        hnsw.setUseIVFPQ(false);

        PhysicalOlapScanOperator scan = buildScan(table, meta, null, hnsw);
        ColumnRefSet result = new OlapScanLazyMaterializationSupport().predicateUsedColumns(scan);

        Assertions.assertTrue(result.isEmpty(),
                "HNSW with no predicate must not force the embedding column eager");
    }

    /**
     * IVFPQ: the carve-out still applies. The embedding column must be added so the
     * row-by-row approx_*_distance refine pass keeps v reaching the TopN.
     */
    @Test
    public void testIvfpqForcesEmbeddingEager() {
        OlapTable table = buildOlapTableWithVectorIndex("v");
        ColumnRefOperator vRef = vectorColumnRef();
        ColumnRefOperator idRef = idColumnRef();
        Map<ColumnRefOperator, Column> meta = twoColumnMeta(vRef, vectorColumnMeta(), idRef, idColumnMeta());

        VectorSearchOptions ivfpq = new VectorSearchOptions();
        ivfpq.setEnableUseANN(true);
        ivfpq.setUseIVFPQ(true);

        PhysicalOlapScanOperator scan = buildScan(table, meta, null, ivfpq);
        ColumnRefSet result = new OlapScanLazyMaterializationSupport().predicateUsedColumns(scan);

        Assertions.assertTrue(result.contains(vRef),
                "IVFPQ must include the embedding column in predicate-used");
        Assertions.assertFalse(result.contains(idRef),
                "Non-vector columns must not be force-included by the IVFPQ carve-out");
    }

    /**
     * No ANN (isEnableUseANN=false, isUseIVFPQ=false): regression guard. The rule must
     * remain a no-op for non-vector scans regardless of whether the table has a vector
     * index defined.
     */
    @Test
    public void testNoAnnIsNoOp() {
        OlapTable table = buildOlapTableWithVectorIndex("v");
        ColumnRefOperator vRef = vectorColumnRef();
        ColumnRefOperator idRef = idColumnRef();
        Map<ColumnRefOperator, Column> meta = twoColumnMeta(vRef, vectorColumnMeta(), idRef, idColumnMeta());

        VectorSearchOptions noAnn = new VectorSearchOptions();
        // both default to false; set explicitly for clarity
        noAnn.setEnableUseANN(false);
        noAnn.setUseIVFPQ(false);

        PhysicalOlapScanOperator scan = buildScan(table, meta, null, noAnn);
        ColumnRefSet result = new OlapScanLazyMaterializationSupport().predicateUsedColumns(scan);

        Assertions.assertTrue(result.isEmpty(), "Non-ANN scan must have no rule-injected columns");
    }

    /**
     * HNSW with a predicate referencing the embedding column: even though the rule's
     * IVFPQ carve-out is skipped, the predicate's own getUsedColumns must still seed v
     * into the result. Without this, a downstream lazy-mat pass could prune v out from
     * under the predicate and produce wrong results.
     */
    @Test
    public void testHnswPredicateOnEmbeddingStillIncludesIt() {
        OlapTable table = buildOlapTableWithVectorIndex("v");
        ColumnRefOperator vRef = vectorColumnRef();
        ColumnRefOperator idRef = idColumnRef();
        Map<ColumnRefOperator, Column> meta = twoColumnMeta(vRef, vectorColumnMeta(), idRef, idColumnMeta());

        VectorSearchOptions hnsw = new VectorSearchOptions();
        hnsw.setEnableUseANN(true);
        hnsw.setUseIVFPQ(false);

        // ColumnRefOperator is itself a ScalarOperator; using it as the predicate is the
        // smallest scalar tree whose getUsedColumns deterministically returns {v}.
        PhysicalOlapScanOperator scan = buildScan(table, meta, vRef, hnsw);
        ColumnRefSet result = new OlapScanLazyMaterializationSupport().predicateUsedColumns(scan);

        Assertions.assertTrue(result.contains(vRef),
                "Predicate referencing v must keep v in predicate-used even on HNSW");
        Assertions.assertFalse(result.contains(idRef),
                "Predicate on v alone must not pull unrelated columns in");
    }

    /**
     * IVFPQ when the matched vector column ref is NOT in the scan's colRefToColumnMetaMap
     * (e.g. some earlier rule already pruned it). The carve-out must remain a no-op for
     * that column rather than referencing a non-existent ColumnRefOperator id.
     */
    @Test
    public void testIvfpqSkipsWhenVectorColumnAlreadyPruned() {
        OlapTable table = buildOlapTableWithVectorIndex("v");
        ColumnRefOperator idRef = idColumnRef();
        Map<ColumnRefOperator, Column> meta = new LinkedHashMap<>();
        meta.put(idRef, idColumnMeta()); // v intentionally absent

        VectorSearchOptions ivfpq = new VectorSearchOptions();
        ivfpq.setEnableUseANN(true);
        ivfpq.setUseIVFPQ(true);

        PhysicalOlapScanOperator scan = buildScan(table, meta, null, ivfpq);
        ColumnRefSet result = new OlapScanLazyMaterializationSupport().predicateUsedColumns(scan);

        Assertions.assertTrue(result.isEmpty(),
                "If the embedding column is not in the scan output, the carve-out must add nothing");
    }
}
