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

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MaterializedViewRefreshType;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.sql.analyzer.AlterMVClauseAnalyzerVisitor;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.ReorderColumnsClause;
import com.starrocks.thrift.TStorageType;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit coverage for {@code AlterMVClauseAnalyzerVisitor#visitReorderColumnsClause}, the validation gate
 * for {@code ALTER MATERIALIZED VIEW <mv> ORDER BY (cols)}. Mirrors the lightweight, no-cluster fixture
 * style of {@link AddRollupOrderByAnalyzerTest} (the sibling gate for {@code ADD ROLLUP ... ORDER BY}):
 * a real {@link MaterializedView} / {@link LakeMaterializedView} is constructed directly with just
 * enough schema/distribution/keysType wiring to exercise each legality check, instead of standing up a
 * full shared-data test cluster.
 */
public class AlterMvOrderByAnalyzerTest {
    private static final long MV_ID = 10001L;
    private static final long DB_ID = 10000L;
    private static final long BASE_META_ID = 10002L;

    private static ReorderColumnsClause makeClause(List<String> orderBy) {
        return new ReorderColumnsClause(orderBy, null, null, com.starrocks.sql.parser.NodePosition.ZERO);
    }

    private static Column keyColumn(String name) {
        Column column = new Column(name, IntegerType.INT);
        column.setIsKey(true);
        return column;
    }

    private static Column valueColumn(String name, AggregateType aggregateType) {
        Column column = new Column(name, IntegerType.INT);
        column.setAggregationType(aggregateType, false);
        return column;
    }

    /**
     * Builds a real {@link MaterializedView} (or {@link LakeMaterializedView} when {@code cloudNative})
     * with a single base index registered under {@code BASE_META_ID}, so
     * {@code getIndexMetaIdToMeta()}, {@code getSchemaByIndexMetaId()}, {@code getBaseIndexMetaId()} and
     * {@code getKeysType()} all resolve to genuine values rather than mocks.
     */
    private static MaterializedView buildMv(boolean cloudNative, DistributionInfo distributionInfo,
                                             KeysType keysType, List<Column> columns) {
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.ASYNC);
        MaterializedView mv = cloudNative
                ? new LakeMaterializedView(MV_ID, DB_ID, "mv", columns, keysType, partitionInfo,
                        distributionInfo, refreshScheme)
                : new MaterializedView(MV_ID, DB_ID, "mv", columns, keysType, partitionInfo,
                        distributionInfo, refreshScheme);
        mv.setIndexMeta(BASE_META_ID, "mv", columns, 0, 0, (short) columns.size(), TStorageType.COLUMN, keysType);
        mv.setBaseIndexMetaId(BASE_META_ID);
        return mv;
    }

    private static MaterializedView dupRangeMv() {
        List<Column> columns = List.of(keyColumn("k1"), keyColumn("k2"), valueColumn("v1", AggregateType.NONE));
        return buildMv(true, new RangeDistributionInfo(), KeysType.DUP_KEYS, columns);
    }

    @Test
    public void testAlterMvOrderByRangeAccepted() {
        MaterializedView mv = dupRangeMv();
        // Reorder to (k2, k1): a real reorder of the default (k1, k2) leading-key sort key, not a no-op.
        assertDoesNotThrow(() ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("k2", "k1"))));
    }

    @Test
    public void testAlterMvOrderBySyncRejected() {
        // A synchronous MV has no refresh Task for the rewrite to suspend/drain around -- reject
        // explicitly rather than silently no-op the quiescence hooks. Checked before the shared-data gate.
        List<Column> columns = List.of(keyColumn("k1"), keyColumn("k2"), valueColumn("v1", AggregateType.NONE));
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        MaterializedView.MvRefreshScheme refreshScheme = new MaterializedView.MvRefreshScheme();
        refreshScheme.setType(MaterializedViewRefreshType.SYNC);
        MaterializedView mv = new LakeMaterializedView(MV_ID, DB_ID, "mv", columns, KeysType.DUP_KEYS,
                partitionInfo, new RangeDistributionInfo(), refreshScheme);
        mv.setIndexMeta(BASE_META_ID, "mv", columns, 0, 0, (short) columns.size(), TStorageType.COLUMN, KeysType.DUP_KEYS);
        mv.setBaseIndexMetaId(BASE_META_ID);
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("k2", "k1"))));
        assertTrue(ex.getMessage().contains("only supported on asynchronous materialized views"),
                "expected asynchronous-only message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderBySharedNothingRejected() {
        // A shared-nothing (non-cloud-native) MV must be rejected even though it is range-distributed --
        // the shared-data gate is checked before the range-distribution gate.
        List<Column> columns = List.of(keyColumn("k1"), keyColumn("k2"), valueColumn("v1", AggregateType.NONE));
        MaterializedView mv = buildMv(false, new RangeDistributionInfo(), KeysType.DUP_KEYS, columns);
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("k2", "k1"))));
        assertTrue(ex.getMessage().contains("only supported on shared-data"),
                "expected shared-data-only message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderByHashRejected() {
        List<Column> columns = List.of(keyColumn("k1"), keyColumn("k2"), valueColumn("v1", AggregateType.NONE));
        DistributionInfo hash = new HashDistributionInfo(1, List.of(columns.get(0)));
        MaterializedView mv = buildMv(true, hash, KeysType.DUP_KEYS, columns);
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("k2", "k1"))));
        assertTrue(ex.getMessage().contains("only supported on range-distributed materialized views"),
                "expected range-distributed-only message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderByUnknownColumnRejected() {
        MaterializedView mv = dupRangeMv();
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("no_such_col"))));
        assertTrue(ex.getMessage().contains("does not exist"),
                "expected column-not-found message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderByDuplicateColumnRejected() {
        MaterializedView mv = dupRangeMv();
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("k1", "k1"))));
        assertTrue(ex.getMessage().contains("Duplicate ORDER BY column"),
                "expected duplicate-column message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderBySameAsCurrentRejected() {
        // No explicit mv_sort_keys property set -> current sort key falls back to the leading key
        // columns (k1, k2). ORDER BY (K1, K2) is the same sort key case-insensitively -> reject.
        MaterializedView mv = dupRangeMv();
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("K1", "k2"))));
        assertTrue(ex.getMessage().contains("already the sort key"),
                "expected no-op-reorder message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderByAggPermutationAccepted() {
        List<Column> columns = List.of(keyColumn("k1"), keyColumn("k2"), valueColumn("v1", AggregateType.SUM));
        MaterializedView mv = buildMv(true, new RangeDistributionInfo(), KeysType.AGG_KEYS, columns);
        // (k2, k1) is a permutation of the full key-column set {k1, k2} -> accepted.
        assertDoesNotThrow(() ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("k2", "k1"))));
    }

    @Test
    public void testAlterMvOrderByAggNonKeySetRejected() {
        List<Column> columns = List.of(keyColumn("k1"), keyColumn("k2"), valueColumn("v1", AggregateType.SUM));
        MaterializedView mv = buildMv(true, new RangeDistributionInfo(), KeysType.AGG_KEYS, columns);
        // (k1) alone is a strict subset of the key-column set {k1, k2} -> rejected.
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("k1"))));
        assertTrue(ex.getMessage().contains("must be exactly the key columns"),
                "expected key-set-mismatch message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderByPrimaryKeysRejected() {
        List<Column> columns = List.of(keyColumn("k1"), keyColumn("k2"), valueColumn("v1", AggregateType.NONE));
        MaterializedView mv = buildMv(true, new RangeDistributionInfo(), KeysType.PRIMARY_KEYS, columns);
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("k2", "k1"))));
        assertTrue(ex.getMessage().contains("not supported for"),
                "expected unsupported-keysType message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderByFromRollupRejected() {
        // The MV path reuses the rollup reorder-columns grammar, but an MV has no rollup to target.
        MaterializedView mv = dupRangeMv();
        ReorderColumnsClause clause = new ReorderColumnsClause(List.of("k2", "k1"), "some_rollup", null,
                com.starrocks.sql.parser.NodePosition.ZERO);
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, clause));
        assertTrue(ex.getMessage().contains("does not support 'FROM <rollup>'"),
                "expected FROM-rollup rejection message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderByPropertiesRejected() {
        MaterializedView mv = dupRangeMv();
        ReorderColumnsClause clause = new ReorderColumnsClause(List.of("k2", "k1"), null,
                Map.of("some_property", "some_value"), com.starrocks.sql.parser.NodePosition.ZERO);
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, clause));
        assertTrue(ex.getMessage().contains("does not support PROPERTIES"),
                "expected PROPERTIES rejection message, got: " + ex.getMessage());
    }

    @Test
    public void testAlterMvOrderByMultiIndexRejected() {
        MaterializedView mv = dupRangeMv();
        // Register a second index meta (e.g. a sync/rollup index) beyond the base index.
        List<Column> extraSchema = mv.getSchemaByIndexMetaId(BASE_META_ID).subList(0, 1);
        mv.setIndexMeta(BASE_META_ID + 1, "extra_idx", extraSchema, 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        SemanticException ex = assertThrows(SemanticException.class, () ->
                new AlterMVClauseAnalyzerVisitor(mv).analyze(null, makeClause(List.of("k2", "k1"))));
        assertTrue(ex.getMessage().contains("carries additional"),
                "expected multi-index rejection message, got: " + ex.getMessage());
    }
}
