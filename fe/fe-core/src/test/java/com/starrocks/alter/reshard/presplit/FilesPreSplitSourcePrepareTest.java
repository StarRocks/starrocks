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

package com.starrocks.alter.reshard.presplit;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.TableFunctionTable;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link FilesPreSplitSource#prepare} for the customer statement, whose partition column is fed by a
 * literal ({@code '20260917' AS dt}): the wiring that carries the constant into the scan context,
 * and the per-rollup sort-key gate. The column mapping itself is covered by
 * {@link InsertSelectSourceColumnsTest}; FILES() schema inference is stubbed out.
 */
public class FilesPreSplitSourcePrepareTest {

    private static final long BASE_INDEX_META_ID = 1L;
    private static final long ROLLUP_INDEX_META_ID = 2L;

    private static final Column EXP_ID = new Column("exp_id", IntegerType.BIGINT);
    private static final Column GRP_ID = new Column("grp_id", IntegerType.BIGINT);
    private static final Column BUCKET_ID = new Column("bucket_id", IntegerType.BIGINT);
    private static final Column DT = new Column("dt", DateType.DATE);
    private static final Column TS = new Column("ts", DateType.DATETIME);

    @Test
    public void literalFedPartitionColumnReachesTheScanContextAsAConstant() {
        PreSplitFlow.Prepared prepared = prepareCustomerStatement(/*rollupSortKey*/ null);

        Assertions.assertNotNull(prepared, "the customer statement must be admitted");
        InsertFromFilesScanContext scanContext = (InsertFromFilesScanContext) prepared.scanContext();
        Assertions.assertEquals(Map.of("dt", "'20260917'"), scanContext.targetToConstantSql());
        // dt is not in the file, so no FILES column may be sampled for it.
        Assertions.assertFalse(scanContext.targetToSourceColumnNames().containsKey("dt"),
                "dt must not be projected from FILES: " + scanContext.targetToSourceColumnNames());
        Assertions.assertEquals(List.of(DT), prepared.partitionColumns());
        Assertions.assertEquals(List.of(EXP_ID), prepared.sortKeyColumns());
    }

    @Test
    public void rollupWhoseKeyIsOnlyTheLiteralIsDeclined() {
        // The base key (exp_id) passes inside resolve(); a rollup keyed only on the literal dt is
        // degenerate and must decline the whole load.
        Assertions.assertNull(prepareCustomerStatement(List.of(DT)));
    }

    @Test
    public void rollupWhoseKeyMixesTheLiteralWithAFileColumnIsAdmitted() {
        PreSplitFlow.Prepared prepared = prepareCustomerStatement(List.of(DT, EXP_ID));

        Assertions.assertNotNull(prepared);
        Assertions.assertEquals(1, prepared.secondaryIndexSpecs().size());
    }

    /**
     * Runs prepare() on the customer statement against target (exp_id, grp_id, bucket_id, dt DATE),
     * ORDER BY (exp_id), PARTITION BY (dt), with one rollup keyed on {@code rollupSortKey} when it is
     * non-null. The FILES schema is (exp_id, grp_id, bucket_id): dt comes from the directory name.
     */
    @Test
    public void literalFedPartitionColumnIsNotAttributedAsAMissingColumn() {
        // THE REGRESSION GUARD for the customer statement. dt is fed by a literal, not by a FILES
        // column, so a presence check written against targetToSource alone would decline it AND
        // label it source_missing_sampled_column. It must do neither.
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.SOURCE_MISSING_SAMPLED_COLUMN.name().toLowerCase();
            long before = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue();
            Assertions.assertNotNull(prepareCustomerStatement(/*rollupSortKey*/ null),
                    "a literal-fed partition column must still pre-split");
            Assertions.assertEquals(before,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "a literal-fed column is fed; it must not be recorded as a missing column");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void allLiteralRollupKeyIsDeclinedButNotAttributedAsAMissingColumn() {
        // Every column IS fed, so this is a degenerate key rather than a missing column: decline
        // without a reason rather than with the wrong one.
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.SOURCE_MISSING_SAMPLED_COLUMN.name().toLowerCase();
            long before = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue();
            Assertions.assertNull(prepareCustomerStatement(List.of(DT)));
            Assertions.assertEquals(before,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "an all-literal key is degenerate, not a missing column");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void trulyUnfedRollupKeyIsDeclinedAndAttributed() {
        // A rollup key naming a column that is in NEITHER map -- no FILES column, no literal. This is
        // the one shape that earns SOURCE_MISSING_SAMPLED_COLUMN.
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.SOURCE_MISSING_SAMPLED_COLUMN.name().toLowerCase();
            String computed = SkipReason.UNSUPPORTED_SAMPLED_PROJECTION.name().toLowerCase();
            long before = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue();
            long computedBefore = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(computed).getValue();
            Assertions.assertNull(prepareCustomerStatement(List.of(new Column("ghost", IntegerType.BIGINT))));
            Assertions.assertEquals(before + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "an unfed sampled column must be recorded as source_missing_sampled_column");
            Assertions.assertEquals(computedBefore,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(computed).getValue().longValue());
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void omittedPartitionColumnRemainsAMissingSampledSourceColumn() {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String missing = SkipReason.SOURCE_MISSING_SAMPLED_COLUMN.name().toLowerCase();
            String computed = SkipReason.UNSUPPORTED_SAMPLED_PROJECTION.name().toLowerCase();
            long missingBefore = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(missing).getValue();
            long computedBefore = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(computed).getValue();
            Assertions.assertNull(prepareCustomerStatement(/*rollupSortKey*/ null, List.of(EXP_ID),
                    /*dtProjection*/ null, List.of(EXP_ID, GRP_ID, BUCKET_ID)));
            Assertions.assertEquals(missingBefore + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(missing).getValue().longValue());
            Assertions.assertEquals(computedBefore,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(computed).getValue().longValue());
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void unsafeComputedPartitionColumnHasItsOwnSkipReason() {
        // from_unixtime reads the session time zone, which the ROOT sampling context does not share.
        assertComputedProjectionSkip(/*rollupSortKey*/ null, List.of(EXP_ID),
                "grp_id", "from_unixtime(exp_id) AS dt", List.of(EXP_ID, GRP_ID, BUCKET_ID));
    }

    @Test
    public void unsafeComputedBaseSortKeyHasItsOwnSkipReason() {
        assertComputedProjectionSkip(/*rollupSortKey*/ null, List.of(DT),
                "grp_id", "from_unixtime(exp_id) AS dt", List.of(EXP_ID, GRP_ID, BUCKET_ID));
    }

    @Test
    public void unsafeComputedRollupSortKeyHasItsOwnSkipReason() {
        assertComputedProjectionSkip(List.of(GRP_ID), List.of(EXP_ID),
                "concat(exp_id, current_user()) AS grp_id", "'20260917' AS dt", List.of(EXP_ID, BUCKET_ID));
    }

    @Test
    public void computedPartitionColumnReachesTheScanContextAsAnExpression() {
        PreSplitFlow.Prepared prepared = assertAdmittedWithoutSkip(/*rollupSortKey*/ null, List.of(EXP_ID),
                "grp_id", "date_trunc('day', ts) AS dt", List.of(EXP_ID, GRP_ID, BUCKET_ID, TS));

        InsertFromFilesScanContext scanContext = (InsertFromFilesScanContext) prepared.scanContext();
        Assertions.assertEquals(Map.of("dt", "date_trunc('day', `ts`)"), scanContext.targetToExpressionSql());
        Assertions.assertFalse(scanContext.targetToSourceColumnNames().containsKey("dt"));
        Assertions.assertTrue(scanContext.targetToConstantSql().isEmpty());
        Assertions.assertEquals(List.of(DT), prepared.partitionColumns());
    }

    @Test
    public void computedBaseSortKeyIsAdmitted() {
        PreSplitFlow.Prepared prepared = assertAdmittedWithoutSkip(/*rollupSortKey*/ null, List.of(DT, EXP_ID),
                "grp_id", "CAST(ts AS DATE) AS dt", List.of(EXP_ID, GRP_ID, BUCKET_ID, TS));

        Assertions.assertEquals(Map.of("dt", "CAST(`ts` AS DATE)"),
                ((InsertFromFilesScanContext) prepared.scanContext()).targetToExpressionSql());
    }

    @Test
    public void computedRollupSortKeyIsAdmitted() {
        PreSplitFlow.Prepared prepared = assertAdmittedWithoutSkip(List.of(GRP_ID), List.of(EXP_ID),
                "exp_id + 1 AS grp_id", "'20260917' AS dt", List.of(EXP_ID, BUCKET_ID));

        Assertions.assertEquals(1, prepared.secondaryIndexSpecs().size());
        InsertFromFilesScanContext scanContext = (InsertFromFilesScanContext) prepared.scanContext();
        Assertions.assertEquals(Map.of("grp_id", "`exp_id` + 1"), scanContext.targetToExpressionSql());
        Assertions.assertEquals(Map.of("dt", "'20260917'"), scanContext.targetToConstantSql());
    }

    @Test
    public void byPositionComputedPartitionColumnIsAdmitted() {
        // No alias: by position the fourth output is dt whatever it is called.
        PreSplitFlow.Prepared prepared = prepareStatement(
                "INSERT INTO t SELECT exp_id, grp_id, bucket_id, date_trunc('day', ts) "
                        + "FROM FILES(\"path\" = \"s3://b/*\", \"format\" = \"parquet\")",
                /*rollupSortKey*/ null, List.of(EXP_ID), List.of(EXP_ID, GRP_ID, BUCKET_ID, TS));

        Assertions.assertNotNull(prepared);
        Assertions.assertEquals(Map.of("dt", "date_trunc('day', `ts`)"),
                ((InsertFromFilesScanContext) prepared.scanContext()).targetToExpressionSql());
    }

    private static PreSplitFlow.Prepared assertAdmittedWithoutSkip(
            List<Column> rollupSortKey, List<Column> baseSortKey, String grpProjection, String dtProjection,
            List<Column> sourceColumns) {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String computed = SkipReason.UNSUPPORTED_SAMPLED_PROJECTION.name().toLowerCase();
            String missing = SkipReason.SOURCE_MISSING_SAMPLED_COLUMN.name().toLowerCase();
            long computedBefore = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(computed).getValue();
            long missingBefore = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(missing).getValue();
            PreSplitFlow.Prepared prepared = prepareCustomerStatement(rollupSortKey, baseSortKey,
                    grpProjection, dtProjection, sourceColumns);
            Assertions.assertNotNull(prepared, "a safe computed key must be admitted");
            Assertions.assertEquals(computedBefore,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(computed).getValue().longValue());
            Assertions.assertEquals(missingBefore,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(missing).getValue().longValue());
            return prepared;
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    private static void assertComputedProjectionSkip(List<Column> rollupSortKey, List<Column> baseSortKey,
                                                     String grpProjection, String dtProjection,
                                                     List<Column> sourceColumns) {
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String computed = SkipReason.UNSUPPORTED_SAMPLED_PROJECTION.name().toLowerCase();
            String missing = SkipReason.SOURCE_MISSING_SAMPLED_COLUMN.name().toLowerCase();
            long computedBefore = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(computed).getValue();
            long missingBefore = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED
                    .getMetric(missing).getValue();
            Assertions.assertNull(prepareCustomerStatement(rollupSortKey, baseSortKey,
                    grpProjection, dtProjection, sourceColumns));
            Assertions.assertEquals(computedBefore + 1L,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(computed).getValue().longValue());
            Assertions.assertEquals(missingBefore,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(missing).getValue().longValue());
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    @Test
    public void allLiteralBaseSortKeyIsDeclinedButNotAttributedAsAMissingColumn() {
        // dt IS fed -- by a literal -- so nothing is missing; the base sort key is simply degenerate,
        // one value for every row, and no cut can separate them. Decline without a reason rather than
        // with the wrong one. This is the ONLY shape that reaches the sortKeySampleable gate: an unfed
        // column is caught earlier and attributed, which is why that gate needs its own case.
        boolean savedHasInit = MetricRepo.hasInit;
        MetricRepo.hasInit = true;
        try {
            String label = SkipReason.SOURCE_MISSING_SAMPLED_COLUMN.name().toLowerCase();
            long before = MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue();
            Assertions.assertNull(prepareCustomerStatement(/*rollupSortKey*/ null, List.of(DT)),
                    "an all-literal base sort key is degenerate and must be declined");
            Assertions.assertEquals(before,
                    MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(label).getValue().longValue(),
                    "an all-literal base sort key is degenerate, not a missing column");
        } finally {
            MetricRepo.hasInit = savedHasInit;
        }
    }

    private static PreSplitFlow.Prepared prepareCustomerStatement(List<Column> rollupSortKey) {
        return prepareCustomerStatement(rollupSortKey, List.of(EXP_ID));
    }

    private static PreSplitFlow.Prepared prepareCustomerStatement(
            List<Column> rollupSortKey, List<Column> baseSortKey) {
        return prepareCustomerStatement(rollupSortKey, baseSortKey, "'20260917' AS dt",
                List.of(EXP_ID, GRP_ID, BUCKET_ID));
    }

    private static PreSplitFlow.Prepared prepareCustomerStatement(
            List<Column> rollupSortKey, List<Column> baseSortKey, String dtProjection,
            List<Column> sourceColumns) {
        return prepareCustomerStatement(rollupSortKey, baseSortKey, "grp_id", dtProjection, sourceColumns);
    }

    private static PreSplitFlow.Prepared prepareCustomerStatement(
            List<Column> rollupSortKey, List<Column> baseSortKey, String grpProjection, String dtProjection,
            List<Column> sourceColumns) {
        return prepareStatement(
                "INSERT INTO t BY NAME SELECT exp_id, " + grpProjection + ", bucket_id"
                        + (dtProjection == null ? "" : ", " + dtProjection) + " "
                        + "FROM FILES(\"path\" = \"s3://b/dt=20260917/*\", \"format\" = \"parquet\")",
                rollupSortKey, baseSortKey, sourceColumns);
    }

    private static PreSplitFlow.Prepared prepareStatement(
            String sql, List<Column> rollupSortKey, List<Column> baseSortKey, List<Column> sourceColumns) {
        InsertStmt stmt = (InsertStmt) SqlParser.parseSingleStatement(sql, SqlModeHelper.MODE_DEFAULT);
        SelectRelation selectRelation = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        FileTableFunctionRelation filesRelation = (FileTableFunctionRelation) selectRelation.getRelation();
        TableFunctionTable filesTable = mock(TableFunctionTable.class);
        when(filesTable.getFullVisibleSchema()).thenReturn(sourceColumns);
        when(filesTable.loadFileList()).thenReturn(List.of());
        filesRelation.setTable(filesTable);

        OlapTable target = mock(OlapTable.class);
        when(target.getBaseSchemaWithoutGeneratedColumn()).thenReturn(List.of(EXP_ID, GRP_ID, BUCKET_ID, DT));
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.getPartitionColumns(any())).thenReturn(List.of(DT));
        when(target.getPartitionInfo()).thenReturn(partitionInfo);
        when(target.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndexMeta baseMeta = mock(MaterializedIndexMeta.class);
        when(baseMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndexMeta rollupMeta = mock(MaterializedIndexMeta.class);
        when(rollupMeta.getIndexMetaId()).thenReturn(ROLLUP_INDEX_META_ID);
        when(target.getVisibleIndexMetas())
                .thenReturn(rollupSortKey == null ? List.of(baseMeta) : List.of(baseMeta, rollupMeta));

        ConnectContext context = mock(ConnectContext.class);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(sessionVariable.getTimeZone()).thenReturn("UTC");
        when(context.getSessionVariable()).thenReturn(sessionVariable);

        try (MockedConstruction<QueryAnalyzer> ignoredAnalyzer = Mockito.mockConstruction(QueryAnalyzer.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target)).thenReturn(baseSortKey);
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target, ROLLUP_INDEX_META_ID))
                    .thenReturn(rollupSortKey);
            return new FilesPreSplitSource().prepare(stmt, selectRelation, target, /*database*/ null, context);
        }
    }
}
