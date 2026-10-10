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
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.ast.FileTableFunctionRelation;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.VarcharType;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.ACTIVITY_DATE;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.MONTH_SQL;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.activityMonth;
import static com.starrocks.alter.reshard.presplit.PresplitTestSupport.stubGeneratedSchema;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link FilesPreSplitSource#prepare} for the customer statement, whose partition column is fed by a
 * literal ({@code '20260917' AS dt}): the wiring that carries the constant into the scan context.
 * The column mapping itself is covered by
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
    private static final Column ACTIVITY_MONTH = activityMonth();
    private static final Column PAYLOAD = new Column("payload",
            new StructType(List.of(new StructField("name", 0, VarcharType.VARCHAR, null)), true), true);

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

    @Test
    public void scanContextCarriesTheUsersSessionSemantics() {
        SessionVariable userSession = new SessionVariable();
        userSession.setSqlMode(SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_PIPES_AS_CONCAT);
        userSession.setTimeZone("Asia/Shanghai");
        userSession.setCboEqBaseType("varchar");

        PreSplitFlow.Prepared prepared = prepareStatement("INSERT INTO t BY NAME SELECT exp_id, grp_id, bucket_id, "
                        + "'20260917' AS dt FROM FILES(\"path\" = \"s3://b/dt=20260917/*\", \"format\" = \"parquet\")",
                /*rollupSortKey*/ null, List.of(EXP_ID), List.of(EXP_ID, GRP_ID, BUCKET_ID), userSession);

        Assertions.assertNotNull(prepared);
        SampleSessionSemantics carried = ((InsertFromFilesScanContext) prepared.scanContext()).sessionSemantics();
        Assertions.assertEquals(SampleSessionSemantics.capture(userSession), carried);
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

    @Test
    public void generatedPartitionColumnIsComputedFromTheFileColumnItReads() {
        long missingBefore = eligibilitySkips(SkipReason.SOURCE_MISSING_SAMPLED_COLUMN);
        long generatedBefore = eligibilitySkips(SkipReason.UNSUPPORTED_GENERATED_COLUMN);

        PreSplitFlow.Prepared prepared = prepareOverwriteStatement("exp_id, activity_date",
                List.of(EXP_ID, ACTIVITY_DATE), ACTIVITY_MONTH, List.of(EXP_ID, ACTIVITY_DATE), /*analyzed*/ false,
                ACTIVITY_MONTH);

        Assertions.assertNotNull(prepared, "a generated partition column over a FILES column must be sampled");
        Assertions.assertEquals(Map.of("activity_date_month", MONTH_SQL),
                ((InsertFromFilesScanContext) prepared.scanContext()).targetToExpressionSql());
        Assertions.assertEquals(missingBefore, eligibilitySkips(SkipReason.SOURCE_MISSING_SAMPLED_COLUMN));
        Assertions.assertEquals(generatedBefore, eligibilitySkips(SkipReason.UNSUPPORTED_GENERATED_COLUMN));
    }

    @Test
    public void generatedPartitionColumnWhoseInputTheFileLacksHasItsOwnSkipReason() {
        // The column is computed by the table, not missing from the file: it must not be reported as
        // source_missing_sampled_column.
        long missingBefore = eligibilitySkips(SkipReason.SOURCE_MISSING_SAMPLED_COLUMN);
        long generatedBefore = eligibilitySkips(SkipReason.UNSUPPORTED_GENERATED_COLUMN);

        Assertions.assertNull(prepareOverwriteStatement("exp_id", List.of(EXP_ID, ACTIVITY_DATE), ACTIVITY_MONTH,
                List.of(EXP_ID), /*analyzed*/ false, ACTIVITY_MONTH));

        Assertions.assertEquals(missingBefore, eligibilitySkips(SkipReason.SOURCE_MISSING_SAMPLED_COLUMN));
        Assertions.assertEquals(generatedBefore + 1L, eligibilitySkips(SkipReason.UNSUPPORTED_GENERATED_COLUMN));
    }

    @Test
    public void computedKeyOverAColumnPushedDownElsewhereIsDeclinedOnlyUnderPushDown() {
        // dt = CAST(ts AS DATE) while ts also feeds grp_id BIGINT: push-down reads ts as BIGINT, the sampler as
        // DATETIME. With push-down off (config and statement property both off) the key stays sampleable.
        String byPosition = "INSERT INTO t SELECT exp_id, ts, bucket_id, CAST(ts AS DATE) "
                + "FROM FILES(\"path\" = \"s3://b/*\", \"format\" = \"parquet\")";
        String withProperty = "INSERT INTO t PROPERTIES('enable_push_down_schema' = 'true') SELECT exp_id, ts, "
                + "bucket_id, CAST(ts AS DATE) FROM FILES(\"path\" = \"s3://b/*\", \"format\" = \"parquet\")";
        List<Column> files = List.of(EXP_ID, TS, BUCKET_ID);
        boolean saved = Config.files_enable_insert_push_down_column_type;
        try {
            Config.files_enable_insert_push_down_column_type = true;
            Assertions.assertNull(prepareStatement(byPosition, null, List.of(EXP_ID), files));
            Config.files_enable_insert_push_down_column_type = false;
            Assertions.assertNotNull(prepareStatement(byPosition, null, List.of(EXP_ID), files));
            Assertions.assertNull(prepareStatement(withProperty, null, List.of(EXP_ID), files));
        } finally {
            Config.files_enable_insert_push_down_column_type = saved;
        }
    }

    // INSERT OVERWRITE runs the hook on an analyzed statement: a push-down function and a bound table of target types.
    private static final Column N = new Column("n", DateType.DATETIME, true);
    private static final Column M = new Column("m", DateType.DATE, true);
    private static final String COMPUTED_OVER_RETYPED = "exp_id, CAST(x AS DATETIME) AS n, x AS m";
    private static final String COMPUTED_OVER_UNCHANGED = "exp_id, ts, date_trunc('day', ts) AS dt";

    @Test
    public void overwriteChecksTheRetypeAgainstTheFileTypesReadAgain() {
        // The file stores x as a DATETIME; the bound table reports m's type, DATE, against which n would look safe.
        long computedBefore = eligibilitySkips(SkipReason.UNSUPPORTED_SAMPLED_PROJECTION);
        try (MockedConstruction<TableFunctionTable> reRead =
                reReadFilesSchema(List.of(EXP_ID, new Column("x", DateType.DATETIME)), List.of())) {
            Assertions.assertNull(prepareOverwriteStatement(COMPUTED_OVER_RETYPED, List.of(EXP_ID, N, M), N,
                    List.of(EXP_ID, new Column("x", DateType.DATE)), /*analyzed*/ true));
            Assertions.assertEquals(1, reRead.constructed().size());
        }

        Assertions.assertEquals(computedBefore + 1L, eligibilitySkips(SkipReason.UNSUPPORTED_SAMPLED_PROJECTION));
    }

    @Test
    public void overwriteStillSamplesAComputedKeyOverAFileColumnPushDownLeftUnchanged() {
        // ts is a DATETIME in the file and in the target, so push-down changed nothing and dt reads what the
        // sampler reads. The re-read table is the one prepare() carries on with.
        long computedBefore = eligibilitySkips(SkipReason.UNSUPPORTED_SAMPLED_PROJECTION);
        try (MockedConstruction<TableFunctionTable> reRead = reReadFilesSchema(List.of(EXP_ID, TS),
                List.of(PresplitTestSupport.brokerFileStatus("s3://b/x.parquet", 1024L)))) {
            PreSplitFlow.Prepared prepared = prepareOverwriteStatement(COMPUTED_OVER_UNCHANGED, List.of(EXP_ID, TS, DT),
                    DT, List.of(EXP_ID, TS), /*analyzed*/ true);

            Assertions.assertNotNull(prepared, "a computed key over an unchanged column must still be sampled");
            InsertFromFilesScanContext scanContext = (InsertFromFilesScanContext) prepared.scanContext();
            Assertions.assertSame(reRead.constructed().get(0), scanContext.sourceTable());
            Assertions.assertEquals(Map.of("dt", "date_trunc('day', `ts`)"), scanContext.targetToExpressionSql());
            Assertions.assertEquals(1024L, prepared.estimatedBytes());
        }
        Assertions.assertEquals(computedBefore, eligibilitySkips(SkipReason.UNSUPPORTED_SAMPLED_PROJECTION));
    }

    @Test
    public void overwriteWhoseFilesSchemaCannotBeReadAgainIsSkipped() {
        long computedBefore = eligibilitySkips(SkipReason.UNSUPPORTED_SAMPLED_PROJECTION);
        try (MockedConstruction<TableFunctionTable> ignoredReRead = Mockito.mockConstruction(TableFunctionTable.class,
                (table, construction) -> {
                    throw new DdlException("simulated file listing failure");
                })) {
            Assertions.assertNull(prepareOverwriteStatement(COMPUTED_OVER_UNCHANGED, List.of(EXP_ID, TS, DT), DT,
                    List.of(EXP_ID, TS), /*analyzed*/ true));
        }
        Assertions.assertEquals(computedBefore, eligibilitySkips(SkipReason.UNSUPPORTED_SAMPLED_PROJECTION));
    }

    @Test
    public void regularInsertUsesTheBoundFilesTableAsItIs() {
        // Without a push-down function the hook ran before analysis, so the bound table carries the file's types.
        try (MockedConstruction<TableFunctionTable> reRead = reReadFilesSchema(List.of(EXP_ID, TS), List.of())) {
            Assertions.assertNotNull(prepareOverwriteStatement(COMPUTED_OVER_UNCHANGED, List.of(EXP_ID, TS, DT), DT,
                    List.of(EXP_ID, TS), /*analyzed*/ false));
            Assertions.assertTrue(reRead.constructed().isEmpty());
        }
    }

    @Test
    public void overwriteWithACallFreeWhereKeepsTheBoundFilesTable() {
        // Only a WHERE clause that holds a call is analyzed against the source's column types. With a call-free one
        // and direct projections only, no source column's type decides anything, so the schema is not read again.
        try (MockedConstruction<TableFunctionTable> reRead = reReadFilesSchema(List.of(EXP_ID, TS, BUCKET_ID),
                List.of())) {
            PreSplitFlow.Prepared prepared = prepareOverwriteStatement("exp_id, ts", "bucket_id > 10",
                    /*analyzeWhere*/ true, List.of(EXP_ID, TS), TS, List.of(EXP_ID, TS, BUCKET_ID), /*analyzed*/ true);

            Assertions.assertNotNull(prepared);
            Assertions.assertTrue(reRead.constructed().isEmpty());
            Assertions.assertEquals("`table_function_table`.`bucket_id` > 10",
                    ((InsertFromFilesScanContext) prepared.scanContext()).wherePredicateSql());
        }
    }

    @Test
    public void overwriteReadsTheFilesSchemaAgainForAGeneratedSampledColumn() {
        // Direct projections and no WHERE clause: only the generated partition column makes a column type matter. The
        // file stores activity_date as a DATE; the bound table reports the target's DATETIME.
        try (MockedConstruction<TableFunctionTable> reRead = reReadFilesSchema(
                List.of(EXP_ID, new Column("activity_date", DateType.DATE)),
                List.of(PresplitTestSupport.brokerFileStatus("s3://b/x.parquet", 1024L)))) {
            PreSplitFlow.Prepared prepared = prepareOverwriteStatement("exp_id, activity_date",
                    List.of(EXP_ID, ACTIVITY_DATE), ACTIVITY_MONTH, List.of(EXP_ID, ACTIVITY_DATE), /*analyzed*/ true,
                    ACTIVITY_MONTH);

            Assertions.assertNotNull(prepared);
            Assertions.assertEquals(1, reRead.constructed().size());
            InsertFromFilesScanContext scanContext = (InsertFromFilesScanContext) prepared.scanContext();
            Assertions.assertSame(reRead.constructed().get(0), scanContext.sourceTable());
            Assertions.assertEquals(Map.of("activity_date_month", MONTH_SQL), scanContext.targetToExpressionSql());
        }
    }

    @Test
    public void whereIsCheckedForBuiltinCallsAgainstTheFileSchema() {
        String insert = "INSERT INTO t BY NAME SELECT exp_id, grp_id, bucket_id, '20260917' AS dt "
                + "FROM FILES(\"path\" = \"s3://b/dt=20260917/*\", \"format\" = \"parquet\") WHERE ";
        List<Column> files = List.of(EXP_ID, GRP_ID, BUCKET_ID);

        // upper is allowlisted by name, but no built-in upper takes two arguments.
        Assertions.assertNull(prepareStatement(insert + "upper(grp_id, grp_id) = 'x'", null, List.of(EXP_ID), files));
        PreSplitFlow.Prepared prepared = prepareStatement(insert + "abs(grp_id) > 10", null, List.of(EXP_ID), files);
        Assertions.assertNotNull(prepared);
        Assertions.assertEquals("(abs(`grp_id`)) > 10",
                ((InsertFromFilesScanContext) prepared.scanContext()).wherePredicateSql());
    }

    @Test
    public void overwriteChecksTheWhereAgainstTheFileSchemaReadAgain() {
        // A call in the WHERE clause makes the column types matter. The bound table lacks bucket_id, so only a check
        // against the schema read again admits the predicate.
        try (MockedConstruction<TableFunctionTable> reRead = reReadFilesSchema(List.of(EXP_ID, TS, BUCKET_ID),
                List.of(PresplitTestSupport.brokerFileStatus("s3://b/x.parquet", 1024L)))) {
            PreSplitFlow.Prepared prepared = prepareOverwriteStatement("exp_id, ts", "abs(bucket_id) > 10",
                    /*analyzeWhere*/ false, List.of(EXP_ID, TS), TS, List.of(EXP_ID, TS), /*analyzed*/ true);

            Assertions.assertNotNull(prepared, "the predicate analyzes against the schema read again");
            Assertions.assertEquals(1, reRead.constructed().size());
            Assertions.assertEquals("(abs(`bucket_id`)) > 10",
                    ((InsertFromFilesScanContext) prepared.scanContext()).wherePredicateSql());
        }
    }

    @Test
    public void overwriteAdmitsAnAnalyzedStructFieldPredicate() {
        // The statement's analysis rewrote payload.name into a STRUCT field read whose column name is the field path;
        // it must resolve again, with a call to bind and without one.
        for (String where : List.of("payload.name = 'X'", "upper(payload.name) = 'X'")) {
            try (MockedConstruction<TableFunctionTable> reRead = reReadFilesSchema(List.of(EXP_ID, TS, PAYLOAD),
                    List.of(PresplitTestSupport.brokerFileStatus("s3://b/x.parquet", 1024L)))) {
                PreSplitFlow.Prepared prepared = prepareOverwriteStatement("exp_id, ts", where,
                        /*analyzeWhere*/ true, List.of(EXP_ID, TS), TS, List.of(EXP_ID, TS, PAYLOAD),
                        /*analyzed*/ true);

                Assertions.assertNotNull(prepared, where);
                Assertions.assertNotNull(((InsertFromFilesScanContext) prepared.scanContext()).wherePredicateSql(),
                        where);
            }
        }
    }

    @Test
    public void whereWithADecimalLiteralIsDeclinedWhenTheSamplerWouldReadItAsADouble() {
        // The check runs on the folded WHERE clause in the user's sql_mode: CAST(concat('1', '.5') AS DECIMAL(10, 1))
        // folds to CAST(1.5 AS DECIMAL64(10,1)), whose 1.5 the sampler reads as a DOUBLE only under DOUBLE_LITERAL.
        String insert = "INSERT INTO t BY NAME SELECT exp_id, grp_id, bucket_id, '20260917' AS dt "
                + "FROM FILES(\"path\" = \"s3://b/dt=20260917/*\", \"format\" = \"parquet\") WHERE "
                + "grp_id < CAST(concat('1', '.5') AS DECIMAL(10, 1))";
        List<Column> files = List.of(EXP_ID, GRP_ID, BUCKET_ID);
        SessionVariable doubleLiteralSession = new SessionVariable();
        doubleLiteralSession.setSqlMode(SqlModeHelper.MODE_DEFAULT | SqlModeHelper.MODE_DOUBLE_LITERAL);

        PreSplitFlow.Prepared prepared = prepareStatement(insert, null, List.of(EXP_ID), files, new SessionVariable());
        Assertions.assertNotNull(prepared);
        Assertions.assertEquals("`grp_id` < (CAST(1.5 AS DECIMAL64(10,1)))",
                ((InsertFromFilesScanContext) prepared.scanContext()).wherePredicateSql());
        Assertions.assertNull(prepareStatement(insert, null, List.of(EXP_ID), files, doubleLiteralSession));
    }

    /** Stubs every {@code TableFunctionTable} prepare() constructs to infer {@code schema} over {@code files}. */
    private static MockedConstruction<TableFunctionTable> reReadFilesSchema(List<Column> schema,
                                                                           List<TBrokerFileStatus> files) {
        return Mockito.mockConstruction(TableFunctionTable.class, (table, construction) -> {
            when(table.getFullVisibleSchema()).thenReturn(schema);
            when(table.loadFileList()).thenReturn(files);
        });
    }

    /**
     * prepare() on {@code INSERT INTO t BY NAME SELECT <select> FROM FILES(...)} into
     * {@code targetColumns} plus {@code generated}, ORDER BY (exp_id), PARTITION BY ({@code partitionColumn}), whose
     * bound FILES table reports {@code boundSchema}. {@code analyzed} adds a no-op push-down function, as on INSERT
     * OVERWRITE.
     */
    private static PreSplitFlow.Prepared prepareOverwriteStatement(
            String select, List<Column> targetColumns, Column partitionColumn, List<Column> boundSchema,
            boolean analyzed, Column... generated) {
        return prepareOverwriteStatement(select, /*where*/ null, /*analyzeWhere*/ false, targetColumns,
                partitionColumn, boundSchema, analyzed, generated);
    }

    /**
     * {@link #prepareOverwriteStatement} filtering by {@code where}, if not null; {@code analyzeWhere} analyzes it
     * against {@code boundSchema} first, as INSERT OVERWRITE leaves it.
     */
    private static PreSplitFlow.Prepared prepareOverwriteStatement(
            String select, String where, boolean analyzeWhere, List<Column> targetColumns, Column partitionColumn,
            List<Column> boundSchema, boolean analyzed, Column... generated) {
        InsertStmt stmt = (InsertStmt) SqlParser.parseSingleStatement("INSERT INTO t BY NAME SELECT " + select
                + " FROM FILES(\"path\" = \"s3://b/*\", \"format\" = \"parquet\")"
                + (where == null ? "" : " WHERE " + where), SqlModeHelper.MODE_DEFAULT);
        SelectRelation selectRelation = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        FileTableFunctionRelation filesRelation = (FileTableFunctionRelation) selectRelation.getRelation();
        TableFunctionTable filesTable = mock(TableFunctionTable.class);
        when(filesTable.getFullVisibleSchema()).thenReturn(boundSchema);
        when(filesTable.loadFileList()).thenReturn(List.of());
        filesRelation.setTable(filesTable);
        if (analyzed) {
            filesRelation.setPushDownSchemaFunc(table -> { });
        }

        OlapTable target = mock(OlapTable.class);
        when(target.getName()).thenReturn("t");
        stubGeneratedSchema(target, targetColumns, generated);
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.getPartitionColumns(any())).thenReturn(List.of(partitionColumn));
        when(target.getPartitionInfo()).thenReturn(partitionInfo);
        when(target.getBaseIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        MaterializedIndexMeta baseMeta = mock(MaterializedIndexMeta.class);
        when(baseMeta.getIndexMetaId()).thenReturn(BASE_INDEX_META_ID);
        when(target.getVisibleIndexMetas()).thenReturn(List.of(baseMeta));

        ConnectContext context = mock(ConnectContext.class);
        when(context.getCurrentComputeResource()).thenReturn(mock(ComputeResource.class));
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(sessionVariable.getTimeZone()).thenReturn("UTC");
        when(context.getSessionVariable()).thenReturn(sessionVariable);
        if (analyzeWhere) {
            List<Field> fields = new ArrayList<>();
            for (Column column : boundSchema) {
                fields.add(new Field(column.getName(), column.getType(), filesRelation.getName(), null));
            }
            ExpressionAnalyzer.analyzeExpression(selectRelation.getWhereClause(), new AnalyzeState(),
                    new Scope(RelationId.anonymous(), new RelationFields(fields)), context);
        }

        boolean savedPushDown = Config.files_enable_insert_push_down_column_type;
        boolean savedHasInit = MetricRepo.hasInit;
        Config.files_enable_insert_push_down_column_type = true;
        MetricRepo.hasInit = true;
        try (MockedConstruction<QueryAnalyzer> ignoredAnalyzer = Mockito.mockConstruction(QueryAnalyzer.class);
                MockedStatic<MetaUtils> metaUtils = Mockito.mockStatic(MetaUtils.class)) {
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target)).thenReturn(List.of(EXP_ID));
            return new FilesPreSplitSource().prepare(stmt, selectRelation, target, /*database*/ null, context);
        } finally {
            Config.files_enable_insert_push_down_column_type = savedPushDown;
            MetricRepo.hasInit = savedHasInit;
        }
    }

    private static long eligibilitySkips(SkipReason reason) {
        return MetricRepo.COUNTER_TABLET_PRE_SPLIT_ELIGIBILITY_SKIPPED.getMetric(reason.name().toLowerCase()).getValue();
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
        SessionVariable sessionVariable = mock(SessionVariable.class);
        when(sessionVariable.getTimeZone()).thenReturn("UTC");
        return prepareStatement(sql, rollupSortKey, baseSortKey, sourceColumns, sessionVariable);
    }

    /** {@link #prepareStatement} for a user whose session is {@code sessionVariable}. */
    private static PreSplitFlow.Prepared prepareStatement(
            String sql, List<Column> rollupSortKey, List<Column> baseSortKey, List<Column> sourceColumns,
            SessionVariable sessionVariable) {
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
