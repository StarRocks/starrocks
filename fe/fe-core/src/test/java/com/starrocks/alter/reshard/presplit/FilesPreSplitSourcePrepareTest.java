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
    private static PreSplitFlow.Prepared prepareCustomerStatement(List<Column> rollupSortKey) {
        InsertStmt stmt = (InsertStmt) SqlParser.parseSingleStatement(
                "INSERT INTO t BY NAME SELECT exp_id, grp_id, bucket_id, '20260917' AS dt "
                        + "FROM FILES(\"path\" = \"s3://b/dt=20260917/*\", \"format\" = \"parquet\")",
                SqlModeHelper.MODE_DEFAULT);
        SelectRelation selectRelation = (SelectRelation) stmt.getQueryStatement().getQueryRelation();
        FileTableFunctionRelation filesRelation = (FileTableFunctionRelation) selectRelation.getRelation();
        TableFunctionTable filesTable = mock(TableFunctionTable.class);
        when(filesTable.getFullVisibleSchema()).thenReturn(List.of(EXP_ID, GRP_ID, BUCKET_ID));
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
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target)).thenReturn(List.of(EXP_ID));
            metaUtils.when(() -> MetaUtils.getRangeDistributionColumns(target, ROLLUP_INDEX_META_ID))
                    .thenReturn(rollupSortKey);
            return new FilesPreSplitSource().prepare(stmt, selectRelation, target, /*database*/ null, context);
        }
    }
}
