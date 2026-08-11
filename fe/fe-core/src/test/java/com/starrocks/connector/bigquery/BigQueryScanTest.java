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

package com.starrocks.connector.bigquery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.starrocks.catalog.BigQueryTable;
import com.starrocks.catalog.Column;
import com.starrocks.planner.BigQueryScanNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalBigQueryScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalBigQueryScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.rule.implementation.BigQueryScanImplementationRule;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TTableType;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

public class BigQueryScanTest extends BigQueryMockedBase {

    static ColumnRefOperator idColumnRef =
            new ColumnRefOperator(1, com.starrocks.type.IntegerType.BIGINT, "id", true);
    static ColumnRefOperator nameColumnRef =
            new ColumnRefOperator(2, com.starrocks.type.StringType.STRING, "name", true);

    static Map<ColumnRefOperator, Column> scanColumnMap = new HashMap<>() {
        {
            put(idColumnRef, new Column("id", com.starrocks.catalog.Type.BIGINT));
            put(nameColumnRef, new Column("name", com.starrocks.catalog.ScalarType.createDefaultCatalogString()));
        }
    };

    static LogicalBigQueryScanOperator logicalScan;
    static PhysicalBigQueryScanOperator physicalScan;
    static BigQueryScanImplementationRule rule = new BigQueryScanImplementationRule();

    @Mocked
    static OptimizerContext optimizerContext;

    @BeforeAll
    public static void setUp() {
        initMock();

        logicalScan = new LogicalBigQueryScanOperator(
                bigQueryTable,
                scanColumnMap,
                Maps.newHashMap(),
                -1,
                new BinaryPredicateOperator(BinaryType.EQ,
                        new ColumnRefOperator(1, com.starrocks.type.IntegerType.BIGINT, "id", true),
                        ConstantOperator.createBigint(42L)));

        OptExpression logicalExpr = new OptExpression(logicalScan);
        List<OptExpression> transformed = rule.transform(logicalExpr, optimizerContext);
        physicalScan = (PhysicalBigQueryScanOperator) transformed.get(0).getOp();
    }

    // ---- Logical operator ----

    @Test
    public void testLogicalScanHasTable() {
        Assertions.assertNotNull(logicalScan.getTable());
        Assertions.assertInstanceOf(BigQueryTable.class, logicalScan.getTable());
        Assertions.assertEquals(TABLE_NAME, logicalScan.getTable().getName());
    }

    @Test
    public void testLogicalScanPredicates() {
        Assertions.assertNotNull(logicalScan.getPredicate());
        Assertions.assertInstanceOf(BinaryPredicateOperator.class, logicalScan.getPredicate());
    }

    @Test
    public void testLogicalScanBuilder() {
        LogicalBigQueryScanOperator.Builder builder = new LogicalBigQueryScanOperator.Builder();
        LogicalBigQueryScanOperator clone = builder.withOperator(logicalScan).build();
        Assertions.assertEquals(logicalScan, clone);
    }

    @Test
    public void testLogicalScanIsUnpartitioned() {
        // BigQuery tables always appear unpartitioned in Phase 1
        Assertions.assertTrue(logicalScan.getTable().isUnPartitioned());
    }

    // ---- Implementation rule ----

    @Test
    public void testImplementationRuleProducesPhysicalOperator() {
        OptExpression expr = new OptExpression(logicalScan);
        List<OptExpression> result = rule.transform(expr, optimizerContext);
        Assertions.assertEquals(1, result.size());
        Assertions.assertInstanceOf(PhysicalBigQueryScanOperator.class, result.get(0).getOp());
    }

    // ---- Physical operator ----

    @Test
    public void testPhysicalScanPredicates() {
        ScanOperatorPredicates predicates = physicalScan.getScanOperatorPredicates();
        Assertions.assertNotNull(predicates);
    }

    @Test
    public void testPhysicalScanUsedColumnsIncludesBothColumns() {
        ColumnRefSet usedCols = physicalScan.getUsedColumns();
        Assertions.assertNotNull(usedCols);
        // The scan selects 2 columns
        Assertions.assertEquals(2, usedCols.size());
    }

    @Test
    public void testPhysicalScanSetPredicates() {
        ScanOperatorPredicates original = physicalScan.getScanOperatorPredicates();
        ScanOperatorPredicates replacement = new ScanOperatorPredicates();
        physicalScan.setScanOperatorPredicates(replacement);
        Assertions.assertSame(replacement, physicalScan.getScanOperatorPredicates());
        // Restore
        physicalScan.setScanOperatorPredicates(original);
    }

    // ---- BigQueryScanNode ----

    @Test
    public void testBigQueryScanNodeSetupScanRangeLocations() {
        // Build a minimal TupleDescriptor pointing at bigQueryTable
        com.starrocks.analysis.DescriptorTable descTable = new com.starrocks.analysis.DescriptorTable();
        com.starrocks.analysis.TupleDescriptor tupleDesc = descTable.createTupleDescriptor();
        tupleDesc.setTable(bigQueryTable);

        // Add slot descriptors for id and name
        for (Column col : bigQueryTable.getFullSchema()) {
            com.starrocks.analysis.SlotDescriptor slot = descTable.addSlotDescriptor(tupleDesc);
            slot.setColumn(col);
            slot.setIsNullable(true);
        }
        tupleDesc.computeMemLayout();

        BigQueryScanNode scanNode = new BigQueryScanNode(
                new PlanNodeId(1), tupleDesc, "BigQueryScanNode");

        // setupScanRangeLocations calls MetadataMgr.getRemoteFiles (mocked in initMock)
        scanNode.setupScanRangeLocations(tupleDesc, null, ImmutableList.of());

        // Should produce 2 scan ranges (one per mock stream)
        Assertions.assertEquals(2, scanNode.getScanRangeLocations(0).size());

        // Validate that each scan range has use_bigquery_jni_reader set
        scanNode.getScanRangeLocations(0).forEach(loc -> {
            var hdfsRange = loc.getScan_range().getHdfs_scan_range();
            Assertions.assertTrue(hdfsRange.isUse_bigquery_jni_reader());
            Assertions.assertNotNull(hdfsRange.getBigquery_split_infos());
            Assertions.assertTrue(
                    hdfsRange.getBigquery_split_infos().containsKey("read_stream_name"));
            Assertions.assertTrue(
                    hdfsRange.getBigquery_split_infos().containsKey("read_session_name"));
        });
    }

    @Test
    public void testBigQueryScanNodeEmptyResultWhenNoStreams() {
        // When getRemoteFiles returns empty, scan ranges should be empty too
        when(metadataMgr.getRemoteFiles(any(), any())).thenReturn(ImmutableList.of());

        com.starrocks.analysis.DescriptorTable descTable = new com.starrocks.analysis.DescriptorTable();
        com.starrocks.analysis.TupleDescriptor tupleDesc = descTable.createTupleDescriptor();
        tupleDesc.setTable(bigQueryTable);
        tupleDesc.computeMemLayout();

        BigQueryScanNode scanNode = new BigQueryScanNode(
                new PlanNodeId(2), tupleDesc, "BigQueryScanNode");
        scanNode.setupScanRangeLocations(tupleDesc, null, ImmutableList.of());

        Assertions.assertEquals(0, scanNode.getScanRangeLocations(0).size());

        // Restore mock for other tests
        com.google.common.collect.ImmutableList<com.starrocks.connector.RemoteFileInfo> restored =
                buildRestoredFileInfo();
        when(metadataMgr.getRemoteFiles(any(), any())).thenReturn(restored);
    }

    // ---- BigQueryTable toThrift ----

    @Test
    public void testBigQueryTableToThriftType() {
        com.starrocks.thrift.TTableDescriptor tdesc = bigQueryTable.toThrift(ImmutableList.of());
        Assertions.assertEquals(TTableType.BIGQUERY_TABLE, tdesc.getTableType());
        Assertions.assertEquals(TABLE_NAME, tdesc.getTableName());
        Assertions.assertEquals(DATASET_ID, tdesc.getDbName());
    }

    @Test
    public void testBigQueryTableIsView() {
        // Base table should not be a view
        Assertions.assertFalse(bigQueryTable.isView());
        // Construct a view table and verify the flag
        BigQueryTable viewTable = new BigQueryTable(
                CATALOG_NAME, DATASET_ID, "my_view",
                bigQueryTable.getFullSchema(), 1000L, true);
        Assertions.assertTrue(viewTable.isView());
    }

    // ---- RemoteFileDesc ----

    @Test
    public void testBigQueryRemoteFileDescFields() {
        BigQueryRemoteFileDesc desc = BigQueryRemoteFileDesc
                .createBigQueryRemoteFileDesc("session1", "stream0", 0);
        Assertions.assertEquals("session1", desc.getReadSessionName());
        Assertions.assertEquals("stream0", desc.getReadStreamName());
        Assertions.assertEquals(0, desc.getStreamIndex());
        Assertions.assertFalse(desc.isTempTable());
    }

    @Test
    public void testBigQueryRemoteFileDescTempTable() {
        BigQueryRemoteFileDesc desc = BigQueryRemoteFileDesc
                .createBigQueryRemoteFileDesc("session1", "stream0", 0, true);
        Assertions.assertTrue(desc.isTempTable());
    }

    // ---- Helper ----

    private static com.google.common.collect.ImmutableList<com.starrocks.connector.RemoteFileInfo>
    buildRestoredFileInfo() {
        Map<String, String> params = new HashMap<>();
        params.put("project_id", PROJECT_ID);
        params.put("dataset_id", DATASET_ID);
        params.put("table_id", TABLE_NAME);
        params.put("required_fields", "id,name");
        params.put("credentials_base64", "");
        params.put("read_session_name", mockReadSession.getName());

        com.starrocks.connector.RemoteFileInfo info = new com.starrocks.connector.RemoteFileInfo();
        info.setFiles(ImmutableList.of(
                BigQueryRemoteFileDesc.createBigQueryRemoteFileDesc(
                        mockReadSession.getName(),
                        mockReadSession.getStreams(0).getName(), 0),
                BigQueryRemoteFileDesc.createBigQueryRemoteFileDesc(
                        mockReadSession.getName(),
                        mockReadSession.getStreams(1).getName(), 1)
        ));
        info.setAttachment(params);
        return ImmutableList.of(info);
    }
}
