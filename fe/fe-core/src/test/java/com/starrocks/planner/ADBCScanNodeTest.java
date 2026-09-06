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

package com.starrocks.planner;

import com.starrocks.catalog.ADBCTable;
import com.starrocks.catalog.Column;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.type.IntegerType;
import com.starrocks.thrift.TADBCScanNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for ADBCScanNode SQL generation, Thrift serialization, and EXPLAIN output.
 */
public class ADBCScanNodeTest {

    private ADBCTable mockTable;
    private TupleDescriptor mockTupleDesc;
    private Map<String, String> tableProperties;

    @BeforeEach
    public void setUp() {
        tableProperties = new HashMap<>();
        tableProperties.put("type", "adbc");
        tableProperties.put("driver", "adbc_driver_flightsql");
        tableProperties.put("uri", "grpc://localhost:8815");
        tableProperties.put("user", "admin");
        tableProperties.put("adbc.flight.sql.rpc_timeout", "30");

        mockTable = mock(ADBCTable.class);
        when(mockTable.getDbName()).thenReturn("test_schema");
        when(mockTable.getName()).thenReturn("test_table");
        when(mockTable.getProperties()).thenReturn(tableProperties);

        // Set up tuple descriptor with materialized slots
        mockTupleDesc = mock(TupleDescriptor.class);
        when(mockTupleDesc.getId()).thenReturn(new TupleId(0));
    }

    private ADBCScanNode createScanNodeWithColumns(String... colNames) {
        ArrayList<SlotDescriptor> slots = new ArrayList<>();
        for (String colName : colNames) {
            SlotDescriptor slot = mock(SlotDescriptor.class);
            Column col = new Column(colName, IntegerType.INT);
            when(slot.isMaterialized()).thenReturn(true);
            when(slot.getColumn()).thenReturn(col);
            slots.add(slot);
        }
        when(mockTupleDesc.getSlots()).thenReturn(slots);

        PlanNodeId planNodeId = new PlanNodeId(1);
        ADBCScanNode node = new ADBCScanNode(planNodeId, mockTupleDesc, mockTable);
        node.computeColumnsAndFilters();
        return node;
    }

    @Test
    public void testGetADBCQueryStr_ColumnsOnly() {
        ADBCScanNode node = createScanNodeWithColumns("col1", "col2");
        String query = node.getADBCQueryStr();
        assertEquals("SELECT \"col1\", \"col2\" FROM \"test_schema\".\"test_table\"", query);
    }

    @Test
    public void testGetADBCQueryStr_WithLimit() {
        ADBCScanNode node = createScanNodeWithColumns("col1");
        node.setLimit(100);
        String query = node.getADBCQueryStr();
        assertEquals("SELECT \"col1\" FROM \"test_schema\".\"test_table\" LIMIT 100", query);
    }

    @Test
    public void testToThriftPopulatesScanFields() {
        ADBCScanNode node = createScanNodeWithColumns("col1", "col2");
        node.setLimit(50);

        TPlanNode msg = new TPlanNode();
        node.toThrift(msg);

        TADBCScanNode scanNode = msg.adbc_scan_node;
        assertNotNull(scanNode);
        assertEquals(0, scanNode.getTuple_id());
        assertEquals("\"test_schema\".\"test_table\"", scanNode.getTable_name());
        assertEquals(2, scanNode.getColumns().size());
        assertEquals("\"col1\"", scanNode.getColumns().get(0));
        assertEquals("\"col2\"", scanNode.getColumns().get(1));
        assertEquals(50, scanNode.getLimit());
    }

    @Test
    public void testExplainShowsDriver() {
        ADBCScanNode node = createScanNodeWithColumns("col1");
        String explain = node.getNodeExplainString("  ", TExplainLevel.NORMAL);
        assertTrue(explain.contains("TABLE: \"test_schema\".\"test_table\""));
        assertTrue(explain.contains("QUERY: SELECT \"col1\" FROM \"test_schema\".\"test_table\""));
        assertTrue(explain.contains("DRIVER: adbc_driver_flightsql"));
        assertFalse(explain.contains(tableProperties.get("uri")));
    }

    @Test
    public void testExplainDoesNotExposeConnectionCredentialsOrPlaceholderCounters() {
        tableProperties.put("uri", "grpc://admin:secret@localhost:8815?token=secret");
        tableProperties.put("password", "secret");
        ADBCScanNode node = createScanNodeWithColumns("col1");
        String explain = node.getNodeExplainString("  ", TExplainLevel.VERBOSE);
        assertFalse(explain.contains("secret"));
        assertFalse(explain.contains("{connect_time_ms}"));
        assertTrue(explain.contains("DRIVER: adbc_driver_flightsql"));
    }

    @Test
    public void testIdentifiersEscapeEmbeddedQuotes() {
        when(mockTable.getDbName()).thenReturn("test\"schema");
        when(mockTable.getName()).thenReturn("test\"table");
        ADBCScanNode node = createScanNodeWithColumns("col\"umn", "\"quoted\"");
        assertEquals("SELECT \"col\"\"umn\", \"\"\"quoted\"\"\" FROM \"test\"\"schema\".\"test\"\"table\"",
                node.getADBCQueryStr());
    }

    @Test
    public void testPredicateEscapesIdentifierAndStringLiteral() {
        ADBCScanNode node = createScanNodeWithColumns("col\"umn");
        node.getConjuncts().add(new BinaryPredicate(BinaryType.EQ,
                new SlotRef(null, "col\"umn"), new StringLiteral("O'Reilly\\books")));
        node.computeColumnsAndFilters();
        assertEquals(List.of("\"col\"\"umn\" = 'O''Reilly\\books'"), node.getFilters());
        String query = node.getADBCQueryStr();
        node.computeColumnsAndFilters();
        assertEquals(query, node.getADBCQueryStr());
    }

    @Test
    public void testRepeatedComputeDoesNotDuplicateProjection() {
        ADBCScanNode node = createScanNodeWithColumns("col1");
        String query = node.getADBCQueryStr();
        node.computeColumnsAndFilters();
        assertEquals(query, node.getADBCQueryStr());
    }

    @Test
    public void testCanUseRuntimeAdaptiveDop() {
        PlanNodeId planNodeId = new PlanNodeId(1);
        when(mockTupleDesc.getSlots()).thenReturn(new ArrayList<SlotDescriptor>());
        ADBCScanNode node = new ADBCScanNode(planNodeId, mockTupleDesc, mockTable);
        assertEquals(false, node.canUseRuntimeAdaptiveDop());
    }
}
