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
        tableProperties.put("driver_url", "/opt/adbc/lib/libadbc_driver_flightsql.so");
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
    public void testExplainShowsDriverUrl() {
        ADBCScanNode node = createScanNodeWithColumns("col1");
        String explain = node.getNodeExplainString("  ", TExplainLevel.NORMAL);
        assertTrue(explain.contains("TABLE: \"test_schema\".\"test_table\""));
        assertTrue(explain.contains("QUERY: SELECT \"col1\" FROM \"test_schema\".\"test_table\""));
        assertTrue(explain.contains("DRIVER: /opt/adbc/lib/libadbc_driver_flightsql.so"));
        assertTrue(explain.contains("URI: grpc://localhost:8815"));
    }

    @Test
    public void testExplainShowsDriverNameWhenNoUrl() {
        tableProperties.remove("driver_url");
        tableProperties.put("driver_name", "flightsql");

        ADBCScanNode node = createScanNodeWithColumns("col1");
        String explain = node.getNodeExplainString("  ", TExplainLevel.NORMAL);
        assertTrue(explain.contains("DRIVER: flightsql"));
    }

    @Test
    public void testExplainAnalyzeOutput() {
        ADBCScanNode node = createScanNodeWithColumns("col1");
        String explain = node.getNodeExplainString("  ", TExplainLevel.VERBOSE);
        assertTrue(explain.contains("ConnectTime:"));
        assertTrue(explain.contains("RowsRead:"));
        assertTrue(explain.contains("BytesRead:"));
    }

    @Test
    public void testCanUseRuntimeAdaptiveDop() {
        PlanNodeId planNodeId = new PlanNodeId(1);
        when(mockTupleDesc.getSlots()).thenReturn(new ArrayList<SlotDescriptor>());
        ADBCScanNode node = new ADBCScanNode(planNodeId, mockTupleDesc, mockTable);
        assertEquals(false, node.canUseRuntimeAdaptiveDop());
    }
}
