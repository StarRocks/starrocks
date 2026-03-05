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
import com.starrocks.catalog.Type;
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
 * Wave 0 test scaffold for ADBCScanNode SQL generation and Thrift serialization.
 */
public class ADBCScanNodeTest {

    private ADBCTable mockTable;
    private TupleDescriptor mockTupleDesc;
    private Map<String, String> tableProperties;

    @BeforeEach
    public void setUp() {
        tableProperties = new HashMap<>();
        tableProperties.put("adbc.driver", "flight_sql");
        tableProperties.put("adbc.url", "grpc://localhost:8815");
        tableProperties.put("adbc.username", "testuser");
        tableProperties.put("adbc.password", "testpass");

        mockTable = mock(ADBCTable.class);
        when(mockTable.getDbName()).thenReturn("test_schema");
        when(mockTable.getName()).thenReturn("test_table");
        when(mockTable.getProperties()).thenReturn(tableProperties);

        // Set up tuple descriptor with materialized slots
        mockTupleDesc = mock(TupleDescriptor.class);
        when(mockTupleDesc.getId()).thenReturn(new com.starrocks.thrift.TTupleId(0));
    }

    private ADBCScanNode createScanNodeWithColumns(String... colNames) {
        List<SlotDescriptor> slots = new ArrayList<>();
        for (String colName : colNames) {
            SlotDescriptor slot = mock(SlotDescriptor.class);
            Column col = new Column(colName, Type.INT);
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
    public void testGetADBCDriverName_FlightSql() {
        PlanNodeId planNodeId = new PlanNodeId(1);
        when(mockTupleDesc.getSlots()).thenReturn(new ArrayList<>());
        ADBCScanNode node = new ADBCScanNode(planNodeId, mockTupleDesc, mockTable);
        assertEquals("adbc_driver_flightsql", node.getADBCDriverName());
    }

    @Test
    public void testGetADBCDriverName_Custom() {
        tableProperties.put("adbc.driver", "my_custom_driver");
        PlanNodeId planNodeId = new PlanNodeId(1);
        when(mockTupleDesc.getSlots()).thenReturn(new ArrayList<>());
        ADBCScanNode node = new ADBCScanNode(planNodeId, mockTupleDesc, mockTable);
        assertEquals("my_custom_driver", node.getADBCDriverName());
    }

    @Test
    public void testToThrift() {
        ADBCScanNode node = createScanNodeWithColumns("col1", "col2");
        node.setLimit(50);

        TPlanNode msg = new TPlanNode();
        node.toThrift(msg);

        TADBCScanNode adbcNode = msg.adbc_scan_node;
        assertNotNull(adbcNode);
        assertEquals("\"test_schema\".\"test_table\"", adbcNode.getTable_name());
        assertEquals(2, adbcNode.getColumns().size());
        assertEquals("\"col1\"", adbcNode.getColumns().get(0));
        assertEquals("\"col2\"", adbcNode.getColumns().get(1));
        assertEquals(50, adbcNode.getLimit());
        assertEquals("adbc_driver_flightsql", adbcNode.getAdbc_driver());
        assertEquals("grpc://localhost:8815", adbcNode.getAdbc_uri());
        assertEquals("testuser", adbcNode.getAdbc_username());
        assertEquals("testpass", adbcNode.getAdbc_password());
    }

    @Test
    public void testExplainOutput() {
        ADBCScanNode node = createScanNodeWithColumns("col1");
        String explain = node.getNodeExplainString("  ", TExplainLevel.NORMAL);
        assertTrue(explain.contains("TABLE: \"test_schema\".\"test_table\""));
        assertTrue(explain.contains("QUERY: SELECT \"col1\" FROM \"test_schema\".\"test_table\""));
        assertTrue(explain.contains("DRIVER: flight_sql"));
        assertTrue(explain.contains("URI: grpc://localhost:8815"));
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
        when(mockTupleDesc.getSlots()).thenReturn(new ArrayList<>());
        ADBCScanNode node = new ADBCScanNode(planNodeId, mockTupleDesc, mockTable);
        assertEquals(false, node.canUseRuntimeAdaptiveDop());
    }
}
