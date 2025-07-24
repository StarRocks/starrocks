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

package com.starrocks.qe;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.parser.NodePosition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ShowResultProcessorTest {

    private ShowResultSetMetaData metaData;
    private List<List<String>> testData;

    @Before
    public void setUp() {
        // Create test metadata - simulating SHOW BACKENDS columns
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("BackendId", ScalarType.createVarchar(20)));
        builder.addColumn(new Column("Host", ScalarType.createVarchar(30)));
        builder.addColumn(new Column("Port", ScalarType.createVarchar(10)));
        builder.addColumn(new Column("Alive", ScalarType.createVarchar(10)));
        metaData = builder.build();

        // Create test data
        testData = Lists.newArrayList();
        testData.add(Lists.newArrayList("1", "host1.example.com", "9050", "true"));
        testData.add(Lists.newArrayList("2", "host2.example.com", "9050", "false"));
        testData.add(Lists.newArrayList("3", "host3.example.com", "9050", "true"));
        testData.add(Lists.newArrayList("4", "test.example.com", "9050", "true"));
        testData.add(Lists.newArrayList("5", "prod.example.com", "9050", "false"));
    }

    @Test
    public void testNoClausesProcessing() {
        // Test with no clauses - should return original data
        ShowBackendsStmt statement = new ShowBackendsStmt();
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(5, result.getResultRows().size());
        Assert.assertEquals("1", result.getResultRows().get(0).get(0));
        Assert.assertEquals("host1.example.com", result.getResultRows().get(0).get(1));
    }

    @Test
    public void testLikePatternFiltering() {
        // Test LIKE pattern filtering
        ShowBackendsStmt statement = new ShowBackendsStmt("host%", null, null, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(3, result.getResultRows().size());
        Assert.assertEquals("1", result.getResultRows().get(0).get(0));
        Assert.assertEquals("2", result.getResultRows().get(1).get(0));
        Assert.assertEquals("3", result.getResultRows().get(2).get(0));
    }

    @Test
    public void testLikePatternWithWildcard() {
        // Test LIKE pattern with single character wildcard
        ShowBackendsStmt statement = new ShowBackendsStmt("tes_", null, null, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(1, result.getResultRows().size());
        Assert.assertEquals("4", result.getResultRows().get(0).get(0));
    }

    @Test
    public void testWhereClauseEqualFiltering() throws Exception {
        // Test WHERE clause with equality
        SlotRef slotRef = new SlotRef(new TableName(null, null), "Alive");
        StringLiteral literal = new StringLiteral("true");
        BinaryPredicate predicate = new BinaryPredicate(BinaryType.EQ, slotRef, literal);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, predicate, null, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(3, result.getResultRows().size());
        for (List<String> row : result.getResultRows()) {
            Assert.assertEquals("true", row.get(3));
        }
    }

    @Test
    public void testWhereClauseNotEqualFiltering() throws Exception {
        // Test WHERE clause with not equal
        SlotRef slotRef = new SlotRef(new TableName(null, null), "Alive");
        StringLiteral literal = new StringLiteral("true");
        BinaryPredicate predicate = new BinaryPredicate(BinaryType.NE, slotRef, literal);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, predicate, null, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(2, result.getResultRows().size());
        for (List<String> row : result.getResultRows()) {
            Assert.assertEquals("false", row.get(3));
        }
    }

    @Test
    public void testOrderByAscending() throws Exception {
        // Test ORDER BY ascending
        SlotRef slotRef = new SlotRef(new TableName(null, null), "Host");
        OrderByElement orderBy = new OrderByElement(slotRef, true, false);
        List<OrderByElement> orderByElements = Lists.newArrayList(orderBy);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, null, orderByElements, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(5, result.getResultRows().size());
        Assert.assertEquals("host1.example.com", result.getResultRows().get(0).get(1));
        Assert.assertEquals("host2.example.com", result.getResultRows().get(1).get(1));
        Assert.assertEquals("host3.example.com", result.getResultRows().get(2).get(1));
        Assert.assertEquals("prod.example.com", result.getResultRows().get(3).get(1));
        Assert.assertEquals("test.example.com", result.getResultRows().get(4).get(1));
    }

    @Test
    public void testOrderByDescending() throws Exception {
        // Test ORDER BY descending
        SlotRef slotRef = new SlotRef(new TableName(null, null), "BackendId");
        OrderByElement orderBy = new OrderByElement(slotRef, false, false);
        List<OrderByElement> orderByElements = Lists.newArrayList(orderBy);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, null, orderByElements, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(5, result.getResultRows().size());
        Assert.assertEquals("5", result.getResultRows().get(0).get(0));
        Assert.assertEquals("4", result.getResultRows().get(1).get(0));
        Assert.assertEquals("3", result.getResultRows().get(2).get(0));
        Assert.assertEquals("2", result.getResultRows().get(3).get(0));
        Assert.assertEquals("1", result.getResultRows().get(4).get(0));
    }

    @Test
    public void testMultipleOrderBy() throws Exception {
        // Test multiple ORDER BY columns
        SlotRef slotRef1 = new SlotRef(new TableName(null, null), "Alive");
        SlotRef slotRef2 = new SlotRef(new TableName(null, null), "Host");
        OrderByElement orderBy1 = new OrderByElement(slotRef1, true, false);
        OrderByElement orderBy2 = new OrderByElement(slotRef2, true, false);
        List<OrderByElement> orderByElements = Lists.newArrayList(orderBy1, orderBy2);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, null, orderByElements, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(5, result.getResultRows().size());
        // First should be false entries sorted by host, then true entries sorted by host
        Assert.assertEquals("false", result.getResultRows().get(0).get(3));
        Assert.assertEquals("false", result.getResultRows().get(1).get(3));
        Assert.assertEquals("true", result.getResultRows().get(2).get(3));
        Assert.assertEquals("true", result.getResultRows().get(3).get(3));
        Assert.assertEquals("true", result.getResultRows().get(4).get(3));
    }

    @Test
    public void testLimitOnly() throws Exception {
        // Test LIMIT only
        LimitElement limit = new LimitElement(3);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, null, null, limit, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(3, result.getResultRows().size());
        Assert.assertEquals("1", result.getResultRows().get(0).get(0));
        Assert.assertEquals("2", result.getResultRows().get(1).get(0));
        Assert.assertEquals("3", result.getResultRows().get(2).get(0));
    }

    @Test
    public void testLimitWithOffset() throws Exception {
        // Test LIMIT with OFFSET
        LimitElement limit = new LimitElement(2, 2);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, null, null, limit, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(2, result.getResultRows().size());
        Assert.assertEquals("3", result.getResultRows().get(0).get(0));
        Assert.assertEquals("4", result.getResultRows().get(1).get(0));
    }

    @Test
    public void testCombinedClauses() throws Exception {
        // Test combination of WHERE, ORDER BY, and LIMIT
        SlotRef slotRef = new SlotRef(new TableName(null, null), "Alive");
        StringLiteral literal = new StringLiteral("true");
        BinaryPredicate predicate = new BinaryPredicate(BinaryType.EQ, slotRef, literal);
        
        SlotRef orderSlotRef = new SlotRef(new TableName(null, null), "Host");
        OrderByElement orderBy = new OrderByElement(orderSlotRef, false, false);
        List<OrderByElement> orderByElements = Lists.newArrayList(orderBy);
        
        LimitElement limit = new LimitElement(2);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, predicate, orderByElements, limit, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(2, result.getResultRows().size());
        // Should be alive=true entries, ordered by host descending, limited to 2
        for (List<String> row : result.getResultRows()) {
            Assert.assertEquals("true", row.get(3));
        }
        // First should be test.example.com, then host3.example.com (descending order)
        Assert.assertEquals("test.example.com", result.getResultRows().get(0).get(1));
        Assert.assertEquals("host3.example.com", result.getResultRows().get(1).get(1));
    }

    @Test
    public void testLikeAndWhereCombined() throws Exception {
        // Test LIKE pattern and WHERE clause combined
        SlotRef slotRef = new SlotRef(new TableName(null, null), "Alive");
        StringLiteral literal = new StringLiteral("true");
        BinaryPredicate predicate = new BinaryPredicate(BinaryType.EQ, slotRef, literal);
        
        ShowBackendsStmt statement = new ShowBackendsStmt("host%", predicate, null, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(2, result.getResultRows().size());
        // Should be host% entries that are also alive=true
        Assert.assertEquals("1", result.getResultRows().get(0).get(0));
        Assert.assertEquals("3", result.getResultRows().get(1).get(0));
        for (List<String> row : result.getResultRows()) {
            Assert.assertTrue(row.get(1).startsWith("host"));
            Assert.assertEquals("true", row.get(3));
        }
    }

    @Test
    public void testEmptyResults() {
        // Test with empty data
        List<List<String>> emptyData = Lists.newArrayList();
        ShowBackendsStmt statement = new ShowBackendsStmt();
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, emptyData);
        
        Assert.assertEquals(0, result.getResultRows().size());
    }

    @Test
    public void testInvalidColumnInWhere() throws Exception {
        // Test WHERE clause with invalid column name - should return all rows
        SlotRef slotRef = new SlotRef(new TableName(null, null), "InvalidColumn");
        StringLiteral literal = new StringLiteral("true");
        BinaryPredicate predicate = new BinaryPredicate(BinaryType.EQ, slotRef, literal);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, predicate, null, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        // Should return all rows since invalid column defaults to true
        Assert.assertEquals(5, result.getResultRows().size());
    }

    @Test
    public void testInvalidColumnInOrderBy() throws Exception {
        // Test ORDER BY with invalid column name - should return original order
        SlotRef slotRef = new SlotRef(new TableName(null, null), "InvalidColumn");
        OrderByElement orderBy = new OrderByElement(slotRef, true, false);
        List<OrderByElement> orderByElements = Lists.newArrayList(orderBy);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, null, orderByElements, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(5, result.getResultRows().size());
        // Should maintain original order
        Assert.assertEquals("1", result.getResultRows().get(0).get(0));
        Assert.assertEquals("2", result.getResultRows().get(1).get(0));
    }

    @Test
    public void testNumericSorting() throws Exception {
        // Test numeric sorting (BackendId column)
        SlotRef slotRef = new SlotRef(new TableName(null, null), "BackendId");
        OrderByElement orderBy = new OrderByElement(slotRef, true, false);
        List<OrderByElement> orderByElements = Lists.newArrayList(orderBy);
        
        ShowBackendsStmt statement = new ShowBackendsStmt(null, null, orderByElements, null, NodePosition.ZERO);
        ShowResultSet result = ShowResultProcessor.processShowResult(statement, metaData, testData);
        
        Assert.assertEquals(5, result.getResultRows().size());
        // Should be sorted numerically: 1, 2, 3, 4, 5
        for (int i = 0; i < 5; i++) {
            Assert.assertEquals(String.valueOf(i + 1), result.getResultRows().get(i).get(0));
        }
    }
}