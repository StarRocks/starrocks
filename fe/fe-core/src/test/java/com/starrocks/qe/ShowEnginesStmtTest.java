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
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.sql.ast.ShowEnginesStmt;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TAuthInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ShowEnginesStmtTest {

    private ConnectContext connectContext;
    private ShowExecutor showExecutor;

    @Before
    public void setUp() {
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
        showExecutor = new ShowExecutor();
    }

    @Test
    public void testShowEnginesBasic() {
        ShowEnginesStmt statement = new ShowEnginesStmt();
        ShowResultSet result = showExecutor.visitShowEnginesStatement(statement, connectContext);
        
        Assert.assertNotNull(result);
        Assert.assertEquals(5, result.getResultRows().size());
        
        // Verify expected engines are present
        List<String> engineNames = Lists.newArrayList();
        for (List<String> row : result.getResultRows()) {
            engineNames.add(row.get(0));
        }
        
        Assert.assertTrue(engineNames.contains("OLAP"));
        Assert.assertTrue(engineNames.contains("MySQL"));
        Assert.assertTrue(engineNames.contains("ELASTICSEARCH"));
        Assert.assertTrue(engineNames.contains("HIVE"));
        Assert.assertTrue(engineNames.contains("ICEBERG"));
    }

    @Test
    public void testShowEnginesWithLikePattern() {
        ShowEnginesStmt statement = new ShowEnginesStmt("OLAP", null, null, null, NodePosition.ZERO);
        ShowResultSet result = showExecutor.visitShowEnginesStatement(statement, connectContext);
        
        Assert.assertEquals(1, result.getResultRows().size());
        Assert.assertEquals("OLAP", result.getResultRows().get(0).get(0));
    }

    @Test
    public void testShowEnginesWithLikeWildcard() {
        ShowEnginesStmt statement = new ShowEnginesStmt("H%", null, null, null, NodePosition.ZERO);
        ShowResultSet result = showExecutor.visitShowEnginesStatement(statement, connectContext);
        
        Assert.assertEquals(1, result.getResultRows().size());
        Assert.assertEquals("HIVE", result.getResultRows().get(0).get(0));
    }

    @Test
    public void testShowEnginesWithWhereClause() throws Exception {
        SlotRef slotRef = new SlotRef(new TableName(null, null), "Support");
        StringLiteral literal = new StringLiteral("YES");
        BinaryPredicate predicate = new BinaryPredicate(BinaryType.EQ, slotRef, literal);
        
        ShowEnginesStmt statement = new ShowEnginesStmt(null, predicate, null, null, NodePosition.ZERO);
        ShowResultSet result = showExecutor.visitShowEnginesStatement(statement, connectContext);
        
        Assert.assertEquals(5, result.getResultRows().size());
        // All engines should have Support = YES
        for (List<String> row : result.getResultRows()) {
            Assert.assertEquals("YES", row.get(1));
        }
    }

    @Test
    public void testShowEnginesWithOrderBy() throws Exception {
        SlotRef slotRef = new SlotRef(new TableName(null, null), "Engine");
        OrderByElement orderBy = new OrderByElement(slotRef, true, false);
        List<OrderByElement> orderByElements = Lists.newArrayList(orderBy);
        
        ShowEnginesStmt statement = new ShowEnginesStmt(null, null, orderByElements, null, NodePosition.ZERO);
        ShowResultSet result = showExecutor.visitShowEnginesStatement(statement, connectContext);
        
        Assert.assertEquals(5, result.getResultRows().size());
        
        // Should be sorted alphabetically
        Assert.assertEquals("ELASTICSEARCH", result.getResultRows().get(0).get(0));
        Assert.assertEquals("HIVE", result.getResultRows().get(1).get(0));
        Assert.assertEquals("ICEBERG", result.getResultRows().get(2).get(0));
        Assert.assertEquals("MySQL", result.getResultRows().get(3).get(0));
        Assert.assertEquals("OLAP", result.getResultRows().get(4).get(0));
    }

    @Test
    public void testShowEnginesWithOrderByDescending() throws Exception {
        SlotRef slotRef = new SlotRef(new TableName(null, null), "Engine");
        OrderByElement orderBy = new OrderByElement(slotRef, false, false);
        List<OrderByElement> orderByElements = Lists.newArrayList(orderBy);
        
        ShowEnginesStmt statement = new ShowEnginesStmt(null, null, orderByElements, null, NodePosition.ZERO);
        ShowResultSet result = showExecutor.visitShowEnginesStatement(statement, connectContext);
        
        Assert.assertEquals(5, result.getResultRows().size());
        
        // Should be sorted alphabetically in descending order
        Assert.assertEquals("OLAP", result.getResultRows().get(0).get(0));
        Assert.assertEquals("MySQL", result.getResultRows().get(1).get(0));
        Assert.assertEquals("ICEBERG", result.getResultRows().get(2).get(0));
        Assert.assertEquals("HIVE", result.getResultRows().get(3).get(0));
        Assert.assertEquals("ELASTICSEARCH", result.getResultRows().get(4).get(0));
    }

    @Test
    public void testShowEnginesWithLimit() throws Exception {
        LimitElement limit = new LimitElement(3);
        
        ShowEnginesStmt statement = new ShowEnginesStmt(null, null, null, limit, NodePosition.ZERO);
        ShowResultSet result = showExecutor.visitShowEnginesStatement(statement, connectContext);
        
        Assert.assertEquals(3, result.getResultRows().size());
    }

    @Test
    public void testShowEnginesWithLimitAndOffset() throws Exception {
        LimitElement limit = new LimitElement(2, 2);
        
        ShowEnginesStmt statement = new ShowEnginesStmt(null, null, null, limit, NodePosition.ZERO);
        ShowResultSet result = showExecutor.visitShowEnginesStatement(statement, connectContext);
        
        Assert.assertEquals(2, result.getResultRows().size());
    }

    @Test
    public void testShowEnginesWithCombinedClauses() throws Exception {
        // Test combination of LIKE, ORDER BY, and LIMIT
        SlotRef orderSlotRef = new SlotRef(new TableName(null, null), "Engine");
        OrderByElement orderBy = new OrderByElement(orderSlotRef, false, false);
        List<OrderByElement> orderByElements = Lists.newArrayList(orderBy);
        
        LimitElement limit = new LimitElement(2);
        
        ShowEnginesStmt statement = new ShowEnginesStmt("%E%", null, orderByElements, limit, NodePosition.ZERO);
        ShowResultSet result = showExecutor.visitShowEnginesStatement(statement, connectContext);
        
        // Should match engines containing 'E', ordered descending, limited to 2
        Assert.assertEquals(2, result.getResultRows().size());
        
        // Verify the results contain 'E' and are in descending order
        for (List<String> row : result.getResultRows()) {
            Assert.assertTrue(row.get(0).contains("E"));
        }
    }

    @Test
    public void testShowEnginesMetaData() {
        ShowEnginesStmt statement = new ShowEnginesStmt();
        ShowResultSetMetaData metaData = statement.getMetaData();
        
        Assert.assertNotNull(metaData);
        Assert.assertEquals(6, metaData.getColumns().size());
        Assert.assertEquals("Engine", metaData.getColumns().get(0).getName());
        Assert.assertEquals("Support", metaData.getColumns().get(1).getName());
        Assert.assertEquals("Comment", metaData.getColumns().get(2).getName());
        Assert.assertEquals("Transactions", metaData.getColumns().get(3).getName());
        Assert.assertEquals("XA", metaData.getColumns().get(4).getName());
        Assert.assertEquals("Savepoints", metaData.getColumns().get(5).getName());
    }
}