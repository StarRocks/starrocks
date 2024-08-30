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

import com.starrocks.analysis.TableName;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.plan.PlanTestBase;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ShowEmptyTest extends PlanTestBase {
    @Test
    public void testShowPartitions() {
        // Ok to test
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("test", "test_all_type_partition_by_datetime"),
                null, null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, starRocksAssert.getCtx());

        ShowResultSet resultSet = ShowExecutor.execute(stmt, starRocksAssert.getCtx());
        System.out.println(resultSet.getResultRows());

        // Ready to Assert

        String partitionKeyTitle = resultSet.getMetaData().getColumn(6).getName();
        Assert.assertEquals(partitionKeyTitle, "PartitionKey");
        String valuesTitle = resultSet.getMetaData().getColumn(7).getName();
        Assert.assertEquals(valuesTitle, "Range");

        String partitionKey1 = resultSet.getResultRows().get(0).get(6);
        Assert.assertEquals(partitionKey1, "id_datetime");
        String partitionKey2 = resultSet.getResultRows().get(1).get(6);
        Assert.assertEquals(partitionKey2, "id_datetime");

        String values1 = resultSet.getResultRows().get(0).get(7);
        Assert.assertEquals(values1, "[types: [DATETIME]; keys: [1991-01-01 00:00:00]; " +
                "..types: [DATETIME]; keys: [1991-01-01 12:00:00]; )");
        String values2 = resultSet.getResultRows().get(1).get(7);
        Assert.assertEquals(values2, "[types: [DATETIME]; keys: [1991-01-01 12:00:00]; " +
                "..types: [DATETIME]; keys: [1992-01-02 00:00:00]; )");
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testShowMaterializedViewFromUnknownDatabase() throws DdlException, AnalysisException {
        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("emptyDb", (String) null);

        expectedEx.expect(SemanticException.class);
        expectedEx.expectMessage("Unknown database 'emptyDb'");
        ShowExecutor.execute(stmt, starRocksAssert.getCtx());
    }

    @Test(expected = SemanticException.class)
    public void testShowCreateTableEmptyDb() throws SemanticException, DdlException {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName("emptyDb", "testTable"),
                ShowCreateTableStmt.CreateTableType.TABLE);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, starRocksAssert.getCtx());

        Assert.fail("No Exception throws.");
    }
}
