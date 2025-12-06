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
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MockedLocalMetaStore;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.OptimizeProcDir;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.ShowAlterStmtAnalyzer;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ShowAlterTest {
    @Test
    public void testShowAlterTable() throws AnalysisException, DdlException {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        LocalMetastore originalMetastore = globalStateMgr.getLocalMetastore();
        try {
            MockedLocalMetaStore localMetastore =
                    new MockedLocalMetaStore(globalStateMgr, globalStateMgr.getRecycleBin(), null);
            localMetastore.createDb("testDb");
            Database database = localMetastore.getDb("testDb");
            globalStateMgr.setLocalMetastore(localMetastore);

            List<List<Comparable>> jobInfos = Lists.newArrayList();
            jobInfos.add(Lists.newArrayList(
                    1L, "tbl1", "2024-01-01 10:00:00", "2024-01-01 11:00:00",
                    "OPTIMIZE", 100L, "FINISHED", "done", 100, 10000));
            jobInfos.add(Lists.newArrayList(
                    2L, "tbl2", "2024-02-01 10:00:00", "2024-02-01 11:00:00",
                    "OPTIMIZE", 200L, "RUNNING", "in progress", 50, 10000));

            new MockUp<SchemaChangeHandler>() {
                @Mock
                public List<List<Comparable>> getOptimizeJobInfosByDb(Database db) {
                    if (db.getId() == database.getId()) {
                        return jobInfos;
                    }
                    return Lists.newArrayList();
                }
            };

            ConnectContext ctx = new ConnectContext(null);
            Expr whereClause = new BinaryPredicate(
                    BinaryType.EQ, new SlotRef(null, "TableName"), new StringLiteral("tbl1"));
            ShowAlterStmt stmt = new ShowAlterStmt(ShowAlterStmt.AlterType.OPTIMIZE, "testDb", whereClause, null, null);
            ShowAlterStmtAnalyzer.analyze(stmt, ctx);
            ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

            Assertions.assertEquals("/jobs/" + database.getId() + "/optimize", stmt.getProcPath());
            Assertions.assertEquals(OptimizeProcDir.TITLE_NAMES.size(), resultSet.getMetaData().getColumnCount());
            Assertions.assertEquals(1, resultSet.getResultRows().size());

            List<String> firstRow = resultSet.getResultRows().get(0);
            Assertions.assertEquals("1", firstRow.get(0));
            Assertions.assertEquals("tbl1", firstRow.get(1));
            Assertions.assertEquals("OPTIMIZE", firstRow.get(4));
            Assertions.assertEquals("FINISHED", firstRow.get(6));
        } finally {
            globalStateMgr.setLocalMetastore(originalMetastore);
        }
    }
}
