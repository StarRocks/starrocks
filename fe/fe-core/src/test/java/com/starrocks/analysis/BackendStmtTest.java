// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddBackendClause;
import com.starrocks.sql.ast.AlterSystemStmt;
import com.starrocks.sql.ast.BackendClause;
import com.starrocks.sql.ast.DropBackendClause;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class BackendStmtTest {

    @BeforeAll
    public static void setUp() throws Exception {
    }

    public BackendClause createStmt(int type) {
        BackendClause stmt = null;
        switch (type) {
            case 1:
                // missing ip
                stmt = new AddBackendClause(Lists.newArrayList(":12346"),
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
                break;
            case 2:
                // invalid ip
                stmt = new AddBackendClause(Lists.newArrayList("asdasd:12345"),
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
                break;
            case 3:
                // invalid port
                stmt = new AddBackendClause(Lists.newArrayList("10.1.2.3:123467"),
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
                break;
            case 4:
                // normal add
                stmt = new AddBackendClause(Lists.newArrayList("192.168.1.1:12345"),
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
                break;
            case 5:
                // normal remove
                stmt = new DropBackendClause(Lists.newArrayList("192.168.1.2:12345"), true,
                        WarehouseManager.DEFAULT_WAREHOUSE_NAME);
                break;
            default:
                break;
        }
        return stmt;
    }

    @Test
    public void initBackendsTest1() {
        assertThrows(SemanticException.class, () -> {
            BackendClause stmt = createStmt(1);
            com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext());
        });
    }

    @Test
    public void initBackendsTest3() {
        assertThrows(SemanticException.class, () -> {
            BackendClause stmt = createStmt(3);
            com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext());

        });

    }

    @Test
    public void initBackendsTest4() throws Exception {
        BackendClause stmt = createStmt(4);
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext());

        Assertions.assertEquals("[192.168.1.1:12345]", stmt.getHostPortPairs().toString());
    }

    @Test
    public void initBackendsTest5() throws Exception {
        BackendClause stmt = createStmt(5);
        com.starrocks.sql.analyzer.Analyzer.analyze(new AlterSystemStmt(stmt), new ConnectContext());

        Assertions.assertEquals("[192.168.1.2:12345]", stmt.getHostPortPairs().toString());
    }
}
