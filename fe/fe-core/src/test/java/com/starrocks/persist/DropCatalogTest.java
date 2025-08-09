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


package com.starrocks.persist;

import com.starrocks.connector.ConnectorMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;

public class DropCatalogTest {

    private String fileName = "./DropCatalogTest";
    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @AfterEach
    public void tearDownDrop() throws Exception {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testDropCatalog() throws Exception {
        String dropSql = "DROP CATALOG IF EXISTS hive_catalog";

        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        ConnectorMgr connectorMgr = GlobalStateMgr.getCurrentState().getConnectorMgr();

        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        Assertions.assertFalse(catalogMgr.catalogExists("hive_catalog"));
        Assertions.assertFalse(connectorMgr.connectorExists("hive_catalog"));

        StatementBase dropStmtBase = AnalyzeTestUtil.analyzeSuccess(dropSql);
        Assertions.assertEquals("DROP CATALOG IF EXISTS \'hive_catalog\'", dropStmtBase.toSql());
        Assertions.assertTrue(dropStmtBase instanceof DropCatalogStmt);
        DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) dropStmtBase;
        DDLStmtExecutor.execute(dropCatalogStmt, connectCtx);
        Assertions.assertFalse(catalogMgr.catalogExists("hive_catalog"));
        Assertions.assertFalse(connectorMgr.connectorExists("hive_catalog"));

    }
}
