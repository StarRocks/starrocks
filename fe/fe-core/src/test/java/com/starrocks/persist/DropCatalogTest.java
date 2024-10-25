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
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class DropCatalogTest {

    private String fileName = "./DropCatalogTest";
    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
    }

    @After
    public void tearDownDrop() throws Exception {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testNormal() throws IOException {
        // 1. Write objects to file
        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));

        DropCatalogLog dropCatalogLog =
                new DropCatalogLog("catalog_name");
        dropCatalogLog.write(out);
        out.flush();
        out.close();

        // 2. Read objects from file
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        DropCatalogLog readDropCatalogInfo = DropCatalogLog.read(in);
        Assert.assertEquals(readDropCatalogInfo.getCatalogName(), "catalog_name");
        in.close();
    }
    @Test
    public void testDropCatalog() throws Exception {
        String dropSql = "DROP CATALOG IF EXISTS hive_catalog";

        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        ConnectorMgr connectorMgr = GlobalStateMgr.getCurrentState().getConnectorMgr();

        ConnectContext connectCtx = new ConnectContext();
        connectCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());

        Assert.assertFalse(catalogMgr.catalogExists("hive_catalog"));
        Assert.assertFalse(connectorMgr.connectorExists("hive_catalog"));

        StatementBase dropStmtBase = AnalyzeTestUtil.analyzeSuccess(dropSql);
        Assert.assertEquals("DROP CATALOG IF EXISTS \'hive_catalog\'", dropStmtBase.toSql());
        Assert.assertTrue(dropStmtBase instanceof DropCatalogStmt);
        DropCatalogStmt dropCatalogStmt = (DropCatalogStmt) dropStmtBase;
        DDLStmtExecutor.execute(dropCatalogStmt, connectCtx);
        Assert.assertFalse(catalogMgr.catalogExists("hive_catalog"));
        Assert.assertFalse(connectorMgr.connectorExists("hive_catalog"));

    }
}
