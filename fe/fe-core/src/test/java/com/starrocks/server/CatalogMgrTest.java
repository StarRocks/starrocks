// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.server;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.common.DdlException;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wildfly.common.Assert;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CatalogMgrTest {
    private static StarRocksAssert starRocksAssert;
    private String fileName = "./testCatalogMgr";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0.1:9083\")";
        starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);
    }

    @After
    public void tearDownCreate() throws Exception {
        File file = new File(fileName);
        file.delete();
    }

    @Test
    public void testReplay() throws DdlException {
        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        Map<String, String> config = new HashMap<>();
        config.put("type", "hive");
        config.put("hive.metastore.uris", "thrift://127.0.0.1:9083");

        Catalog catalog = new ExternalCatalog(10000, "catalog_1", "", config);
        catalogMgr.replayCreateCatalog(catalog);
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists("catalog_1"));
        Assert.assertTrue(GlobalStateMgr.getCurrentState().getConnectorMgr().connectorExists("catalog_1"));

        DropCatalogLog log = new DropCatalogLog("catalog_1");
        catalogMgr.replayDropCatalog(log);
        Assert.assertFalse(GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists("catalog_1"));
        Assert.assertFalse(GlobalStateMgr.getCurrentState().getConnectorMgr().connectorExists("catalog_1"));

        config.put("type", "hhhhhhive");
        config.put("hive.metastore.uris", "thrift://127.0.0.1:9083");

        catalog = new ExternalCatalog(10000, "catalog_2", "", config);
        catalogMgr.replayCreateCatalog(catalog);
        Assert.assertFalse(GlobalStateMgr.getCurrentState().getCatalogMgr().catalogExists("catalog_1"));
        Assert.assertFalse(GlobalStateMgr.getCurrentState().getConnectorMgr().connectorExists("catalog_1"));
<<<<<<< HEAD
=======

        config.put("type", "paimon");
        final ExternalCatalog catalog1 = new ExternalCatalog(10000, "catalog_3", "", config);
        Assert.assertThrows(DdlException.class, () -> {
            catalogMgr.replayCreateCatalog(catalog1);
        });
    }

    @Test
    public void testCreate() throws DdlException {
        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        Map<String, String> config = new HashMap<>();

        config.put("type", "paimon");
        final ExternalCatalog catalog = new ExternalCatalog(10000, "catalog_0", "", config);
        Assert.assertThrows(DdlException.class, () -> {
            catalogMgr.replayCreateCatalog(catalog);
        });
    }

    @Test
    public void testCreateExceptionMsg() {
        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        Map<String, String> config = new HashMap<>();

        config.put("type", "jdbc");

        try {
            catalogMgr.createCatalog("jdbc", "a", "", config);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("Missing"));
        }

        config.put("type", "test_unsupported");

        Assert.assertThrows(DdlException.class, () -> {
            catalogMgr.createCatalog("test_unsupported", "b", "", config);
        });
>>>>>>> 2cf96cb785 ([BugFix]keep connector/catalog map in consistent as much as possible (#46995))
    }

    @Test
    public void testLoadCatalog() throws IOException, DdlException {
        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        Assert.assertTrue(catalogMgr.catalogExists("hive_catalog"));

        File file = new File(fileName);
        file.createNewFile();
        DataOutputStream out = new DataOutputStream(new FileOutputStream(file));
        catalogMgr.saveCatalogs(out, 0);
        out.flush();
        out.close();

        catalogMgr.dropCatalog(new DropCatalogStmt("hive_catalog"));
        Assert.assertFalse(catalogMgr.catalogExists("hive_catalog"));

        DataInputStream in = new DataInputStream(new FileInputStream(file));
        catalogMgr.loadCatalogs(in, 0);
        Assert.assertTrue(catalogMgr.catalogExists("hive_catalog"));
    }

}
