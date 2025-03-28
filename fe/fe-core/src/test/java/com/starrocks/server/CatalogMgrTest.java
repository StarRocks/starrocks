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


package com.starrocks.server;

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.MockUp;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
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

        config.put("type", "paimon");
        final ExternalCatalog catalog1 = new ExternalCatalog(10000, "catalog_3", "", config);
        catalogMgr.replayCreateCatalog(catalog1);
    }

    @Test
    public void testCreate() throws DdlException {
        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        Map<String, String> config = new HashMap<>();

        config.put("type", "paimon");
        final ExternalCatalog catalog = new ExternalCatalog(10000, "catalog_0", "", config);
        catalogMgr.replayCreateCatalog(catalog);
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
    }

    @Test
    public void testLoadCatalogWithException() throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        CatalogMgr catalogMgr = GlobalStateMgr.getCurrentState().getCatalogMgr();
        Assert.assertTrue(catalogMgr.catalogExists("hive_catalog"));

        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        catalogMgr.save(image.getImageWriter());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(image.getJsonReader());

        CatalogMgr loadCatalogMgr = new CatalogMgr(new ConnectorMgr());
        loadCatalogMgr.load(reader);
        Assert.assertTrue(loadCatalogMgr.catalogExists("hive_catalog"));

        // test load with ddl exception
        loadCatalogMgr = new CatalogMgr(new ConnectorMgr());
        reader = new SRMetaBlockReaderV2(image.getJsonReader());
        new MockUp<CatalogMgr>() {
            @mockit.Mock
            public void replayCreateCatalog(Catalog catalog) throws DdlException {
                throw new DdlException("create catalog failed");
            }
        };

        try {
            loadCatalogMgr.load(reader);
        } catch (Exception e) {
            // should not throw exception
            Assert.fail();
        }
        Assert.assertFalse(loadCatalogMgr.catalogExists("hive_catalog"));
    }
}
