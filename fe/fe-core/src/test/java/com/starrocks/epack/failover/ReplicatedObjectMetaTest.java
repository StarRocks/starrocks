// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover;

import com.starrocks.epack.sql.ast.CreatePrimaryFailoverGroupStmt;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class ReplicatedObjectMetaTest {
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        starRocksAssert = new StarRocksAssert(AnalyzeTestUtil.getConnectContext());
        starRocksAssert.withDatabase("test").useDatabase("test");
    }

    @Test
    public void testSerialization() throws Exception {
        CreatePrimaryFailoverGroupStmt stmt = (CreatePrimaryFailoverGroupStmt) analyzeSuccess(
                "CREATE FAILOVER GROUP test_group1 " +
                    "CATALOGS = default_catalog " +
                    "MEMBERS = " +
                        "'az1:SELF'," +
                        "'az2:192.168.0.1:9090'" +
                    "SCHEDULE = '1h'");

        ReplicatedObjectMgr objectMgr = new ReplicatedObjectMgr(stmt);
        ReplicatedObjectMeta objectMeta = objectMgr.saveToObjectMeta();

        for (Long catalogId : objectMeta.getCatalogMetas().keySet()) {
            ReplicatedObjectMeta.CatalogMeta catalogMeta = objectMeta.getCatalogMetas().get(catalogId);
            Assert.assertTrue(CatalogMgr.isInternalCatalog(catalogMeta.getCatalogId()));
            Assert.assertTrue(CatalogMgr.isInternalCatalog(catalogMeta.getCatalogName()));
            Assert.assertTrue(!catalogMeta.getDatabases().isEmpty());
        }
    }
}
