// This file is made available under Elastic License 2.0.

package com.starrocks.catalog;

import com.google.common.collect.Maps;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.CreateResourceStmt;
import com.starrocks.common.UserException;
import com.starrocks.external.iceberg.IcebergCatalogType;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergResourceTest {
    private Analyzer analyzer;

    @Before
    public void setUp() {
        analyzer = AccessTestUtil.fetchAdminAnalyzer();
    }

    @Test
    public void testFromStmt(@Mocked GlobalStateMgr globalStateMgr, @Injectable Auth auth) throws UserException {
        new Expectations() {
            {
                globalStateMgr.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        String name = "iceberg0";
        String type = "iceberg";
        String catalogType = "HIVE";
        String metastoreURIs = "thrift://127.0.0.1:9380";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("iceberg.catalog.type", catalogType);
        properties.put("iceberg.catalog.hive.metastore.uris", metastoreURIs);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        IcebergResource resource = (IcebergResource) Resource.fromStmt(stmt);
        Assert.assertEquals("iceberg0", resource.getName());
        Assert.assertEquals(type, resource.getType().name().toLowerCase());
        Assert.assertEquals(IcebergCatalogType.fromString(catalogType), resource.getCatalogType());
        Assert.assertEquals(metastoreURIs, resource.getHiveMetastoreURIs());
        Map<String, String> newURI = new HashMap<>();
        newURI.put("iceberg.catalog.hive.metastore.uris", "thrift://127.0.0.2:9380");
        resource.alterProperties(newURI);
        Assert.assertEquals("thrift://127.0.0.2:9380", resource.getHiveMetastoreURIs());
    }

    @Test
    public void testCustomStmt(@Mocked GlobalStateMgr globalStateMgr, @Injectable Auth auth) throws UserException {
        new Expectations() {
            {
                globalStateMgr.getAuth();
                result = auth;
                auth.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        String name = "iceberg1";
        String type = "iceberg";
        String catalogType = "CUSTOM";
        String catalogImpl = "com.starrocks.external.iceberg.IcebergHiveCatalog";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("type", type);
        properties.put("iceberg.catalog.type", catalogType);
        properties.put("iceberg.catalog-impl", catalogImpl);
        CreateResourceStmt stmt = new CreateResourceStmt(true, name, properties);
        stmt.analyze(analyzer);
        IcebergResource resource = (IcebergResource) Resource.fromStmt(stmt);
        Assert.assertEquals("iceberg1", resource.getName());
        Assert.assertEquals(type, resource.getType().name().toLowerCase());
        Assert.assertEquals(IcebergCatalogType.fromString(catalogType), resource.getCatalogType());
        Assert.assertEquals(catalogImpl, resource.getIcebergImpl());
    }

    @Test
    public void testSerialization() throws Exception {
        Resource resource = new IcebergResource("iceberg0");
        String metastoreURIs = "thrift://127.0.0.1:9380";
        String catalogType = "HIVE";
        Map<String, String> properties = Maps.newHashMap();
        properties.put("iceberg.catalog.hive.metastore.uris", metastoreURIs);
        properties.put("iceberg.catalog.type", catalogType);
        resource.setProperties(properties);

        String json = GsonUtils.GSON.toJson(resource);
        Resource resource2 = GsonUtils.GSON.fromJson(json, Resource.class);
        Assert.assertTrue(resource2 instanceof IcebergResource);
        Assert.assertEquals(metastoreURIs, ((IcebergResource) resource2).getHiveMetastoreURIs());
    }
}
