// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Resource;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import com.starrocks.sql.ast.ShowResourcesStmt;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

/**
 * TEST :
 * [Create | Alter | Drop | Show ] Resource
 */
public class ResourceStmtTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCreateResource() {
        CreateResourceStmt stmt = (CreateResourceStmt) analyzeSuccess(
                "create external resource 'spark0' PROPERTIES(\"type\"  =  \"spark\")");
        // spark resource
        Assert.assertEquals("spark0", stmt.getResourceName());
        Assert.assertEquals(Resource.ResourceType.SPARK, stmt.getResourceType());

        // hive resource
        stmt = (CreateResourceStmt) analyzeSuccess("create external resource 'hive0' PROPERTIES(\"type\"  =  \"hive\")");
        Assert.assertEquals("hive0", stmt.getResourceName());
        Assert.assertEquals(Resource.ResourceType.HIVE, stmt.getResourceType());

        // JDBC resource
        stmt = (CreateResourceStmt) analyzeSuccess("create external resource jdbc0 properties (\n" +
                "    \"type\"=\"jdbc\",\n" +
                "    \"user\"=\"postgres\",\n" +
                "    \"password\"=\"changeme\");");
        Assert.assertEquals("jdbc0", stmt.getResourceName());
        Assert.assertEquals("postgres", stmt.getProperties().get("user"));
        Assert.assertEquals(Resource.ResourceType.JDBC, stmt.getResourceType());

        // bad cases
        analyzeFail("create external resource '00hudi' PROPERTIES(\"type\"  =  \"hudi\")");
        analyzeFail("create resource 'hudi01' PROPERTIES(\"type\"  =  \"hudi\")");
        analyzeFail("create external resource 'hudi01'");
        analyzeFail("create external resource 'hudi01' PROPERTIES(\"amory\"  =  \"test\")");
        analyzeFail("create external resource 'hudi01' PROPERTIES(\"type\" = \"amory\")");
    }

    @Test
    public void testShowResourcesTest() {
        ShowResourcesStmt stmt = (ShowResourcesStmt) analyzeSuccess("Show Resources");
        Assert.assertNotNull(stmt.getMetaData());
        Assert.assertNotNull(stmt.getRedirectStatus());

        analyzeFail("show resource");
    }

    @Test
    public void testAlterResourceTest() {
        AlterResourceStmt stmt = (AlterResourceStmt) analyzeSuccess("alter RESOURCE hive0 SET PROPERTIES (\"hive.metastore.uris\" = \"thrift://10.10.44.91:9083\")");
        Assert.assertEquals("hive0", stmt.getResourceName());
        Assert.assertEquals("thrift://10.10.44.91:9083", stmt.getProperties().get("hive.metastore.uris"));
        // bad cases: name, type, properties
        analyzeFail("alter resource 00hudi SET PROPERTIES(\"password\"  =  \"hudi\")");
        analyzeFail("ALTER RESOURCE hive0 SET PROPERTIES (\"type\" = \"hudi\")");
        analyzeFail("ALTER RESOURCE hive0");
    }

    @Test
    public void testDropResourceTest() {
        DropResourceStmt stmt = (DropResourceStmt) analyzeSuccess("Drop Resource 'hive0'");
        Assert.assertEquals("hive0", stmt.getResourceName());
        // bad case for resource name
        analyzeFail("drop resource 01hive");
    }
}
