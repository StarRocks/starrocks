package com.starrocks.sql.analyzer;

import com.starrocks.analysis.AlterResourceStmt;
import com.starrocks.analysis.CreateResourceStmt;
import com.starrocks.analysis.DropResourceStmt;
import com.starrocks.analysis.ShowResourcesStmt;
import com.starrocks.catalog.Resource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

/**
 * TEST :
 *  [Create | Alter | Drop | Show ] Resource
 */
public class ResourceStmtTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testCreateResource() {
        CreateResourceStmt stmt = (CreateResourceStmt) analyzeSuccess("create external resource 'spark0' PROPERTIES(\"type\"  =  \"spark\")");
        Assert.assertTrue( stmt.isSupportNewPlanner());
        // spark resource
        Assert.assertEquals("spark0", stmt.getResourceName());
        Assert.assertEquals(Resource.ResourceType.SPARK, stmt.getResourceType());
        Assert.assertEquals("CREATE EXTERNAL RESOURCE 'spark0' PROPERTIES(\"type\"  =  \"spark\")", stmt.toString());

        // hive resource
        stmt = (CreateResourceStmt) analyzeSuccess("create external resource 'hive0' PROPERTIES(\"type\"  =  \"hive\")");
        Assert.assertEquals("hive0", stmt.getResourceName());
        Assert.assertEquals(Resource.ResourceType.HIVE, stmt.getResourceType());
        Assert.assertEquals("CREATE EXTERNAL RESOURCE 'hive0' PROPERTIES(\"type\"  =  \"hive\")", stmt.toString());

        // JDBC resource
        stmt = (CreateResourceStmt) analyzeSuccess("create external resource jdbc0 properties (\n" +
                "    \"type\"=\"jdbc\",\n" +
                "    \"user\"=\"postgres\",\n" +
                "    \"password\"=\"changeme\"\n"+
                ");");
        Assert.assertEquals("jdbc0", stmt.getResourceName());
        Assert.assertEquals("postgres", stmt.getProperties().get("user"));
        Assert.assertEquals(Resource.ResourceType.JDBC, stmt.getResourceType());
        Assert.assertEquals("CREATE EXTERNAL RESOURCE 'jdbc0' PROPERTIES(" +
                "\"password\"  =  \"changeme\", " +
                "\"type\"  =  \"jdbc\", " +
                "\"user\"  =  \"postgres\")", stmt.toString());

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
        Assert.assertTrue( stmt.isSupportNewPlanner());
        Assert.assertEquals("SHOW RESOURCES", stmt.toString());
        Assert.assertNotNull(stmt.getMetaData());
        Assert.assertNotNull(stmt.getRedirectStatus());

        analyzeFail("show resource");
    }
    @Test
    public void testAlterResourceTest() {
        AlterResourceStmt stmt = (AlterResourceStmt) analyzeSuccess("alter RESOURCE hive0 SET PROPERTIES (\"hive.metastore.uris\" = \"thrift://10.10.44.91:9083\")");
        Assert.assertTrue( stmt.isSupportNewPlanner());
        Assert.assertEquals("ALTER RESOURCE 'hive0' SET PROPERTIES(\"hive.metastore.uris\" = \"thrift://10.10.44.91:9083\")", stmt.toString());
        Assert.assertEquals("hive0", stmt.getResourceName());
        Assert.assertEquals("thrift://10.10.44.91:9083", stmt.getProperties().get("hive.metastore.uris"));
        // bad cases: name, type, properties
        analyzeFail("alter resource 00hudi SET PROPERTIES(\"password\"  =  \"hudi\")");
        analyzeFail("ALTER RESOURCE hive0 SET PROPERTIES (\"type\" = \"hudi\")");
        analyzeFail("ALTER RESOURCE hive0");
    }

    @Test
    public void testDropResourceTest() {
        DropResourceStmt stmt = (DropResourceStmt)  analyzeSuccess("Drop Resource 'hive0'");
        Assert.assertTrue(stmt.isSupportNewPlanner());
        Assert.assertEquals("DROP RESOURCE 'hive0'", stmt.toString());
        Assert.assertEquals("hive0", stmt.getResourceName());
        // bad case for resource name
        analyzeFail("drop resource 01hive");
    }
}
