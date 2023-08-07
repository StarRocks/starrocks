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

    @Test
    public void testCreateResourceStmtToString() {
        CreateResourceStmt stmt = (CreateResourceStmt) analyzeSuccess(
                "CREATE EXTERNAL RESOURCE \"spark0\" PROPERTIES " +
                        "( \"type\" = \"spark\", \"spark.master\" = \"yarn\", \"spark.submit.deployMode\" = \"cluster\"," +
                        " \"spark.jars\" = \"xxx.jar,yyy.jar\", \"spark.files\" = \"/tmp/aaa,/tmp/bbb\", " +
                        "\"spark.executor.memory\" = \"1g\", \"spark.yarn.queue\" = \"queue0\"," +
                        "\"spark.hadoop.yarn.resourcemanager.address\" = \"resourcemanager_host:8032\"," +
                        "\"spark.hadoop.fs.defaultFS\" = \"hdfs://namenode_host:9000\", " +
                        "\"working_dir\" = \"hdfs://namenode_host:9000/tmp/starrocks\", " +
                        "\"broker\" = \"broker0\", \"broker.username\" = \"user0\", \"broker.password\" = \"password0\" )");

        Assert.assertEquals("CREATE EXTERNAL RESOURCE spark0 PROPERTIES (\"spark.executor.memory\" = \"1g\"," +
                " \"spark.master\" = \"yarn\", \"working_dir\" = \"hdfs://namenode_host:9000/tmp/starrocks\", \"broker.password\" = \"***\"," +
                " \"spark.submit.deployMode\" = \"cluster\", \"type\" = \"spark\", \"broker\" = \"broker0\"," +
                " \"spark.hadoop.yarn.resourcemanager.address\" = \"resourcemanager_host:8032\"," +
                " \"spark.files\" = \"/tmp/aaa,/tmp/bbb\", \"spark.hadoop.fs.defaultFS\" = \"hdfs://namenode_host:9000\"," +
                " \"spark.yarn.queue\" = \"queue0\", \"broker.username\" = \"user0\"," +
                " \"spark.jars\" = \"xxx.jar,yyy.jar\")", AstToStringBuilder.toString(stmt));
    }
}
