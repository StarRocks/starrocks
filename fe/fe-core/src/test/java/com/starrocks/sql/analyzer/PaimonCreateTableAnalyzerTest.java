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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.connector.paimon.PaimonTableSchemaConverter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.parser.SqlParser;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class PaimonCreateTableAnalyzerTest {
    private ConnectContext context;

    @BeforeAll
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.initWithoutTableAndDb(RunMode.SHARED_NOTHING);
    }

    @BeforeEach
    public void mockExternalCatalog() {
        context = AnalyzeTestUtil.getConnectContext();
        CatalogMgr catalogs = GlobalStateMgr.getCurrentState().getCatalogMgr();
        MetadataMgr metadata = GlobalStateMgr.getCurrentState().getMetadataMgr();
        ExternalCatalog catalog = new ExternalCatalog(100001, "paimon_ddl", "", Map.of("type", "paimon"));
        new Expectations(catalogs, metadata) {
            {
                catalogs.catalogExists("paimon_ddl");
                result = true;
                minTimes = 0;
                catalogs.getCatalogType("paimon_ddl");
                result = "paimon";
                minTimes = 0;
                catalogs.getCatalogByName("paimon_ddl");
                result = catalog;
                minTimes = 0;
                metadata.getDb((ConnectContext) any, "paimon_ddl", "db");
                result = new Database(100002, "db");
                minTimes = 0;
                metadata.tableExists((ConnectContext) any, "paimon_ddl", "db", anyString);
                result = false;
                minTimes = 0;
            }
        };
    }

    @Test
    public void testInferredEngineUsesPaimonKeysAndPartitions() {
        CreateTableStmt stmt = analyze("create table paimon_ddl.db.t " +
                "(payload array<int>, Id bigint, dt date) primary key (Id, dt) partition by (DT) " +
                "properties ('bucket'='3', 'bucket-key'='Id', 'file.compression'='zstd')");
        Assertions.assertEquals("paimon", stmt.getEngineName());
        Assertions.assertFalse(stmt.getColumns().get(0).isKey());
        Assertions.assertTrue(stmt.getColumns().get(0).isAllowNull());
        Assertions.assertTrue(stmt.getColumns().get(1).isKey());
        Assertions.assertFalse(stmt.getColumns().get(1).isAllowNull());
        Assertions.assertNull(stmt.getColumns().get(0).getAggregationType());
        Assertions.assertEquals(3, stmt.getColumnDefs().size());
        Assertions.assertNull(stmt.getDistributionDesc());
        Assertions.assertFalse(stmt.getProperties().containsKey("replication_num"));
        Assertions.assertEquals(List.of("dt"), PaimonTableSchemaConverter.toSchema(stmt).partitionKeys());
    }

    @Test
    public void testPropertyPrimaryKeyMatchesNativePrimaryKey() {
        CreateTableStmt stmt = analyze("create table paimon_ddl.db.t (v varchar(17), Id int) " +
                "properties ('primary-key'='ID', 'bucket'='2')");
        Assertions.assertNull(stmt.getKeysDesc());
        Assertions.assertFalse(stmt.getColumns().get(0).isKey());
        Assertions.assertTrue(stmt.getColumns().get(1).isKey());
        Assertions.assertFalse(stmt.getColumns().get(1).isAllowNull());
        Assertions.assertEquals(List.of("Id"), PaimonTableSchemaConverter.toSchema(stmt).primaryKeys());
    }

    @Test
    public void testAppendCreateKeepsNullabilityAndSdkBucketDefault() {
        CreateTableStmt stmt = analyze("create table paimon_ddl.db.t (id int not null, dt date) partition by (dt)");
        Assertions.assertFalse(stmt.getColumns().get(0).isAllowNull());
        Assertions.assertTrue(stmt.getColumns().get(1).isAllowNull());
        Assertions.assertFalse(stmt.getColumns().get(0).isKey());
        Assertions.assertNull(stmt.getProperties());
        Assertions.assertFalse(PaimonTableSchemaConverter.toSchema(stmt).options().containsKey("bucket"));
    }

    @Test
    public void testCtasDoesNotInjectOlapOptionsOrMakePartitionColumnNotNull() {
        CreateTableAsSelectStmt ctas = (CreateTableAsSelectStmt) SqlParser.parse(
                "create table paimon_ddl.db.t partition by (dt) " +
                        "as select cast(null as int) as id, cast(null as date) as dt", 0L).get(0);
        CTASAnalyzer.analyze(ctas, context);
        CreateTableStmt stmt = ctas.getCreateTableStmt();
        Assertions.assertEquals("paimon", stmt.getEngineName());
        Assertions.assertNull(stmt.getProperties());
        Assertions.assertNull(stmt.getDistributionDesc());
        Assertions.assertEquals(2, stmt.getColumnDefs().size());
        Assertions.assertTrue(stmt.getColumns().get(1).isAllowNull());
        Assertions.assertInstanceOf(ListPartitionDesc.class, stmt.getPartitionDesc());
        Assertions.assertNotNull(ctas.getInsertStmt().getQueryStatement());
    }

    @Test
    public void testCtasPrimaryKeyPropertyMakesOnlyKeyNotNull() {
        CreateTableAsSelectStmt ctas = (CreateTableAsSelectStmt) SqlParser.parse(
                "create table paimon_ddl.db.t properties ('primary-key'='id', 'bucket'='2') " +
                        "as select cast(null as varchar(9)) as payload, cast(null as int) as id", 0L).get(0);
        CTASAnalyzer.analyze(ctas, context);
        CreateTableStmt stmt = ctas.getCreateTableStmt();
        Assertions.assertTrue(stmt.getColumns().get(0).isAllowNull());
        Assertions.assertFalse(stmt.getColumns().get(1).isAllowNull());
        Assertions.assertNull(stmt.getKeysDesc());
        Assertions.assertNull(stmt.getDistributionDesc());
        Assertions.assertFalse(stmt.getProperties().containsKey("replication_num"));
    }

    @Test
    public void testOlapCtasStillGetsReplicationAndDistributionDefaults() {
        CreateTableAsSelectStmt ctas = (CreateTableAsSelectStmt) SqlParser.parse(
                "create table test.paimon_slice_olap_ctas as select 1 as id", 0L).get(0);
        CTASAnalyzer.analyze(ctas, context);
        Assertions.assertTrue(ctas.getCreateTableStmt().getProperties().containsKey("replication_num"));
        Assertions.assertInstanceOf(RandomDistributionDesc.class, ctas.getCreateTableStmt().getDistributionDesc());
    }

    @Test
    public void testUnsupportedClausesAreRejectedBeforeRewrites() {
        assertInvalid("(id int, dt date) partition by days(dt)", "partition expressions");
        assertInvalid("(id int) distributed by hash(id) buckets 2", "DISTRIBUTED BY");
        assertInvalid("(id int default '1')", "DEFAULT NULL");
        assertInvalid("(id int, g int as (id + 1))", "generated");
        assertInvalid("(id bigint auto_increment)", "AUTO_INCREMENT");
        assertInvalid("(id int, v int sum)", "aggregate");
        assertInvalid("(id int null) primary key (id)", "nullable");
        assertInvalid("(id int) duplicate key (id)", "PRIMARY KEY");
        assertInvalid("(id int) properties ('path'='hidden')", "path");
    }

    private CreateTableStmt analyze(String sql) {
        CreateTableStmt stmt = (CreateTableStmt) SqlParser.parse(sql, 0L).get(0);
        CreateTableAnalyzer.analyze(stmt, context);
        return stmt;
    }

    private void assertInvalid(String definition, String expected) {
        SemanticException error = Assertions.assertThrows(SemanticException.class,
                () -> analyze("create table paimon_ddl.db.t " + definition));
        Assertions.assertTrue(error.getMessage().contains(expected), error.getMessage());
    }
}
