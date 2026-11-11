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
import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.connector.paimon.PaimonTableSchemaConverter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.parser.SqlParser;
import mockit.Expectations;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        Assertions.assertFalse(stmt.getColumnDefs().get(1).isKey());
        Assertions.assertTrue(stmt.getColumnDefs().get(1).isAllowNull());
        Assertions.assertFalse(stmt.getColumnDefs().get(2).isKey());
        Assertions.assertTrue(stmt.getColumnDefs().get(2).isAllowNull());
        Assertions.assertNull(stmt.getColumns().get(0).getAggregationType());
        Assertions.assertEquals(3, stmt.getColumnDefs().size());
        Assertions.assertNull(stmt.getDistributionDesc());
        Assertions.assertFalse(stmt.getProperties().containsKey("replication_num"));
        Schema schema = PaimonTableSchemaConverter.toSchema(stmt);
        Assertions.assertEquals(List.of("Id", "dt"), schema.primaryKeys());
        Assertions.assertEquals(List.of("dt"), schema.partitionKeys());
        Assertions.assertTrue(schema.fields().get(0).type().isNullable());
        Assertions.assertFalse(schema.fields().get(1).type().isNullable());
        Assertions.assertFalse(schema.fields().get(2).type().isNullable());
    }

    @Test
    public void testPropertyPrimaryKeyMatchesNativePrimaryKey() {
        CreateTableStmt stmt = analyze("create table paimon_ddl.db.t (v varchar(17), Id int) " +
                "properties ('primary-key'='ID', 'bucket'='2')");
        Assertions.assertNull(stmt.getKeysDesc());
        Assertions.assertFalse(stmt.getColumns().get(0).isKey());
        Assertions.assertFalse(stmt.getColumnDefs().get(1).isKey());
        Assertions.assertTrue(stmt.getColumnDefs().get(1).isAllowNull());
        Schema schema = PaimonTableSchemaConverter.toSchema(stmt);
        Assertions.assertEquals(List.of("Id"), schema.primaryKeys());
        Assertions.assertTrue(schema.fields().get(0).type().isNullable());
        Assertions.assertFalse(schema.fields().get(1).type().isNullable());
    }

    @Test
    public void testQuotedKeyAndPartitionNamesPreserveWhitespace(@TempDir Path warehouse) throws Exception {
        Options options = Options.fromMap(Map.of("warehouse", warehouse.toUri().toString()));
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", false);
            for (String columns : List.of("id int, ` id ` int, seq int", "` id ` int, seq int")) {
                CreateTableStmt stmt = analyze("create table paimon_ddl.db.t (" + columns + ") " +
                        "primary key (` id `, seq) partition by (` id `)");
                Schema schema = PaimonTableSchemaConverter.toSchema(stmt);
                Assertions.assertEquals(List.of(" id ", "seq"), schema.primaryKeys());
                Assertions.assertEquals(List.of(" id "), schema.partitionKeys());
                catalog.createTable(Identifier.create("db", "t"), schema, false);
                try (Catalog fresh = CatalogFactory.createCatalog(CatalogContext.create(options))) {
                    Table table = fresh.getTable(Identifier.create("db", "t"));
                    Assertions.assertEquals(List.of(" id ", "seq"), table.primaryKeys());
                    Assertions.assertEquals(List.of(" id "), table.partitionKeys());
                    for (org.apache.paimon.types.DataField field : table.rowType().getFields()) {
                        Assertions.assertEquals(field.name().equals("id"), field.type().isNullable());
                    }
                }
                catalog.dropTable(Identifier.create("db", "t"), false);
            }
        }
        assertInvalid("(id int) primary key (` id `)", "does not exist");
        assertInvalid("(id int) partition by (` id `)", "does not exist");
        assertInvalid("(id int, ` id ` int) primary key (` id `) properties ('primary-key'=' id ')",
                "same columns");
    }

    @Test
    public void testPrimaryKeyDefaultNullRemainsRejected() {
        for (String keys : List.of("primary key(id)", "properties ('primary-key'='id')")) {
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> analyze("create table paimon_ddl.db.t (id bigint default null) " + keys));
            Assertions.assertTrue(error.getMessage().contains("Invalid default value"), error.getMessage());
        }
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
    public void testNativeSchemaValidationIsDeferredUntilExecution(@TempDir Path warehouse) throws Exception {
        Options options = Options.fromMap(Map.of("warehouse", warehouse.toUri().toString()));
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            catalog.createDatabase("db", false);
            PaimonMetadata metadata = new PaimonMetadata("paimon_ddl", new HdfsEnvironment(), catalog,
                    new ConnectorProperties(ConnectorType.PAIMON));
            for (Map.Entry<String, String> invalid : Map.of(
                    "(id largeint)", "Unsupported Paimon column type",
                    "(id int) properties ('bucket'='0')", "bucket",
                    "(id int) properties ('path'='hidden')", "path").entrySet()) {
                CreateTableStmt stmt = analyze("create table paimon_ddl.db.t " + invalid.getKey());
                SemanticException error = Assertions.assertThrows(SemanticException.class,
                        () -> metadata.createTable(context, stmt));
                Assertions.assertTrue(error.getMessage().contains(invalid.getValue()), error.getMessage());
                Assertions.assertThrows(Catalog.TableNotExistException.class,
                        () -> catalog.getTable(Identifier.create("db", "t")));
            }
        }
    }

    @Test
    public void testDatabaseIfExistsAndTableDdlDispatch(@TempDir Path warehouse) throws Exception {
        Options options = Options.fromMap(Map.of("warehouse", warehouse.toUri().toString(), "cache-enabled", "true"));
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options))) {
            PaimonMetadata ddl = new PaimonMetadata("paimon_ddl", new HdfsEnvironment(), catalog,
                    new ConnectorProperties(ConnectorType.PAIMON));
            MetadataMgr metadata = GlobalStateMgr.getCurrentState().getMetadataMgr();
            new Expectations(metadata) {
                {
                    metadata.getOptionalMetadata("paimon_ddl");
                    result = Optional.of(ddl);
                    minTimes = 0;
                }
            };
            executeDdl("create database if not exists paimon_ddl.ddl_sql");
            executeDdl("create database if not exists paimon_ddl.ddl_sql");
            Assertions.assertThrows(DdlException.class, () -> executeDdl("create database paimon_ddl.ddl_sql"));
            for (String keys : List.of("primary key(id)", "properties ('primary-key'='id')")) {
                executeDdl("create table paimon_ddl.ddl_sql.pk_demo (payload varchar(100), id bigint) " + keys);
                try (Catalog fresh = CatalogFactory.createCatalog(CatalogContext.create(options))) {
                    Table table = fresh.getTable(Identifier.create("ddl_sql", "pk_demo"));
                    Assertions.assertEquals(List.of("id"), table.primaryKeys());
                    Assertions.assertTrue(table.rowType().getTypeAt(0).isNullable());
                    Assertions.assertFalse(table.rowType().getTypeAt(1).isNullable());
                }
                executeDdl("drop table paimon_ddl.ddl_sql.pk_demo");
            }
            executeDdl("create table if not exists paimon_ddl.ddl_sql.append_demo " +
                    "(id bigint not null, payload varchar(100), dt date) partition by(dt) properties('bucket'='-1')");
            executeDdl("create table if not exists paimon_ddl.ddl_sql.append_demo (different int)");
            Assertions.assertEquals(List.of("id", "payload", "dt"),
                    catalog.getTable(Identifier.create("ddl_sql", "append_demo")).rowType().getFieldNames());
            Assertions.assertThrows(DdlException.class, () -> executeDdl("drop database paimon_ddl.ddl_sql force"));
            InsertStmt insert = (InsertStmt) SqlParser.parse(
                    "insert into paimon_ddl.ddl_sql.append_demo values (1, 'blocked', '2026-11-02')", 0L).get(0);
            UnsupportedException error = Assertions.assertThrows(UnsupportedException.class,
                    () -> StatementPlanner.plan(insert, context));
            Assertions.assertTrue(error.getMessage().contains("Only support insert"), error.getMessage());
            Assertions.assertEquals(DmlStmt.INVALID_TXN_ID, insert.getTxnId());
            executeDdl("drop table if exists paimon_ddl.ddl_sql.append_demo");
            executeDdl("drop table if exists paimon_ddl.ddl_sql.append_demo");
            executeDdl("drop database if exists paimon_ddl.ddl_sql");
            executeDdl("drop database if exists paimon_ddl.ddl_sql");
            Assertions.assertFalse(catalog.listDatabases().contains("ddl_sql"));
        }
    }

    @Test
    public void testCreateAfterExternalDropBypassesStaleCache(@TempDir Path warehouse) throws Exception {
        Options options = Options.fromMap(Map.of("warehouse", warehouse.toUri().toString(), "cache-enabled", "true"));
        Options externalOptions = Options.fromMap(Map.of("warehouse", warehouse.toUri().toString(),
                "cache-enabled", "false"));
        try (Catalog catalog = CatalogFactory.createCatalog(CatalogContext.create(options));
                Catalog external = CatalogFactory.createCatalog(CatalogContext.create(externalOptions))) {
            PaimonMetadata ddl = new PaimonMetadata("paimon_ddl", new HdfsEnvironment(), catalog,
                    new ConnectorProperties(ConnectorType.PAIMON));
            MetadataMgr metadata = GlobalStateMgr.getCurrentState().getMetadataMgr();
            new Expectations(metadata) {
                {
                    metadata.getOptionalMetadata("paimon_ddl");
                    result = Optional.of(ddl);
                    minTimes = 0;
                }
            };
            executeDdl("create database paimon_ddl.ddl_cache");
            Identifier id = Identifier.create("ddl_cache", "t");
            for (String clause : List.of("", "if not exists ")) {
                executeDdl("create table paimon_ddl.ddl_cache.t (old_column int)");
                Assertions.assertTrue(ddl.tableExists(context, "ddl_cache", "t"));
                Table cached = catalog.getTable(id);
                external.dropTable(id, false);
                Assertions.assertTrue(ddl.tableExists(context, "ddl_cache", "t"));
                Assertions.assertSame(cached, catalog.getTable(id));
                Assertions.assertThrows(Catalog.TableNotExistException.class, () -> external.getTable(id));

                executeDdl("create table " + clause + "paimon_ddl.ddl_cache.t (new_column bigint)");
                Assertions.assertEquals(List.of("new_column"), external.getTable(id).rowType().getFieldNames());
                Assertions.assertEquals(List.of("new_column"), catalog.getTable(id).rowType().getFieldNames());
                executeDdl("create table if not exists paimon_ddl.ddl_cache.t (ignored_column int)");
                Assertions.assertEquals(List.of("new_column"), external.getTable(id).rowType().getFieldNames());
                Assertions.assertThrows(DdlException.class,
                        () -> executeDdl("create table paimon_ddl.ddl_cache.t (id int)"));
                executeDdl("drop table paimon_ddl.ddl_cache.t");
            }
            executeDdl("drop database paimon_ddl.ddl_cache");
        }
    }

    @Test
    public void testPaimonCtasIsRejectedBeforeCreatingATable() {
        for (String engine : List.of("", "engine=paimon")) {
            CreateTableAsSelectStmt ctas = (CreateTableAsSelectStmt) SqlParser.parse(
                    "create table paimon_ddl.db.t " + engine + " as select 1 as id", 0L).get(0);
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> CTASAnalyzer.analyze(ctas, context));
            Assertions.assertTrue(error.getMessage().contains("Paimon CREATE TABLE AS SELECT is not supported"));
            Assertions.assertTrue(ctas.getCreateTableStmt().getColumnDefs().isEmpty());
        }
    }

    @Test
    public void testLikeAndInsertRemainUnsupported() {
        MetadataMgr metadata = GlobalStateMgr.getCurrentState().getMetadataMgr();
        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, "paimon_ddl", "db", "source");
                result = new PaimonTable();
                minTimes = 0;
            }
        };
        for (String target : List.of("paimon_ddl.db.t", "test.t")) {
            CreateTableLikeStmt like = (CreateTableLikeStmt) SqlParser.parse(
                    "create table " + target + " like paimon_ddl.db.source", 0L).get(0);
            SemanticException error = Assertions.assertThrows(SemanticException.class,
                    () -> CreateTableLikeAnalyzer.analyze(like, context));
            Assertions.assertTrue(error.getMessage().contains("CREATE_TABLE_LIKE"), error.getMessage());
        }
        for (String mode : List.of("into", "overwrite")) {
            InsertStmt insert = (InsertStmt) SqlParser.parse(
                    "insert " + mode + " paimon_ddl.db.source values (1)", 0L).get(0);
            UnsupportedException error = Assertions.assertThrows(UnsupportedException.class,
                    () -> InsertAnalyzer.analyze(insert, context));
            Assertions.assertTrue(error.getMessage().contains("Only support insert"), error.getMessage());
        }
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
    public void testUnsupportedClausesAreRejectedDuringAnalysis() {
        assertInvalid("(id int, dt date) partition by days(dt)", "partition expressions");
        assertInvalid("(id int) distributed by hash(id) buckets 2", "distribution desc");
        assertInvalid("(id int) order by(id)", "ORDER BY");
        assertInvalid("(id int) rollup (r(id))", "ROLLUP");
        assertInvalid("(id int) broker properties ('broker.name'='unused')", "PROPERTIES");
        assertInvalid("(id int default '1')", "DEFAULT NULL");
        assertInvalid("(id int, g int as (id + 1))", "generated");
        assertInvalid("(id bigint auto_increment)", "AUTO_INCREMENT");
        assertInvalid("(id int, v int sum)", "aggregate");
        assertInvalid("(id int null) primary key (id)", "nullable");
        assertInvalid("(id int) duplicate key (id)", "PRIMARY KEY");
        assertInvalid("(id int) unique key (id)", "PRIMARY KEY");
        assertInvalid("(id int) primary key (missing)", "does not exist");
        assertInvalid("(id int) primary key (id,id)", "Duplicate");
        assertInvalid("(id int, other int) primary key (id) properties ('primary-key'='other')", "same columns");
        assertInvalid("(id int, dt date) partition by (dt, DT)", "Duplicate");
        assertInvalid("(id int) partition by (missing)", "does not exist");
        assertInvalid("(id int) partition by range(id) (partition p1 values less than ('10'))", "PARTITION BY");
    }

    @Test
    public void testRejectedPrimaryKeyPreservesParsedColumns() {
        CreateTableStmt stmt = (CreateTableStmt) SqlParser.parse(
                "create table paimon_ddl.db.t (id bigint, dt date null) primary key(id, dt)", 0L).get(0);
        List<Boolean> nullable = stmt.getColumnDefs().stream().map(ColumnDef::isAllowNull).toList();
        List<Boolean> keyFlags = stmt.getColumnDefs().stream().map(ColumnDef::isKey).toList();

        Assertions.assertThrows(SemanticException.class, () -> CreateTableAnalyzer.analyze(stmt, context));

        Assertions.assertEquals(nullable, stmt.getColumnDefs().stream().map(ColumnDef::isAllowNull).toList());
        Assertions.assertEquals(keyFlags, stmt.getColumnDefs().stream().map(ColumnDef::isKey).toList());
    }

    private void executeDdl(String sql) throws Exception {
        context.getState().reset();
        StatementBase stmt = SqlParser.parse(sql, 0L).get(0);
        Analyzer.analyze(stmt, context);
        DDLStmtExecutor.execute(stmt, context);
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
