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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.connector.hive.HiveStorageFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubmitTaskStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.IntegerType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The guard decides on the statement's mutation target, so every case here is about which catalog the
 * target lives in - never about the statement kind on its own. Statements are parsed rather than built by
 * hand so that the visitor dispatch being exercised is the real one: a default method forwarding somewhere
 * unexpected is exactly the failure this is meant to catch.
 */
public class LakeFormationWriteGuardTest {

    private static final String LF = "lf_catalog";
    private static final String OTHER = "hive_catalog";

    private ConnectContext context;

    @BeforeEach
    public void setUp() {
        // Only lf_catalog answers yes; everything else must pass straight through.
        new MockUp<LakeFormationCatalogs>() {
            @Mock
            public boolean isLakeFormationCatalog(String catalogName) {
                return LF.equals(catalogName);
            }
        };
        context = new ConnectContext();
        context.setCurrentCatalog(OTHER);
    }

    private static StatementBase parse(String sql) {
        List<StatementBase> statements = SqlParser.parse(sql, new SessionVariable());
        return statements.get(0);
    }

    private void assertRefused(String sql) {
        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationWriteGuard.check(parse(sql), context), sql);
        assertTrue(e.getMessage().contains("not supported") || e.getMessage().contains("is not supported"),
                e.getMessage());
    }

    private void assertAllowed(String sql) {
        assertDoesNotThrow(() -> LakeFormationWriteGuard.check(parse(sql), context), sql);
    }

    @Test
    public void testRefusesDdlWhoseTargetIsALakeFormationCatalog() {
        assertRefused("CREATE TABLE " + LF + ".db.t (id INT)");
        assertRefused("DROP TABLE " + LF + ".db.t");
        assertRefused("ALTER TABLE " + LF + ".db.t ADD COLUMN c INT");
        assertRefused("TRUNCATE TABLE " + LF + ".db.t");
        assertRefused("CREATE DATABASE " + LF + ".db");
        assertRefused("DROP DATABASE " + LF + ".db");
    }

    /**
     * Not just a cache drop: the chain ends in a bare Glue load whose result lands in the cross-query table
     * cache, where the next principal would find an untrimmed schema.
     */
    @Test
    public void testRefusesRefreshExternalTable() {
        assertRefused("REFRESH EXTERNAL TABLE " + LF + ".db.t");
    }

    /** Statistics are data, and this version reads none of a registered table's data. */
    @Test
    public void testRefusesAnalyze() {
        assertRefused("ANALYZE TABLE " + LF + ".db.t");
    }

    /**
     * The metadata level guard cannot cover this one: MetadataMgr returns early for IF NOT EXISTS when the
     * table already exists, so the connector's createTableLike is never reached and the statement would
     * report success.
     */
    @Test
    public void testRefusesCreateTableLike() {
        assertRefused("CREATE TABLE " + LF + ".db.copy LIKE " + OTHER + ".db.t");
        assertRefused("CREATE TABLE IF NOT EXISTS " + LF + ".db.copy LIKE " + OTHER + ".db.t");
    }

    /** Refused at submit time, or the user gets a successful SUBMIT and a task that fails forever after. */
    @Test
    public void testRefusesSubmitTaskWhoseTargetIsALakeFormationCatalog() {
        assertRefused("SUBMIT TASK AS CREATE TABLE " + LF + ".db.t AS SELECT 1 AS id");
    }

    /**
     * A submitted task is stored under a catalog, and the analyzer fills that in from the session when the
     * task name is unqualified - so the task's own catalog says nothing about what the task writes to.
     * Judging by it would refuse every task submitted while a Lake Formation catalog happens to be current,
     * including one that only reads from it.
     */
    @Test
    public void testSubmitTaskIsJudgedByItsTargetNotByTheSessionsCatalog() {
        context.setCurrentCatalog(LF);
        assertAllowed("SUBMIT TASK AS CREATE TABLE " + OTHER + ".db.t AS SELECT * FROM " + LF + ".db.src");
        assertAllowed("SUBMIT TASK AS INSERT INTO " + OTHER + ".db.t SELECT * FROM " + LF + ".db.src");

        // The target still decides: an unqualified CTAS target does resolve against the session's catalog.
        assertRefused("SUBMIT TASK AS CREATE TABLE db.t AS SELECT 1 AS id");
    }

    @Test
    public void testRefusesCreateAnalyzeJob() {
        assertRefused("CREATE ANALYZE TABLE " + LF + ".db.t");
    }

    /** The rule is about the target. Reading a Lake Formation table is exactly what this version supports. */
    @Test
    public void testAllowsLakeFormationTableAsASource() {
        assertAllowed("SELECT * FROM " + LF + ".db.t");
        assertAllowed("CREATE TABLE " + OTHER + ".db.copy AS SELECT * FROM " + LF + ".db.t");
        assertAllowed("INSERT INTO " + OTHER + ".db.t SELECT * FROM " + LF + ".db.t");
        assertAllowed("SUBMIT TASK AS CREATE TABLE " + OTHER + ".db.t AS SELECT * FROM " + LF + ".db.t");
    }

    @Test
    public void testLeavesOtherCatalogsAlone() {
        assertAllowed("CREATE TABLE " + OTHER + ".db.t (id INT)");
        assertAllowed("DROP TABLE " + OTHER + ".db.t");
        assertAllowed("ANALYZE TABLE " + OTHER + ".db.t");
        assertAllowed("REFRESH EXTERNAL TABLE " + OTHER + ".db.t");
    }

    /**
     * The guard runs on every statement for every user, so the thing most likely to break by accident is an
     * ordinary internal-catalog statement. None of these name a catalog at all.
     */
    @Test
    public void testInternalCatalogStatementsAreUntouched() {
        assertAllowed("SELECT 1");
        assertAllowed("CREATE TABLE db.t (id INT)");
        assertAllowed("DROP TABLE db.t");
        assertAllowed("SHOW DATABASES");
        assertAllowed("SET query_timeout = 10");
    }

    @Test
    public void testNullStatementIsNotAnError() {
        assertDoesNotThrow(() -> LakeFormationWriteGuard.check(null, context));
    }

    /**
     * The catalog name check cannot cover this one: an INSERT names its target through a resolved table,
     * so the guard reads the table instance rather than a name. The message has to name the table, or the
     * user cannot tell which of several targets was refused.
     */
    @Test
    public void testRefusesAnInsertWhoseResolvedTargetIsGoverned() {
        InsertStmt statement = (InsertStmt) parse("INSERT INTO " + OTHER + ".db.t SELECT 1");
        statement.setTargetTable(governedTable());

        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationWriteGuard.check(statement, context));
        assertTrue(e.getMessage().contains("Writing to"), e.getMessage());
        assertTrue(e.getMessage().contains("db.t"), e.getMessage());
    }

    /** Same table level check inside a submitted task, which is a second place the target is resolved. */
    @Test
    public void testRefusesASubmitTaskWhoseResolvedInsertTargetIsGoverned() {
        SubmitTaskStmt statement =
                (SubmitTaskStmt) parse("SUBMIT TASK AS INSERT INTO " + OTHER + ".db.t SELECT 1");
        statement.getInsertStmt().setTargetTable(governedTable());

        LakeFormationTableAccessException e = assertThrows(LakeFormationTableAccessException.class,
                () -> LakeFormationWriteGuard.check(statement, context));
        assertTrue(e.getMessage().contains("SUBMIT TASK writing to"), e.getMessage());
    }

    private static LakeFormationHiveTable governedTable() {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("id", IntegerType.INT));
        HiveTable physical = HiveTable.builder()
                .setId(1L)
                .setTableName("t")
                .setCatalogName(LF)
                .setHiveDbName("db")
                .setHiveTableName("t")
                .setTableLocation("s3://bucket/db/t")
                .setCreateTime(1600000000L)
                .setFullSchema(schema)
                .setPartitionColumnNames(new ArrayList<>())
                .setDataColumnNames(new ArrayList<>(List.of("id")))
                .setProperties(new HashMap<>())
                .setSerdeProperties(new HashMap<>())
                .setStorageFormat(HiveStorageFormat.PARQUET)
                .build();
        return LakeFormationHiveTable.of(physical, List.of(new Column("id", IntegerType.INT)),
                new LakeFormationTableIdentity(LF, null, "us-west-2", "db", "t"));
    }
}
