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

package com.starrocks.connector.iceberg;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.TableName;
import com.starrocks.common.DdlException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.procedure.RemoveOrphanFilesProcedure;
import com.starrocks.connector.iceberg.procedure.RollbackToSnapshotProcedure;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
<<<<<<< HEAD
=======
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnPosition;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
>>>>>>> 4de021e ([BugFix][RB-1622] Validate Iceberg write property values on CREATE and ALTER TABLE (#83542))
import com.starrocks.sql.ast.ProcedureArgument;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.Snapshot;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

<<<<<<< HEAD
=======
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
>>>>>>> 4de021e ([BugFix][RB-1622] Validate Iceberg write property values on CREATE and ALTER TABLE (#83542))
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.iceberg.IcebergTableOperation.REMOVE_ORPHAN_FILES;
import static com.starrocks.connector.iceberg.IcebergTableOperation.ROLLBACK_TO_SNAPSHOT;

public class IcebergTableOperationTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    private static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();
    public static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testRollbackToSnapshot(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                       @Mocked org.apache.iceberg.BaseTable table,
                                       @Mocked Snapshot snapshot) throws Exception {
        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }
        };

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return table;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName) {
                return true;
            }
        };

        new Expectations() {
            {
                snapshot.snapshotId();
                result = 1L;
                minTimes = 0;
                snapshot.parentId();
                result = null;
                minTimes = 0;
                snapshot.timestampMillis();
                result = System.currentTimeMillis();
                minTimes = 0;

                table.currentSnapshot();
                result = snapshot;
                minTimes = 0;
                table.snapshot(1L);
                result = snapshot;
                minTimes = 0;
                table.snapshots();
                result = List.of(snapshot);
                minTimes = 0;
            }
        };

        TableName tableName = new TableName(CATALOG_NAME, "db", "table");
        AlterTableOperationClause clause = new AlterTableOperationClause(
                NodePosition.ZERO, ROLLBACK_TO_SNAPSHOT.toString(),
                List.of(new ProcedureArgument(RollbackToSnapshotProcedure.SNAPSHOT_ID,
                        new IntLiteral(1, NodePosition.ZERO))), null);
        clause.setTableProcedure(RollbackToSnapshotProcedure.getInstance());
        clause.setAnalyzedArgs(Map.of(RollbackToSnapshotProcedure.SNAPSHOT_ID,
                ConstantOperator.createBigint(1)));
        IcebergAlterTableExecutor executor = new IcebergAlterTableExecutor(
                new AlterTableStmt(new TableRef(QualifiedName.of(List.of(
                        tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                        null, NodePosition.ZERO), List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()),
                icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();
    }

    @Test
    public void testAlterTableExecuteRemoveOrphanFiles(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                                       @Mocked Snapshot snapshot) throws Exception {
        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }
        };

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableA;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName) {
                return true;
            }
        };


        // Normalize Date
        TableName tableName = new TableName(CATALOG_NAME, "db", "table");
        AlterTableOperationClause clause = new AlterTableOperationClause(NodePosition.ZERO,
                REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN,
                ConstantOperator.createChar("2024-01-01 00:00:00")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        IcebergAlterTableExecutor executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                new TableRef(QualifiedName.of(List.of(
                        tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                        null, NodePosition.ZERO),
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();

        // Illegal date
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN,
                ConstantOperator.createChar("illegal date")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                new TableRef(QualifiedName.of(List.of(
                        tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                        null, NodePosition.ZERO),
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        IcebergAlterTableExecutor finalExecutor = executor;
        Assertions.assertThrows(DdlException.class, finalExecutor::execute);

        // Default retention interval
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN, ConstantOperator.createChar("")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                new TableRef(QualifiedName.of(List.of(
                        tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                        null, NodePosition.ZERO),
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        finalExecutor = executor;
        Assertions.assertThrows(DdlException.class, finalExecutor::execute);

        // Mock snapshot behavior
        new Expectations() {
            {
                snapshot.snapshotId();
                result = 1L;
                minTimes = 0;

                snapshot.timestampMillis();
                result = System.currentTimeMillis();
                minTimes = 0;

                snapshot.parentId();
                result = 0L;
                minTimes = 0;
            }
        };

        new MockUp<org.apache.iceberg.BaseTable>() {
            @Mock
            public org.apache.iceberg.Snapshot currentSnapshot() {
                return snapshot;
            }

            @Mock
            public Iterable<org.apache.iceberg.Snapshot> snapshots() {
                return List.of(snapshot, snapshot, snapshot);
            }
        };

        // inject snapshot
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN,
                ConstantOperator.createChar("2024-01-01 00:00:00")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                new TableRef(QualifiedName.of(List.of(
                        tableName.getCatalog(), tableName.getDb(), tableName.getTbl())),
                        null, NodePosition.ZERO),
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();
    }
<<<<<<< HEAD
=======

    @Test
    public void testAlterTableAddAndDropNestedField() throws Exception {
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "s", Types.StructType.of(
                        Types.NestedField.optional(3, "a", Types.IntegerType.get()),
                        Types.NestedField.optional(4, "arr", Types.ListType.ofOptional(5, Types.StructType.of(
                                Types.NestedField.optional(6, "x", Types.IntegerType.get())))))));
        org.apache.iceberg.Table table = create(schema, PartitionSpec.unpartitioned(), "nested", 2);

        List<AlterClause> clauses = List.of(
                new AddFieldClause("s", new StructFieldDesc("b", List.of(),
                        new TypeDef(IntegerType.BIGINT), ColumnPosition.FIRST), null),
                new AddFieldClause("S", new StructFieldDesc("y", List.of("ARR", "[*]"),
                        new TypeDef(VarcharType.VARCHAR), new ColumnPosition("X", NodePosition.ZERO)), null),
                new DropFieldClause("s", "A", List.of(), null));
        new IcebergAlterTableExecutor(new AlterTableStmt(
                new TableRef(QualifiedName.of(List.of(CATALOG_NAME, "db", "nested")), null, NodePosition.ZERO),
                clauses), table, null, connectContext, HDFS_ENVIRONMENT).execute();

        table.refresh();
        Types.StructType s = table.schema().findType("s").asStructType();
        Assertions.assertEquals(List.of("b", "arr"),
                s.fields().stream().map(Types.NestedField::name).collect(Collectors.toList()));
        Assertions.assertEquals(Types.LongType.get(), s.field("b").type());
        Types.StructType element = table.schema().findType("s.arr.element").asStructType();
        Assertions.assertEquals(List.of("x", "y"),
                element.fields().stream().map(Types.NestedField::name).collect(Collectors.toList()));
        Assertions.assertEquals(Types.StringType.get(), element.field("y").type());

        // the only field of a struct cannot be dropped
        AlterTableStmt dropOnly = new AlterTableStmt(
                new TableRef(QualifiedName.of(List.of(CATALOG_NAME, "db", "nested")), null, NodePosition.ZERO),
                List.of(new DropFieldClause("s", "x", List.of("arr", "[*]"), null),
                        new DropFieldClause("s", "y", List.of("arr", "[*]"), null)));
        Assertions.assertThrows(DdlException.class, () -> new IcebergAlterTableExecutor(dropOnly, table, null,
                connectContext, HDFS_ENVIRONMENT).execute());
        table.refresh();
        Assertions.assertEquals(2, table.schema().findType("s.arr.element").asStructType().fields().size());

        // the field to drop must exist
        AlterTableStmt dropMissing = new AlterTableStmt(
                new TableRef(QualifiedName.of(List.of(CATALOG_NAME, "db", "nested")), null, NodePosition.ZERO),
                List.of(new DropFieldClause("s", "a", List.of(), null)));
        Assertions.assertThrows(DdlException.class, () -> new IcebergAlterTableExecutor(dropMissing, table, null,
                connectContext, HDFS_ENVIRONMENT).execute());
    }

    private static ColumnDef intColumnDef(String name, boolean allowNull, String defaultValue) {
        ColumnDef.DefaultValueDef defaultValueDef = defaultValue == null ? ColumnDef.DefaultValueDef.NOT_SET
                : new ColumnDef.DefaultValueDef(true, new StringLiteral(defaultValue));
        return new ColumnDef(name, new TypeDef(IntegerType.INT), false, null, null, allowNull, defaultValueDef, null);
    }

    private void addColumns(org.apache.iceberg.Table table, ColumnDef... columnDefs) throws Exception {
        AlterClause clause = columnDefs.length == 1
                ? new AddColumnClause(columnDefs[0], null, null, Map.of())
                : new AddColumnsClause(List.of(columnDefs), null, Map.of());
        new IcebergAlterTableExecutor(new AlterTableStmt(
                new TableRef(QualifiedName.of(List.of(CATALOG_NAME, "db", table.name())), null, NodePosition.ZERO),
                List.of(clause)), table, null, connectContext, HDFS_ENVIRONMENT).execute();
        table.refresh();
    }

    @Test
    public void testAddRequiredColumnWithDefault() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        org.apache.iceberg.Table v3 = create(schema, PartitionSpec.unpartitioned(), "v3", 3);

        // v3: a NOT NULL column with a default is added as required, with the default as initial- and write-default
        addColumns(v3, intColumnDef("c1", false, "7"));
        addColumns(v3, intColumnDef("c2", false, "8"), intColumnDef("c3", true, null));
        Types.NestedField c1 = v3.schema().findField("c1");
        Assertions.assertTrue(c1.isRequired());
        Assertions.assertEquals(7, c1.initialDefault());
        Assertions.assertEquals(7, c1.writeDefault());
        Assertions.assertTrue(v3.schema().findField("c2").isRequired());
        Assertions.assertTrue(v3.schema().findField("c3").isOptional());

        // v3 without a default: rows in the existing data files would have no value
        Assertions.assertThrows(DdlException.class, () -> addColumns(v3, intColumnDef("c4", false, null)));

        // v2 has no column defaults
        org.apache.iceberg.Table v2 = create(schema, PartitionSpec.unpartitioned(), "v2", 2);
        Assertions.assertThrows(DdlException.class, () -> addColumns(v2, intColumnDef("c1", false, "7")));
        Assertions.assertThrows(DdlException.class,
                () -> addColumns(v2, intColumnDef("c1", true, null), intColumnDef("c2", false, "7")));
        v2.refresh();
        Assertions.assertNull(v2.schema().findField("c1"));
    }

    private void setProperties(org.apache.iceberg.Table table, Map<String, String> properties) throws Exception {
        new IcebergAlterTableExecutor(new AlterTableStmt(
                new TableRef(QualifiedName.of(List.of(CATALOG_NAME, "db", table.name())), null, NodePosition.ZERO),
                List.of(new ModifyTablePropertiesClause(new LinkedHashMap<>(properties)))),
                table, null, connectContext, HDFS_ENVIRONMENT).execute();
        table.refresh();
    }

    @Test
    public void testModifyTablePropertiesRejectsValuesIcebergCannotParse() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        org.apache.iceberg.Table table = create(schema, PartitionSpec.unpartitioned(), "props", 2);
        Map<String, String> before = Map.copyOf(table.properties());

        List<Map<String, String>> invalid = List.of(
                Map.of("write.parquet.compression-codec", "xxx"),
                Map.of("write.parquet.compression-codec", "zlib"),
                Map.of("write.parquet.compression-codec", "none"),
                Map.of("write.delete.parquet.compression-codec", "xxx"),
                Map.of("write.format.default", "xxx"),
                Map.of("write.format.default", "puffin"),
                Map.of("write.target-file-size-bytes", "-100"),
                Map.of("write.target-file-size-bytes", "0"),
                Map.of("write.target-file-size-bytes", "abc"),
                Map.of("write.target-file-size-bytes", " 134217728 "),
                // SR aliases resolve to the same native keys
                Map.of("file_format", "puffin"),
                Map.of("compression_codec", "zlib"),
                Map.of("compression_codec", "none"));
        for (Map<String, String> properties : invalid) {
            Map.Entry<String, String> entry = properties.entrySet().iterator().next();
            DdlException e = Assertions.assertThrows(DdlException.class, () -> setProperties(table, properties),
                    properties.toString());
            Assertions.assertTrue(e.getMessage().contains("'" + entry.getValue() + "'"), e.getMessage());
            Assertions.assertEquals(before, table.properties(), properties.toString());
        }

        // the alias codec follows the format set in the same statement, so an invalid one is reported first
        Map<String, String> codecFirst = new LinkedHashMap<>();
        codecFirst.put("compression_codec", "snappy");
        codecFirst.put("write.format.default", "xxx");
        DdlException e = Assertions.assertThrows(DdlException.class, () -> setProperties(table, codecFirst));
        Assertions.assertTrue(e.getMessage().contains("'xxx'"), e.getMessage());
        Assertions.assertEquals(before, table.properties());
    }

    @Test
    public void testModifyTablePropertiesKeepsValidValues() throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        org.apache.iceberg.Table table = create(schema, PartitionSpec.unpartitioned(), "props_ok", 2);

        // codecs are case-insensitive, and every codec Iceberg recognizes is allowed, even one SR cannot write
        setProperties(table, Map.of("write.parquet.compression-codec", "ZSTD",
                "write.delete.parquet.compression-codec", "lz4_raw",
                "write.target-file-size-bytes", "134217728"));
        Assertions.assertEquals("ZSTD", table.properties().get("write.parquet.compression-codec"));
        Assertions.assertEquals("lz4_raw", table.properties().get("write.delete.parquet.compression-codec"));
        Assertions.assertEquals("134217728", table.properties().get("write.target-file-size-bytes"));

        // an alias codec on a parquet table lands on the parquet codec key
        setProperties(table, Map.of("compression_codec", "snappy"));
        Assertions.assertEquals("snappy", table.properties().get("write.parquet.compression-codec"));

        // native keys are case-sensitive: an upper-case key is just a custom property
        setProperties(table, Map.of("WRITE.PARQUET.COMPRESSION-CODEC", "xxx", "foo.bar", "baz"));
        Assertions.assertEquals("xxx", table.properties().get("WRITE.PARQUET.COMPRESSION-CODEC"));
        Assertions.assertEquals("baz", table.properties().get("foo.bar"));

        // formats SR cannot write stay settable for other engines
        setProperties(table, Map.of("write.format.default", "avro"));
        Assertions.assertEquals("avro", table.properties().get("write.format.default"));

        // an alias codec that lands on the ORC key is not checked against the parquet codecs
        setProperties(table, Map.of("file_format", "orc", "compression_codec", "zlib"));
        Assertions.assertEquals("ORC", table.properties().get("write.format.default"));
        Assertions.assertEquals("zlib", table.properties().get("write.orc.compression-codec"));

        // the alias codec follows a native default format set in the same statement, not the table's
        org.apache.iceberg.Table parquet = create(schema, PartitionSpec.unpartitioned(), "props_native_orc", 2);
        String parquetCodec = parquet.properties().get("write.parquet.compression-codec");
        setProperties(parquet, Map.of("write.format.default", "orc", "compression_codec", "zlib"));
        Assertions.assertEquals("orc", parquet.properties().get("write.format.default"));
        Assertions.assertEquals("zlib", parquet.properties().get("write.orc.compression-codec"));
        Assertions.assertEquals(parquetCodec, parquet.properties().get("write.parquet.compression-codec"));
    }
>>>>>>> 4de021e ([BugFix][RB-1622] Validate Iceberg write property values on CREATE and ALTER TABLE (#83542))
}
