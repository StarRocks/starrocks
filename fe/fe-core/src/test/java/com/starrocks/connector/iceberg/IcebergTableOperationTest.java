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
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddFieldClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnPosition;
import com.starrocks.sql.ast.DropFieldClause;
import com.starrocks.sql.ast.ProcedureArgument;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.StructFieldDesc;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
}
