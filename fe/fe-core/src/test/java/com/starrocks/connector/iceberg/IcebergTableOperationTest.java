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
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
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
}
