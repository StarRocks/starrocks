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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VariantType;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PaimonTableSchemaConverterTest {
    @Test
    public void testExactScalarTypesNullabilityAndComments() {
        List<Type> sourceTypes = List.of(BooleanType.BOOLEAN, IntegerType.TINYINT, IntegerType.SMALLINT,
                IntegerType.INT, IntegerType.BIGINT, FloatType.FLOAT, FloatType.DOUBLE, DateType.DATE,
                DateType.DATETIME, TypeFactory.createCharType(13), TypeFactory.createVarcharType(79),
                TypeFactory.createVarbinary(23), TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 31, 7),
                VariantType.VARIANT);
        List<DataType> expected = List.of(DataTypes.BOOLEAN(), DataTypes.TINYINT(), DataTypes.SMALLINT(),
                DataTypes.INT(), DataTypes.BIGINT(), DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.DATE(),
                DataTypes.TIMESTAMP(6), DataTypes.CHAR(13), DataTypes.VARCHAR(79), DataTypes.VARBINARY(23),
                DataTypes.DECIMAL(31, 7), DataTypes.VARIANT());
        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < sourceTypes.size(); i++) {
            columns.add(new Column("c" + i, sourceTypes.get(i), i % 2 == 0, "comment " + i));
        }
        RowType row = schemaFromColumns(columns);
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i).copy(i % 2 == 0), row.getFields().get(i).type());
            Assertions.assertEquals("comment " + i, row.getFields().get(i).description());
        }
    }

    @Test
    public void testNestedTypesReceiveGloballyUniqueIds() {
        StructType nested = new StructType(new ArrayList<>(List.of(
                new StructField("leaf", IntegerType.INT, "nested comment"),
                new StructField("payload", VariantType.VARIANT))));
        StructType outer = new StructType(new ArrayList<>(List.of(new StructField("a", nested), new StructField("b", nested))));
        RowType row = schemaFromColumns(List.of(
                new Column("arr", new ArrayType(outer), false, "array comment"),
                new Column("map", new MapType(TypeFactory.createVarcharType(17), nested), true),
                new Column("last", nested, false)));
        Set<Integer> ids = new HashSet<>();
        assertUniqueIds(row, ids);
        Assertions.assertEquals(13, ids.size());
        Assertions.assertFalse(row.getFields().get(0).type().isNullable());
        org.apache.paimon.types.ArrayType array = (org.apache.paimon.types.ArrayType) row.getFields().get(0).type();
        Assertions.assertTrue(array.getElementType().isNullable());
        RowType child = (RowType) ((RowType) array.getElementType()).getFields().get(0).type();
        Assertions.assertEquals("nested comment", child.getFields().get(0).description());
        org.apache.paimon.types.MapType map = (org.apache.paimon.types.MapType) row.getFields().get(1).type();
        Assertions.assertEquals(DataTypes.VARCHAR(17), map.getKeyType());
        Assertions.assertTrue(map.getValueType().isNullable());
    }

    @Test
    public void testSchemaOptionsKeysAndDefaultBucketArePreserved() {
        CreateTableStmt stmt = parse("create table p.db.t (payload varchar(27) default null comment 'payload', " +
                "Id bigint, dt date) engine=paimon primary key (Id, dt) comment 'table comment' partition by (dt) " +
                "properties ('primary-key'=' id, DT ', 'bucket'='4', 'bucket-key'='Id', 'file.format'='parquet', " +
                "'file.compression'='zstd', 'snapshot.num-retained.min'='2', 'snapshot.num-retained.max'='9')");
        Schema schema = PaimonTableSchemaConverter.toSchema(stmt);
        Assertions.assertEquals(List.of("Id", "dt"), schema.primaryKeys());
        Assertions.assertEquals(List.of("dt"), schema.partitionKeys());
        Assertions.assertEquals("payload", schema.fields().get(0).description());
        Assertions.assertEquals("table comment", schema.comment());
        Assertions.assertEquals("zstd", schema.options().get("file.compression"));
        Assertions.assertFalse(schema.options().containsKey("primary-key"));
        Assertions.assertFalse(schema.fields().get(1).type().isNullable());
        Assertions.assertFalse(schema.fields().get(2).type().isNullable());
        Assertions.assertTrue(schema.fields().get(0).type().isNullable());

        Schema defaults = PaimonTableSchemaConverter.toSchema(parse("create table p.db.t (id int) engine=paimon"));
        Assertions.assertFalse(defaults.options().containsKey("bucket"));
        Schema pkDefaults = PaimonTableSchemaConverter.toSchema(parse(
                "create table p.db.t (id int) engine=paimon properties ('primary-key'='id')"));
        Assertions.assertFalse(pkDefaults.options().containsKey("bucket"));
        Assertions.assertEquals(List.of("id"), pkDefaults.primaryKeys());
    }

    @Test
    public void testQuotedKeyAndPartitionNamesPreserveWhitespace() {
        Schema schema = PaimonTableSchemaConverter.toSchema(parse(
                "create table p.db.t (id int, ` id ` int, seq int) engine=paimon " +
                        "primary key (` id `, seq) partition by (` id `)"));
        Assertions.assertEquals(List.of(" id ", "seq"), schema.primaryKeys());
        Assertions.assertEquals(List.of(" id "), schema.partitionKeys());
        Assertions.assertTrue(schema.fields().get(0).type().isNullable());
        Assertions.assertFalse(schema.fields().get(1).type().isNullable());

        Schema propertySchema = PaimonTableSchemaConverter.toSchema(parse(
                "create table p.db.t (id int, ` id ` int) engine=paimon properties ('primary-key'=' id ')"));
        Assertions.assertEquals(List.of("id"), propertySchema.primaryKeys());
        Assertions.assertFalse(propertySchema.fields().get(0).type().isNullable());
        Assertions.assertTrue(propertySchema.fields().get(1).type().isNullable());
    }

    @Test
    public void testPrimaryKeyDdlAcceptsNativeDynamicBucketModes() {
        for (String bucket : List.of("-1", "-2", "2")) {
            Schema schema = PaimonTableSchemaConverter.toSchema(parse(
                    "create table p.db.t (id int) engine=paimon primary key(id) properties ('bucket'='" + bucket + "')"));
            Assertions.assertEquals(bucket, schema.options().get("bucket"));
            Assertions.assertEquals(List.of("id"), schema.primaryKeys());
        }
    }

    @Test
    public void testNativeCrossPartitionPrimaryKeySchemaIsPreserved() {
        Schema schema = PaimonTableSchemaConverter.toSchema(parse(
                "create table p.db.t (id int, dt date) engine=paimon primary key(id) partition by(dt)"));
        Assertions.assertEquals(List.of("id"), schema.primaryKeys());
        Assertions.assertEquals(List.of("dt"), schema.partitionKeys());
        Assertions.assertFalse(schema.fields().get(0).type().isNullable());
        Assertions.assertTrue(schema.fields().get(1).type().isNullable());
    }

    @Test
    public void testPrimaryKeyConversionPreservesParsedColumns() {
        for (String keys : List.of("primary key(Id, dt)", "properties ('primary-key'=' ID, dt ')")) {
            CreateTableStmt stmt = parse("create table p.db.t (payload varchar(20), Id bigint, " +
                    "dt date not null, required int not null) engine=paimon " + keys);
            List<ColumnDef> definitions = List.copyOf(stmt.getColumnDefs());
            List<Boolean> nullable = definitions.stream().map(ColumnDef::isAllowNull).toList();
            List<Boolean> keyFlags = definitions.stream().map(ColumnDef::isKey).toList();

            Schema first = PaimonTableSchemaConverter.toSchema(stmt);
            Schema second = PaimonTableSchemaConverter.toSchema(stmt);

            Assertions.assertEquals(List.of("Id", "dt"), first.primaryKeys());
            Assertions.assertTrue(first.fields().get(0).type().isNullable());
            Assertions.assertFalse(first.fields().get(1).type().isNullable());
            Assertions.assertFalse(first.fields().get(2).type().isNullable());
            Assertions.assertFalse(first.fields().get(3).type().isNullable());
            Assertions.assertEquals(first.rowType(), second.rowType());
            Assertions.assertEquals(nullable, definitions.stream().map(ColumnDef::isAllowNull).toList());
            Assertions.assertEquals(keyFlags, definitions.stream().map(ColumnDef::isKey).toList());
            for (int i = 0; i < definitions.size(); i++) {
                Assertions.assertSame(definitions.get(i), stmt.getColumnDefs().get(i));
            }
        }
    }

    @Test
    public void testConversionPreservesSuppliedColumns() {
        CreateTableStmt stmt = parse("create table p.db.t (id bigint, payload int) engine=paimon primary key(id)");
        List<Column> columns = stmt.getColumnDefs().stream().map(ColumnBuilder::buildColumn).toList();
        stmt.setColumns(columns);

        Schema schema = PaimonTableSchemaConverter.toSchema(stmt);

        Assertions.assertFalse(schema.fields().get(0).type().isNullable());
        Assertions.assertTrue(schema.fields().get(1).type().isNullable());
        Assertions.assertSame(columns, stmt.getColumns());
        Assertions.assertTrue(columns.get(0).isAllowNull());
        Assertions.assertFalse(columns.get(0).isKey());
    }

    @Test
    public void testUnsafeUnknownAndIllTypedOptionsAreRejected() {
        for (String key : List.of("path", "warehouse", "type", "partition", "fields.id.type", "hive.metastore.uris",
                "s3.access_key", "schema", "global-index.external-path", "unknown-option", "branch", "upsert-key",
                "data-file.external-paths", "data-file.path-directory", "format-table.implementation")) {
            assertInvalid("(id int)", "properties ('" + key + "'='redacted-value')", key);
        }
        assertInvalid("(id int)", "properties ('bucket'='not-an-integer')", "Invalid value");
        assertInvalid("(id int)", "properties ('write-only'='not-a-boolean')", "Invalid value");
        assertInvalid("(id int)", "properties ('snapshot.time-retained'='not-a-duration')", "Invalid value");
        assertInvalid("(id int)", "properties ('bucket'='0')", "bucket");
        assertInvalid("(id int)", "properties ('primary-key'='id', 'bucket'='0')", "bucket");
        assertInvalid("(id int)", "properties ('bucket'='-2')", "bucket");
        assertInvalid("(id int)", "properties ('bucket'='2')", "bucket-key");
    }

    @Test
    public void testTableOptionsAreNormalizedAndCopied() {
        CreateTableStmt stmt = parse("create table p.db.t (id int) engine=paimon primary key(id) " +
                "properties ('BUCKET'='4', 'file.compression'='zstd')");
        Map<String, String> original = Map.copyOf(stmt.getProperties());
        Schema schema = PaimonTableSchemaConverter.toSchema(stmt);
        Assertions.assertEquals(Map.of("bucket", "4", "file.compression", "zstd"), schema.options());
        Assertions.assertEquals(original, stmt.getProperties());
        stmt.getProperties().put("BUCKET", "8");
        Assertions.assertEquals("4", schema.options().get("bucket"));
    }

    @Test
    public void testCanonicalOptionNamesArePreserved() {
        CreateTableStmt stmt = parse("create table p.db.t (id int) engine=paimon " +
                "properties ('VARIANT.INFERSHREDDINGSCHEMA'='false', 'variant.shreddingSchema'='ROW<a INT>')");
        Map<String, String> original = Map.copyOf(stmt.getProperties());
        Schema schema = PaimonTableSchemaConverter.toSchema(stmt);
        Assertions.assertEquals("false", schema.options().get("variant.inferShreddingSchema"));
        Assertions.assertEquals("ROW<a INT>", schema.options().get("variant.shreddingSchema"));
        Assertions.assertEquals(original, stmt.getProperties());
        stmt.getProperties().put("variant.inferShreddingSchema", "true");
        Assertions.assertThrows(SemanticException.class, () -> PaimonTableSchemaConverter.toSchema(stmt));
    }

    @Test
    public void testUnsupportedScalarAndNestedTypesFail() {
        Assertions.assertThrows(SemanticException.class, () -> schemaFromColumns(
                List.of(new Column("id", IntegerType.LARGEINT))));
        Assertions.assertThrows(SemanticException.class, () -> schemaFromColumns(
                List.of(new Column("nested", new ArrayType(IntegerType.LARGEINT)))));
    }

    private static RowType schemaFromColumns(List<Column> columns) {
        List<ColumnDef> definitions = new ArrayList<>();
        for (Column column : columns) {
            definitions.add(new ColumnDef(column.getName(), new TypeDef(column.getType()), false, null, null,
                    column.isAllowNull(), ColumnDef.DefaultValueDef.NOT_SET, column.getComment()));
        }
        CreateTableStmt stmt = new CreateTableStmt(false, true,
                new TableRef(QualifiedName.of(List.of("p", "db", "t")), null, NodePosition.ZERO),
                definitions, "paimon", null, null, null, null, null, "");
        return PaimonTableSchemaConverter.toSchema(stmt).rowType();
    }

    private static void assertUniqueIds(DataType type, Set<Integer> ids) {
        if (type instanceof RowType) {
            for (DataField field : ((RowType) type).getFields()) {
                Assertions.assertTrue(ids.add(field.id()), "Duplicate field ID: " + field.id());
                assertUniqueIds(field.type(), ids);
            }
        } else if (type instanceof org.apache.paimon.types.ArrayType) {
            assertUniqueIds(((org.apache.paimon.types.ArrayType) type).getElementType(), ids);
        } else if (type instanceof org.apache.paimon.types.MapType) {
            assertUniqueIds(((org.apache.paimon.types.MapType) type).getKeyType(), ids);
            assertUniqueIds(((org.apache.paimon.types.MapType) type).getValueType(), ids);
        }
    }

    private static void assertInvalid(String columns, String clauses, String message) {
        SemanticException error = Assertions.assertThrows(SemanticException.class, () ->
                PaimonTableSchemaConverter.toSchema(parse("create table p.db.t " + columns + " engine=paimon " + clauses)));
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    private static CreateTableStmt parse(String sql) {
        return (CreateTableStmt) SqlParser.parse(sql, 0L).get(0);
    }
}
