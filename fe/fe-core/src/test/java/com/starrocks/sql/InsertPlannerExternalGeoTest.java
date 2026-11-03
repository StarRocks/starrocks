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

package com.starrocks.sql;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.type.ArrayType;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.MapType;
import com.starrocks.type.NullType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import mockit.Mock;
import mockit.MockUp;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InsertPlannerExternalGeoTest extends PlanTestNoneDBBase {
    private static Stream<Type> geoTypes() {
        return Stream.of(ColumnTypeConverter.fromIcebergType(Types.GeographyType.crs84()),
                ScalarType.createGeoType(PrimitiveType.GEOMETRY, GeoTypeDescriptor.geometry("EPSG:3857")));
    }

    private static Stream<Arguments> externalTypes() {
        return Stream.of(Table.TableType.ICEBERG, Table.TableType.HIVE, Table.TableType.TABLE_FUNCTION)
                .flatMap(sink -> geoTypes().flatMap(geo -> Stream.of(geo, new ArrayType(geo),
                        new MapType(VarcharType.VARCHAR, geo), new MapType(geo, IntegerType.INT),
                        new StructType(List.of(geo)), new ArrayType(new StructType(List.of(geo))))
                        .map(type -> Arguments.of(sink, type))));
    }

    private static void check(Table.TableType sink, Type target, Type output) {
        InsertPlanner.checkExternalWriteTypes(new Table(sink), List.of(new Column("g", target)),
                List.of(new ColumnRefOperator(1, output, "g", true)));
    }

    private static void reject(Table.TableType sink, Type target, Type output) {
        SemanticException error = assertThrows(SemanticException.class, () -> check(sink, target, output));
        assertEquals("Cannot write native geo values to external table column 'g' (target type " + target.toSql() +
                ", output type " + output.toSql() + "). " +
                "Use ST_AsText/ST_AsBinary to serialize geo values before writing", error.getDetailMsg());
    }

    @ParameterizedTest
    @MethodSource("externalTypes")
    public void testRejectExternalGeo(Table.TableType sink, Type geo) {
        // Direct FE calls also cover types currently blocked by the nested SQL analyzer.
        reject(sink, geo, geo);
        reject(sink, geo, NullType.NULL);
        reject(sink, VarcharType.VARCHAR, geo);
        assertDoesNotThrow(() -> check(Table.TableType.OLAP, geo, geo));
    }

    @Test
    public void testOrdinaryExternalTypes() {
        for (Table.TableType sink : List.of(Table.TableType.ICEBERG, Table.TableType.HIVE, Table.TableType.TABLE_FUNCTION)) {
            for (Type type : List.of(IntegerType.INT, VarcharType.VARCHAR, VarbinaryType.VARBINARY,
                    new ArrayType(IntegerType.INT), new MapType(VarcharType.VARCHAR, IntegerType.INT),
                    new StructType(List.of(IntegerType.INT)))) {
                assertDoesNotThrow(() -> check(sink, type, type));
            }
        }
    }

    @Test
    public void testIcebergPlans() throws Exception {
        starRocksAssert.withCatalog("CREATE EXTERNAL CATALOG external_geo_ut PROPERTIES " +
                "('type'='iceberg', 'iceberg.catalog.type'='hadoop', 'iceberg.catalog.warehouse'='file:///tmp/geo_ut')");
        org.apache.iceberg.Table nativeTable = mock(org.apache.iceberg.Table.class);
        when(nativeTable.spec()).thenReturn(PartitionSpec.unpartitioned());
        when(nativeTable.sortOrder()).thenReturn(SortOrder.unsorted());
        when(nativeTable.properties()).thenReturn(Map.of());
        when(nativeTable.io()).thenReturn(new HadoopFileIO());
        when(nativeTable.location()).thenReturn("file:///tmp/geo_ut/t");
        when(nativeTable.schema()).thenReturn(new Schema(Types.NestedField.optional(1, "g", Types.StringType.get())));
        IcebergTable table = IcebergTable.builder().setCatalogName("external_geo_ut").setCatalogDBName("db")
                .setCatalogTableName("t").setSrTableName("t").setNativeTable(nativeTable)
                .setFullSchema(List.of(new Column("g", VarcharType.VARCHAR))).build();
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(ConnectContext ctx, String catalog, String db) {
                return new Database(1, db);
            }

            @Mock
            public Table getTable(ConnectContext ctx, String catalog, String db, String name) {
                return table;
            }
        };
        String insert = "INSERT INTO external_geo_ut.db.t SELECT ";
        for (Type geo : geoTypes().toList()) {
            table.setNewFullSchema(List.of(new Column("g", geo)));
            assertThrows(SemanticException.class, () -> getFragmentPlan(insert + "NULL"));
        }
        table.setNewFullSchema(List.of(new Column("g", VarcharType.VARCHAR)));
        assertTrue(getFragmentPlan(insert + "'ordinary'").contains("Iceberg TABLE SINK"));
        for (String geo : List.of("ST_GeogFromText('POINT (1 2)')",
                "ST_GeomFromText('POINT (1 2)', 'EPSG:3857')")) {
            assertThrows(SemanticException.class, () -> getFragmentPlan(insert + geo));
            for (String serializer : List.of("ST_AsText", "ST_AsBinary")) {
                Type target = serializer.equals("ST_AsText") ? VarcharType.VARCHAR : VarbinaryType.VARBINARY;
                table.setNewFullSchema(List.of(new Column("g", target)));
                assertTrue(getFragmentPlan(insert + serializer + "(" + geo + ")").contains("Iceberg TABLE SINK"));
            }
        }
    }

    @Test
    public void testFilesPlans() throws Exception {
        for (String format : List.of("parquet", "orc", "csv")) {
            String insert = "INSERT INTO FILES('path'='file:///tmp/external_geo/', 'format'='" + format + "') SELECT ";
            assertTrue(getFragmentPlan(insert + "1 AS g").contains("TABLE FUNCTION TABLE SINK"));
            for (String geo : List.of("ST_GeogFromText('POINT (1 2)')",
                    "ST_GeomFromText('POINT (1 2)', 'EPSG:3857')")) {
                assertThrows(SemanticException.class, () -> getFragmentPlan(insert + geo + " AS g"));
                for (String serializer : List.of("ST_AsText", "ST_AsBinary")) {
                    assertTrue(getFragmentPlan(insert + serializer + "(" + geo + ") AS g")
                            .contains("TABLE FUNCTION TABLE SINK"));
                }
            }
        }
    }
}
