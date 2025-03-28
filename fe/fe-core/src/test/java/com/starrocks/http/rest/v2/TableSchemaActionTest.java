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

package com.starrocks.http.rest.v2;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Type;
import com.starrocks.http.StarRocksHttpTestCase;
import com.starrocks.http.rest.v2.vo.ColumnView;
import com.starrocks.http.rest.v2.vo.DistributionInfoView;
import com.starrocks.http.rest.v2.vo.IndexView;
import com.starrocks.http.rest.v2.vo.MaterializedIndexMetaView;
import com.starrocks.http.rest.v2.vo.PartitionInfoView;
import com.starrocks.http.rest.v2.vo.TableSchemaView;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.thrift.TStorageType;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.JVM)
public class TableSchemaActionTest extends StarRocksHttpTestCase {

    private static final String TABLE_SCHEMA_URL_PATTERN =
            BASE_URL + "/api/v2/catalogs/%s/databases/%s/tables/%s/schema";

    private static final Long TB_GET_TABLE_SCHEMA_ID = testTableId + 10000L;
    private static final String TB_GET_TABLE_SCHEMA_NAME = "tb_table_schema_test";

    private static final Gson GSON = GsonUtils.GSON.newBuilder()
            .registerTypeAdapter(ColumnView.TypeView.class,
                    (JsonDeserializer<ColumnView.TypeView>) (json, typeOf, context) -> {
                        JsonObject jsonObj = json.getAsJsonObject();
                        JsonElement nameElement = jsonObj.get("name");
                        if (nameElement == null) {
                            throw new JsonParseException("Missing 'name' field in JSON.");
                        }

                        String typeName = nameElement.getAsString();
                        if (ColumnView.ArrayTypeView.TYPE_NAME.equalsIgnoreCase(typeName)) {
                            return context.deserialize(json, ColumnView.ArrayTypeView.class);
                        } else if (ColumnView.StructTypeView.TYPE_NAME.equalsIgnoreCase(typeName)) {
                            return context.deserialize(json, ColumnView.StructTypeView.class);
                        } else if (ColumnView.MapTypeView.TYPE_NAME.equalsIgnoreCase(typeName)) {
                            return context.deserialize(json, ColumnView.MapTypeView.class);
                        } else {
                            return context.deserialize(json, ColumnView.ScalarTypeView.class);
                        }
                    }).create();

    @Override
    protected void doSetUp() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(testDbId);
        db.registerTableUnlocked(newOlapTable(TB_GET_TABLE_SCHEMA_ID, TB_GET_TABLE_SCHEMA_NAME));
    }

    @Test
    public void testNonOlapTable() throws Exception {
        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, ES_TABLE_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<TableSchemaView> resp = parseResponseBody(response.body().string());
            assertEquals("403", resp.getCode());
            assertTrue(resp.getMessage().contains("is not a OLAP table"));
        }
    }

    @Test
    public void testOlapTable() throws Exception {
        Request request = newRequest(DEFAULT_INTERNAL_CATALOG_NAME, DB_NAME, TB_GET_TABLE_SCHEMA_NAME);
        try (Response response = networkClient.newCall(request).execute()) {
            RestBaseResultV2<TableSchemaView> resp = parseResponseBody(response.body().string());
            assertEquals("0", resp.getCode());
            TableSchemaView tableSchema = resp.getResult();
            assertEquals(TB_GET_TABLE_SCHEMA_ID, tableSchema.getId());
            assertEquals(TB_GET_TABLE_SCHEMA_NAME, tableSchema.getName());
            assertEquals(Table.TableType.serialize(Table.TableType.OLAP), tableSchema.getTableType());
            assertEquals(KeysType.DUP_KEYS.name(), tableSchema.getKeysType());
            assertEquals("c_" + TB_GET_TABLE_SCHEMA_NAME, tableSchema.getComment());
            assertTrue(tableSchema.getCreateTime() > 0L);

            List<ColumnView> columns = tableSchema.getColumns();
            assertEquals(8, columns.size());
            {
                ColumnView column = columns.get(0);
                assertEquals("c1", column.getName());
                ColumnView.ScalarTypeView colType = (ColumnView.ScalarTypeView) column.getType();
                assertEquals(PrimitiveType.DOUBLE.name(), colType.getName());
                assertEquals(Type.DOUBLE.getTypeSize(), colType.getTypeSize().intValue());
                assertEquals(Type.DOUBLE.getColumnSize(), colType.getColumnSize());
                assertEquals(0, colType.getPrecision().intValue());
                assertEquals(0, colType.getScale().intValue());
                assertNull(column.getAggregationType());
                assertTrue(column.getKey());
                assertFalse(column.getAllowNull());
                assertFalse(column.getAutoIncrement());
                assertNull(column.getDefaultValue());
                assertEquals("NULL", column.getDefaultValueType());
                assertNull(column.getDefaultExpr());
                assertEquals("cc1", column.getComment());
                assertEquals(1, column.getUniqueId().intValue());
            }

            {
                ColumnView column = columns.get(1);
                assertEquals("c2", column.getName());
                ColumnView.ScalarTypeView colType = (ColumnView.ScalarTypeView) column.getType();
                assertEquals(PrimitiveType.DECIMAL64.name(), colType.getName());
                assertEquals(Type.DEFAULT_DECIMAL64.getTypeSize(), colType.getTypeSize().intValue());
                assertEquals(Type.DEFAULT_DECIMAL64.getColumnSize(), colType.getColumnSize());
                assertEquals(Type.DEFAULT_DECIMAL64.getPrecision(), colType.getPrecision());
                assertEquals(Type.DEFAULT_DECIMAL64.getScalarScale(), colType.getScale().intValue());
                assertNull(column.getAggregationType());
                assertFalse(column.getKey());
                assertTrue(column.getAllowNull());
                assertFalse(column.getAutoIncrement());
                assertEquals("0", column.getDefaultValue());
                assertEquals("CONST", column.getDefaultValueType());
                assertNull(column.getDefaultExpr());
                assertEquals("cc2", column.getComment());
                assertEquals(2, column.getUniqueId().intValue());
            }

            {
                ColumnView column = columns.get(2);
                assertEquals("c3", column.getName());

                ColumnView.ArrayTypeView colType = (ColumnView.ArrayTypeView) column.getType();
                assertEquals(ColumnView.ArrayTypeView.TYPE_NAME, colType.getName());
                {
                    ColumnView.ScalarTypeView itemType = (ColumnView.ScalarTypeView) colType.getItemType();
                    assertEquals(PrimitiveType.INT.name(), itemType.getName());
                    assertEquals(Type.INT.getTypeSize(), itemType.getTypeSize().intValue());
                    assertEquals(Type.INT.getColumnSize(), itemType.getColumnSize());
                    assertEquals(0, itemType.getPrecision().intValue());
                    assertEquals(0, itemType.getScale().intValue());
                }

                assertNull(column.getAggregationType());
                assertFalse(column.getKey());
                assertTrue(column.getAllowNull());
                assertFalse(column.getAutoIncrement());
                assertNull(column.getDefaultValue());
                assertEquals("NULL", column.getDefaultValueType());
                assertNull(column.getDefaultExpr());
                assertEquals("cc3", column.getComment());
                assertEquals(3, column.getUniqueId().intValue());
            }

            {
                ColumnView column = columns.get(3);
                assertEquals("c4", column.getName());

                ColumnView.StructTypeView colType = (ColumnView.StructTypeView) column.getType();
                assertEquals(ColumnView.StructTypeView.TYPE_NAME, colType.getName());
                {
                    List<ColumnView.StructTypeView.FieldView> structFields = colType.getFields();
                    assertEquals(2, structFields.size());

                    {
                        ColumnView.StructTypeView.FieldView field = structFields.get(0);
                        assertEquals("c4_a", field.getName());
                        ColumnView.ArrayTypeView fieldType = (ColumnView.ArrayTypeView) field.getType();
                        assertEquals(ColumnView.ArrayTypeView.TYPE_NAME, fieldType.getName());

                        ColumnView.ScalarTypeView fieldItemType = (ColumnView.ScalarTypeView) fieldType.getItemType();
                        assertEquals(PrimitiveType.INT.name(), fieldItemType.getName());
                        assertEquals(Type.INT.getTypeSize(), fieldItemType.getTypeSize().intValue());
                        assertEquals(Type.INT.getColumnSize(), fieldItemType.getColumnSize());
                        assertEquals(0, fieldItemType.getPrecision().intValue());
                        assertEquals(0, fieldItemType.getScale().intValue());
                    }

                    {
                        ColumnView.StructTypeView.FieldView field = structFields.get(1);
                        assertEquals("c4_b", field.getName());
                        ColumnView.StructTypeView fieldType = (ColumnView.StructTypeView) field.getType();
                        assertEquals(ColumnView.StructTypeView.TYPE_NAME, fieldType.getName());

                        List<ColumnView.StructTypeView.FieldView> subStructFields = fieldType.getFields();
                        assertEquals(1, subStructFields.size());

                        {
                            ColumnView.StructTypeView.FieldView subField = subStructFields.get(0);
                            assertEquals("c4_b_1", subField.getName());
                            ColumnView.ScalarTypeView subFieldType = (ColumnView.ScalarTypeView) subField.getType();
                            assertEquals(PrimitiveType.VARCHAR.name(), subFieldType.getName());
                            assertEquals(Type.VARCHAR.getTypeSize(), subFieldType.getTypeSize().intValue());
                            assertEquals(Type.VARCHAR.getColumnSize(), subFieldType.getColumnSize());
                            assertEquals(0, subFieldType.getPrecision().intValue());
                            assertEquals(0, subFieldType.getScale().intValue());
                        }

                    }
                }

                assertNull(column.getAggregationType());
                assertFalse(column.getKey());
                assertTrue(column.getAllowNull());
                assertFalse(column.getAutoIncrement());
                assertNull(column.getDefaultValue());
                assertEquals("NULL", column.getDefaultValueType());
                assertNull(column.getDefaultExpr());
                assertEquals("cc4", column.getComment());
                assertEquals(4, column.getUniqueId().intValue());
            }

            {
                ColumnView column = columns.get(4);
                assertEquals("c5", column.getName());

                ColumnView.MapTypeView colType = (ColumnView.MapTypeView) column.getType();
                assertEquals(ColumnView.MapTypeView.TYPE_NAME, colType.getName());

                {
                    ColumnView.ScalarTypeView keyType = (ColumnView.ScalarTypeView) colType.getKeyType();
                    assertEquals(PrimitiveType.BIGINT.name(), keyType.getName());
                    assertEquals(Type.BIGINT.getTypeSize(), keyType.getTypeSize().intValue());
                    assertEquals(Type.BIGINT.getColumnSize(), keyType.getColumnSize());
                    assertEquals(0, keyType.getPrecision().intValue());
                    assertEquals(0, keyType.getScale().intValue());
                }

                {
                    ColumnView.MapTypeView valueType = (ColumnView.MapTypeView) colType.getValueType();
                    assertEquals(ColumnView.MapTypeView.TYPE_NAME, valueType.getName());

                    ColumnView.ScalarTypeView valKeyType = (ColumnView.ScalarTypeView) valueType.getKeyType();
                    assertEquals(PrimitiveType.INT.name(), valKeyType.getName());
                    assertEquals(Type.INT.getTypeSize(), valKeyType.getTypeSize().intValue());
                    assertEquals(Type.INT.getColumnSize(), valKeyType.getColumnSize());
                    assertEquals(0, valKeyType.getPrecision().intValue());
                    assertEquals(0, valKeyType.getScale().intValue());

                    ColumnView.ScalarTypeView valValueType = (ColumnView.ScalarTypeView) valueType.getValueType();
                    assertEquals(PrimitiveType.VARCHAR.name(), valValueType.getName());
                    assertEquals(Type.VARCHAR.getTypeSize(), valValueType.getTypeSize().intValue());
                    assertEquals(Type.VARCHAR.getColumnSize(), valValueType.getColumnSize());
                    assertEquals(0, valValueType.getPrecision().intValue());
                    assertEquals(0, valValueType.getScale().intValue());
                }

                assertNull(column.getAggregationType());
                assertFalse(column.getKey());
                assertFalse(column.getAllowNull());
                assertFalse(column.getAutoIncrement());
                assertNull(column.getDefaultValue());
                assertEquals("NULL", column.getDefaultValueType());
                assertNull(column.getDefaultExpr());
                assertEquals("cc5", column.getComment());
                assertEquals(5, column.getUniqueId().intValue());
            }

            {
                ColumnView column = columns.get(5);
                assertEquals("c6", column.getName());

                ColumnView.ScalarTypeView colType = (ColumnView.ScalarTypeView) column.getType();
                assertEquals(PrimitiveType.JSON.name(), colType.getName());
                assertEquals(Type.JSON.getTypeSize(), colType.getTypeSize().intValue());
                assertEquals(Type.JSON.getColumnSize(), colType.getColumnSize());
                assertEquals(0, colType.getPrecision().intValue());
                assertEquals(0, colType.getScale().intValue());

                assertNull(column.getAggregationType());
                assertFalse(column.getKey());
                assertFalse(column.getAllowNull());
                assertFalse(column.getAutoIncrement());
                assertNull(column.getDefaultValue());
                assertEquals("NULL", column.getDefaultValueType());
                assertNull(column.getDefaultExpr());
                assertEquals("cc6", column.getComment());
                assertEquals(6, column.getUniqueId().intValue());
            }

            {
                ColumnView column = columns.get(6);
                assertEquals("c7", column.getName());

                ColumnView.ScalarTypeView colType = (ColumnView.ScalarTypeView) column.getType();
                assertEquals(PrimitiveType.BITMAP.name(), colType.getName());
                assertEquals(Type.BITMAP.getTypeSize(), colType.getTypeSize().intValue());
                assertEquals(Type.BITMAP.getColumnSize(), colType.getColumnSize());
                assertEquals(0, colType.getPrecision().intValue());
                assertEquals(0, colType.getScale().intValue());

                assertNull(column.getAggregationType());
                assertFalse(column.getKey());
                assertFalse(column.getAllowNull());
                assertFalse(column.getAutoIncrement());
                assertNull(column.getDefaultValue());
                assertEquals("NULL", column.getDefaultValueType());
                assertNull(column.getDefaultExpr());
                assertEquals("cc7", column.getComment());
                assertEquals(7, column.getUniqueId().intValue());
            }

            {
                ColumnView column = columns.get(7);
                assertEquals("c8", column.getName());

                ColumnView.ScalarTypeView colType = (ColumnView.ScalarTypeView) column.getType();
                assertEquals(PrimitiveType.HLL.name(), colType.getName());
                assertEquals(Type.HLL.getTypeSize(), colType.getTypeSize().intValue());
                assertEquals(Type.HLL.getColumnSize(), colType.getColumnSize());
                assertEquals(0, colType.getPrecision().intValue());
                assertEquals(0, colType.getScale().intValue());

                assertNull(column.getAggregationType());
                assertFalse(column.getKey());
                assertFalse(column.getAllowNull());
                assertFalse(column.getAutoIncrement());
                assertNull(column.getDefaultValue());
                assertEquals("NULL", column.getDefaultValueType());
                assertNull(column.getDefaultExpr());
                assertEquals("cc8", column.getComment());
                assertEquals(8, column.getUniqueId().intValue());
            }

            List<MaterializedIndexMetaView> indexMetas = tableSchema.getIndexMetas();
            assertEquals(1, indexMetas.size());
            {
                MaterializedIndexMetaView indexMeta = indexMetas.get(0);
                assertEquals(TB_GET_TABLE_SCHEMA_ID, indexMeta.getIndexId());
                assertEquals(KeysType.DUP_KEYS.name(), indexMeta.getKeysType());
                List<ColumnView> cols = indexMeta.getColumns();
                assertEquals(1, cols.size());
                assertEquals("c1", cols.get(0).getName());
            }

            PartitionInfoView partitionInfo = tableSchema.getPartitionInfo();
            assertEquals(PartitionType.RANGE.typeString, partitionInfo.getType());
            List<ColumnView> partitionColumns = partitionInfo.getPartitionColumns();
            assertEquals(1, partitionColumns.size());
            assertEquals("c1", partitionColumns.get(0).getName());

            DistributionInfoView distributionInfo = tableSchema.getDefaultDistributionInfo();
            assertEquals(DistributionInfo.DistributionInfoType.HASH.name(), distributionInfo.getType());
            assertEquals(8, distributionInfo.getBucketNum());
            List<ColumnView> distributionColumns = distributionInfo.getDistributionColumns();
            assertEquals(1, distributionColumns.size());
            assertEquals("c1", distributionColumns.get(0).getName());

            assertEquals("cg1", tableSchema.getColocateGroup());

            List<IndexView> indexes = tableSchema.getIndexes();
            assertEquals(2, indexes.size());
            {
                IndexView idx = indexes.get(0);
                assertEquals(TB_GET_TABLE_SCHEMA_ID, idx.getIndexId());
                assertEquals("idx1", idx.getIndexName());
                assertEquals(IndexDef.IndexType.BITMAP.getDisplayName(), idx.getIndexType());
                assertEquals(1, idx.getColumns().size());
                assertEquals("c1", idx.getColumns().get(0).getId());
                assertEquals("c_idx1", idx.getComment());
                assertTrue(idx.getProperties().isEmpty());
            }

            {
                IndexView idx = indexes.get(1);
                assertEquals(TB_GET_TABLE_SCHEMA_ID + 1, idx.getIndexId().longValue());
                assertEquals("idx2", idx.getIndexName());
                assertEquals(IndexDef.IndexType.NGRAMBF.getDisplayName(), idx.getIndexType());
                assertEquals(1, idx.getColumns().size());
                assertEquals("c2", idx.getColumns().get(0).getId());
                assertEquals("c_idx2", idx.getComment());
                assertTrue(idx.getProperties().isEmpty());
            }

            assertEquals(TB_GET_TABLE_SCHEMA_ID, tableSchema.getBaseIndexId());
            assertEquals(TB_GET_TABLE_SCHEMA_ID + 65535L, tableSchema.getMaxIndexId().longValue());
            assertEquals(65535, tableSchema.getMaxColUniqueId().intValue());

            Map<String, String> properties = tableSchema.getProperties();
            assertEquals(2, properties.size());
            assertEquals("3", properties.get("replication_num"));
            assertEquals("cg1", properties.get("colocate_with"));
        }
    }

    private static OlapTable newOlapTable(Long tableId, String tableName) {
        GlobalStateMgr.getCurrentState().getTabletInvertedIndex().clear();

        Column c1 = new Column("c1", Type.DOUBLE, true, null, null, false, null, "cc1", 1);
        Column c2 = new Column("c2", Type.DEFAULT_DECIMAL64, false, null, null, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "cc2", 2);
        Column c3 = new Column("c3", new ArrayType(Type.INT), false, null, null, true, null, "cc3", 3);
        Column c4 = new Column("c4", new StructType(
                Lists.newArrayList(
                        new StructField("c4_a", new ArrayType(Type.INT)),
                        new StructField("c4_b", new StructType(Lists.newArrayList(new StructField("c4_b_1", Type.VARCHAR))))
                )), false, null, null, true, null, "cc4", 4);
        Column c5 = new Column("c5", new MapType(Type.BIGINT,
                new MapType(Type.INT, Type.VARCHAR)), false, null, null, false, null, "cc5", 5);
        Column c6 = new Column("c6", Type.JSON, false, null, null, false, null, "cc6", 6);
        Column c7 = new Column("c7", Type.BITMAP, false, null, null, false, null, "cc7", 7);
        Column c8 = new Column("c8", Type.HLL, false, null, null, false, null, "cc8", 8);
        List<Column> columns = Lists.newArrayList(c1, c2, c3, c4, c5, c6, c7, c8);

        PartitionInfo partitionInfo = new RangePartitionInfo(
                Lists.newArrayList(c1)
        );

        DistributionInfo distributionInfo = new HashDistributionInfo(8, Lists.newArrayList(c1));

        Index idx1 = new Index(
                tableId, "idx1", Lists.newArrayList(ColumnId.create("c1")),
                IndexDef.IndexType.BITMAP, "c_idx1", new HashMap<>());
        Index idx2 = new Index(
                tableId + 1, "idx2", Lists.newArrayList(ColumnId.create("c2")),
                IndexDef.IndexType.NGRAMBF, "c_idx2", new HashMap<>());
        TableIndexes indexes = new TableIndexes(
                Lists.newArrayList(idx1, idx2)
        );
        OlapTable olapTable = new OlapTable(
                tableId,
                tableName,
                columns,
                KeysType.DUP_KEYS,
                partitionInfo,
                distributionInfo,
                indexes,
                Table.TableType.OLAP
        );

        olapTable.setColocateGroup("cg1");
        olapTable.setBaseIndexId(idx1.getIndexId());
        olapTable.setMaxIndexId(idx1.getIndexId() + 65535L);
        olapTable.setMaxColUniqueId(65535);

        olapTable.setIndexMeta(
                idx1.getIndexId(), idx1.getIndexName(), Lists.newArrayList(c1), 0, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS
        );

        olapTable.setComment("c_" + TB_GET_TABLE_SCHEMA_NAME);

        TableProperty tableProperty = new TableProperty(new LinkedHashMap<>() {
            private static final long serialVersionUID = 486917502839696846L;

            {
                put("replication_num", "3");
                put("colocate_with", "cg1");
            }
        });
        olapTable.setTableProperty(tableProperty);
        return olapTable;
    }

    private static RestBaseResultV2<TableSchemaView> parseResponseBody(String body) {
        try {
            return GSON.fromJson(body, new TypeToken<RestBaseResultV2<TableSchemaView>>() {
            }.getType());
        } catch (Exception e) {
            fail(e.getMessage() + ", resp: " + body);
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private Request newRequest(String catalog, String database, String table) {
        return new Request.Builder()
                .get()
                .addHeader(AUTH_KEY, rootAuth)
                .url(toSchemaUrl(catalog, database, table))
                .build();
    }

    private static String toSchemaUrl(String catalog, String database, String table) {
        return String.format(TABLE_SCHEMA_URL_PATTERN, catalog, database, table);
    }

}