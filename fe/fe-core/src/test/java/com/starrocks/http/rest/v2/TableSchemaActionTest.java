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

import com.google.gson.reflect.TypeToken;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
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
import com.starrocks.thrift.TStorageType;
import okhttp3.Request;
import okhttp3.Response;
import org.assertj.core.util.Lists;
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

    @Override
    protected void doSetUp() {
        Database db = GlobalStateMgr.getCurrentState().getDb(testDbId);
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
            assertEquals(2, columns.size());
            {
                ColumnView column = columns.get(0);
                assertEquals("c1", column.getName());
                assertEquals(PrimitiveType.DOUBLE.toString(), column.getPrimitiveType());
                assertEquals(8, column.getPrimitiveTypeSize().intValue());
                assertEquals(Type.DOUBLE.getColumnSize(), column.getColumnSize());
                assertEquals(0, column.getPrecision().intValue());
                assertEquals(0, column.getScale().intValue());
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
                assertEquals(PrimitiveType.DECIMAL64.toString(), column.getPrimitiveType());
                assertEquals(8, column.getPrimitiveTypeSize().intValue());
                assertEquals(Type.DEFAULT_DECIMAL64.getColumnSize(), column.getColumnSize());
                assertEquals(18, column.getPrecision().intValue());
                assertEquals(6, column.getScale().intValue());
                assertEquals(AggregateType.SUM.toSql(), column.getAggregationType());
                assertFalse(column.getKey());
                assertTrue(column.getAllowNull());
                assertFalse(column.getAutoIncrement());
                assertEquals("0", column.getDefaultValue());
                assertEquals("CONST", column.getDefaultValueType());
                assertNull(column.getDefaultExpr());
                assertEquals("cc2", column.getComment());
                assertEquals(2, column.getUniqueId().intValue());
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
                assertEquals("c1", idx.getColumns().get(0));
                assertEquals("c_idx1", idx.getComment());
                assertTrue(idx.getProperties().isEmpty());
            }

            {
                IndexView idx = indexes.get(1);
                assertEquals(TB_GET_TABLE_SCHEMA_ID + 1, idx.getIndexId().longValue());
                assertEquals("idx2", idx.getIndexName());
                assertEquals(IndexDef.IndexType.NGRAMBF.getDisplayName(), idx.getIndexType());
                assertEquals(1, idx.getColumns().size());
                assertEquals("c2", idx.getColumns().get(0));
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

        Column c1 = new Column("c1", Type.DOUBLE, true, null, false, null, "cc1", 1);
        Column c2 = new Column("c2", Type.DEFAULT_DECIMAL64, false, AggregateType.SUM, true,
                new ColumnDef.DefaultValueDef(true, new StringLiteral("0")), "cc2", 2);
        List<Column> columns = Lists.newArrayList(c1, c2);

        PartitionInfo partitionInfo = new RangePartitionInfo(
                Lists.newArrayList(c1)
        );

        DistributionInfo distributionInfo = new HashDistributionInfo(8, Lists.newArrayList(c1));

        Index idx1 = new Index(
                tableId, "idx1", Lists.newArrayList("c1"),
                IndexDef.IndexType.BITMAP, "c_idx1", new HashMap<>());
        Index idx2 = new Index(
                tableId + 1, "idx2", Lists.newArrayList("c2"),
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
            System.out.println("resp: " + body);
            return GsonUtils.GSON.fromJson(body, new TypeToken<RestBaseResultV2<TableSchemaView>>() {
            }.getType());
        } catch (Exception e) {
            fail("invalid resp body: " + body);
            throw new IllegalStateException(e);
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