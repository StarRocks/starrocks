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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.exception.DdlException;
import com.starrocks.connector.iceberg.TableTestBase;
import com.starrocks.external.table.IcebergTableFactory;
import mockit.Mocked;
import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Type.INT;
import static com.starrocks.external.table.ExternalTableFactory.RESOURCE;

public class IcebergTableTest extends TableTestBase {

    @Test(expected = DdlException.class)
    public void testValidateIcebergColumnType() throws DdlException {
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergTable oTable = new IcebergTable(1, "srTableName", "iceberg_catalog",
                "resource_name", "iceberg_db", "iceberg_table", columns, mockedNativeTableB, Maps.newHashMap());
        List<Column> inputColumns = Lists.newArrayList(new Column("k1", INT, true));
        IcebergTableFactory.validateIcebergColumnType(inputColumns, oTable);
    }

    @Test
    public void testCreateTableResourceName(@Mocked Table icebergNativeTable) throws DdlException {

        String resourceName = "Iceberg_resource_29bb53dc_7e04_11ee_9b35_00163e0e489a";
        Map<String, String> properties = new HashMap() {
            {
                put(RESOURCE, resourceName);
            }
        };

        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(1000)
                .setSrTableName("supplier")
                .setCatalogName("iceberg_catalog")
                .setRemoteDbName("iceberg_oss_tpch_1g_parquet_gzip")
                .setRemoteTableName("supplier")
                .setResourceName(resourceName)
                .setFullSchema(new ArrayList<>())
                .setNativeTable(icebergNativeTable)
                .setIcebergProperties(new HashMap<>());
        IcebergTable oTable = tableBuilder.build();
        IcebergTable.Builder newBuilder = IcebergTable.builder();
        IcebergTableFactory.copyFromCatalogTable(newBuilder, oTable, properties);
        IcebergTable table = newBuilder.build();
        Assert.assertEquals(table.getResourceName(), resourceName);
    }
}
