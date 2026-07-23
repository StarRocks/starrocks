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

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.thrift.TFlussTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

public class FlussTableTest {
    private static final String CATALOG = "fluss0";
    private static final String DB = "db1";
    private static final String TABLE = "orders";
    private static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    private static final String FLUSS_CLIENT_ID = "client.id";
    private static final String LAKE_WAREHOUSE = "table.datalake.paimon.warehouse";
    private static final String LAKE_METASTORE = "table.datalake.paimon.metastore";
    private static final String TABLE_LOCAL_OPTION = "table.local.option";

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    @Test
    public void testBuildRuntimeConf(@Mocked org.apache.fluss.client.table.Table nativeFlussTable) {
        TableInfo tableInfo = tableInfo();
        new Expectations() {
            {
                nativeFlussTable.getTableInfo();
                result = tableInfo;
            }
        };

        Configuration catalogConf = new Configuration();
        catalogConf.setString(BOOTSTRAP_SERVERS, "catalog-host:9123");
        catalogConf.setString(FLUSS_CLIENT_ID, "sr-fe");
        catalogConf.setString(LAKE_WAREHOUSE, "oss://catalog-warehouse");
        catalogConf.setString(LAKE_METASTORE, "filesystem");
        catalogConf.setString("catalog.only", "catalog-value");
        FlussTable table = new FlussTable(CATALOG, DB, TABLE, schema(), nativeFlussTable, catalogConf);

        Configuration runtimeConf = table.buildRuntimeConf();
        Map<String, String> runtimeConfMap = runtimeConf.toMap();
        Assertions.assertEquals("catalog-host:9123", runtimeConfMap.get(BOOTSTRAP_SERVERS));
        Assertions.assertEquals("sr-fe", runtimeConfMap.get(FLUSS_CLIENT_ID));
        Assertions.assertEquals("oss://table-warehouse", runtimeConfMap.get(LAKE_WAREHOUSE));
        Assertions.assertEquals("filesystem", runtimeConfMap.get(LAKE_METASTORE));
        Assertions.assertEquals("table-value", runtimeConfMap.get(TABLE_LOCAL_OPTION));
        Assertions.assertEquals("catalog-value", runtimeConfMap.get("catalog.only"));
        Assertions.assertEquals("oss://catalog-warehouse", catalogConf.toMap().get(LAKE_WAREHOUSE));
    }

    @Test
    public void testToThrift(@Mocked org.apache.fluss.client.table.Table nativeFlussTable) throws Exception {
        TableInfo tableInfo = tableInfo();
        new Expectations() {
            {
                nativeFlussTable.getTableInfo();
                result = tableInfo;
            }
        };

        ConnectContext context = UtFrameUtils.createDefaultCtx();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setTimeZone("Asia/Shanghai");
        context.setSessionVariable(sessionVariable);
        context.setThreadLocalInfo();

        Configuration catalogConf = new Configuration();
        catalogConf.setString(BOOTSTRAP_SERVERS, "catalog-host:9123");
        catalogConf.setString(FLUSS_CLIENT_ID, "sr-fe");
        catalogConf.setString(LAKE_WAREHOUSE, "oss://catalog-warehouse");
        catalogConf.setString(LAKE_METASTORE, "filesystem");
        FlussTable table = new FlussTable(CATALOG, DB, TABLE, schema(), nativeFlussTable, catalogConf);

        TTableDescriptor descriptor = table.toThrift(null);
        Assertions.assertEquals(TTableType.FLUSS_TABLE, descriptor.getTableType());
        Assertions.assertEquals(DB, descriptor.getDbName());
        Assertions.assertEquals(TABLE, descriptor.getTableName());
        Assertions.assertEquals(schema().size(), descriptor.getNumCols());

        TFlussTable tFlussTable = descriptor.getFlussTable();
        Assertions.assertTrue(tFlussTable.isSetRuntime_conf());
        Assertions.assertEquals(CATALOG, tFlussTable.getCatalog_name());
        Assertions.assertEquals("Asia/Shanghai", tFlussTable.getTime_zone());

        // BE JNI receives db/table from TTableDescriptor and the following encoded runtime config.
        // The Java reader then reopens Fluss and strips table.datalake.paimon.* for PaimonLakeSource.
        Configuration runtimeConf = decodeRuntimeConf(tFlussTable.getRuntime_conf());
        Map<String, String> runtimeConfMap = runtimeConf.toMap();
        Assertions.assertEquals("catalog-host:9123", runtimeConfMap.get(BOOTSTRAP_SERVERS));
        Assertions.assertEquals("sr-fe", runtimeConfMap.get(FLUSS_CLIENT_ID));
        Assertions.assertEquals("oss://table-warehouse", runtimeConfMap.get(LAKE_WAREHOUSE));
        Assertions.assertEquals("filesystem", runtimeConfMap.get(LAKE_METASTORE));
        Assertions.assertEquals("table-value", runtimeConfMap.get(TABLE_LOCAL_OPTION));
    }

    @Test
    public void testPartitionColumnsAndIdentifiers(@Mocked org.apache.fluss.client.table.Table nativeFlussTable) {
        TableInfo tableInfo = tableInfo();
        new Expectations() {
            {
                nativeFlussTable.getTableInfo();
                result = tableInfo;
            }
        };

        FlussTable table = new FlussTable(CATALOG, DB, TABLE, schema(), nativeFlussTable, new Configuration());
        Assertions.assertEquals(Arrays.asList("dt"), table.getPartitionColumnNames());
        Assertions.assertEquals(Arrays.asList(new Column("dt", StringType.STRING, true)), table.getPartitionColumns());
        Assertions.assertEquals(Arrays.asList("id", "name", "dt"), table.getFieldNames());
        Assertions.assertFalse(table.isUnPartitioned());
        Assertions.assertEquals(TablePath.of(DB, TABLE).toString(), table.getTableLocation());
        Assertions.assertTrue(table.getUUID().contains("fluss0.db1.orders.42"));
        Assertions.assertTrue(table.getTableIdentifier().contains(TABLE + ":fluss0.db1.orders.42"));
    }

    private static List<Column> schema() {
        return Arrays.asList(
                new Column("id", IntegerType.INT, true),
                new Column("name", StringType.STRING, true),
                new Column("dt", StringType.STRING, true));
    }

    private static TableInfo tableInfo() {
        Schema schema = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("dt", DataTypes.STRING())
                .build();
        TableDescriptor descriptor = TableDescriptor.builder()
                .schema(schema)
                .partitionedBy("dt")
                .distributedBy(3, "id")
                .property(LAKE_WAREHOUSE, "oss://table-warehouse")
                .property(TABLE_LOCAL_OPTION, "table-value")
                .build();
        return TableInfo.of(TablePath.of(DB, TABLE), 42L, 7, descriptor, 1000L, 2000L);
    }

    private static Configuration decodeRuntimeConf(String encodedRuntimeConf) throws Exception {
        byte[] bytes = Base64.getUrlDecoder().decode(encodedRuntimeConf);
        return InstantiationUtil.deserializeObject(bytes, FlussTableTest.class.getClassLoader());
    }
}
