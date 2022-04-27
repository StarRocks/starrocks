// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HiveTableTest {

    @Test
    public void testIsRefreshColumn() throws DdlException {
        HiveTable hiveTable = new HiveTable();
        FieldSchema col1Schema = new FieldSchema("col1", "BIGINT", "");
        Map<String, FieldSchema> col1HiveSchemaMap = new HashMap<>();
        col1HiveSchemaMap.put(col1Schema.getName(), col1Schema);
        Column column = new Column("col2", Type.BIGINT);
        Map<String, Column> srSchemaMap = new HashMap<>();
        srSchemaMap.put(column.getName(), column);
        // different col name
        Assert.assertTrue(hiveTable.isRefreshColumn(srSchemaMap, col1HiveSchemaMap));
        // no fresh
        FieldSchema col2Schema = new FieldSchema("col2", "BIGINT", "");
        Map<String, FieldSchema> col2HiveSchemaMap = new HashMap<>();
        col2HiveSchemaMap.put(col2Schema.getName(), col2Schema);
        Assert.assertFalse(hiveTable.isRefreshColumn(srSchemaMap, col2HiveSchemaMap));
        // different col type
        FieldSchema col3Schema = new FieldSchema("col2", "INT", "");
        Map<String, FieldSchema> col3HiveSchemaMap = new HashMap<>();
        col2HiveSchemaMap.put(col3Schema.getName(), col3Schema);
        Assert.assertTrue(hiveTable.isRefreshColumn(srSchemaMap, col3HiveSchemaMap));
        // different col size
        col2HiveSchemaMap.put(col1Schema.getName(), col1Schema);
        Assert.assertTrue(hiveTable.isRefreshColumn(srSchemaMap, col1HiveSchemaMap));
    }
}
