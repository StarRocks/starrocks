// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.external.HiveMetaStoreTableUtils.convertHiveTableColumnType;

public class HiveTableTest {

    @Test
    public void testIsRefreshColumn() throws DdlException {
        HiveTable hiveTable = new HiveTable();
        FieldSchema col1Schema = new FieldSchema("col1", "BIGINT", "");
        List<Column> columns = Lists.newArrayList();
        Type srType = convertHiveTableColumnType(col1Schema.getType());
        Column column = new Column(col1Schema.getName(), srType, true);
        columns.add(column);
        hiveTable.setNewFullSchema(columns);

        Map<String, FieldSchema> col1HiveSchemaMap = new HashMap<>();
        col1HiveSchemaMap.put(col1Schema.getName(), col1Schema);
        // no fresh
        Assert.assertFalse(hiveTable.isRefreshColumn(Lists.newArrayList(col1Schema)));

        FieldSchema col2Schema = new FieldSchema("col2", "BIGINT", "");
        Map<String, FieldSchema> col2HiveSchemaMap = new HashMap<>();
        col2HiveSchemaMap.put(col2Schema.getName(), col2Schema);
        // different col name
        Assert.assertTrue(hiveTable.isRefreshColumn(Lists.newArrayList(col2Schema)));

        // different col type
        FieldSchema col3Schema = new FieldSchema("col2", "INT", "");
        Map<String, FieldSchema> col3HiveSchemaMap = new HashMap<>();
        col2HiveSchemaMap.put(col3Schema.getName(), col3Schema);
        Assert.assertTrue(hiveTable.isRefreshColumn(Lists.newArrayList(col3Schema)));

        col1HiveSchemaMap.put(col3Schema.getName(), col3Schema);
        // different col size
        col2HiveSchemaMap.put(col1Schema.getName(), col1Schema);
        Assert.assertTrue(hiveTable.isRefreshColumn(Lists.newArrayList(col1Schema, col3Schema)));
    }
}
