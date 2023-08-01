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

import com.google.common.collect.Lists;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.InfoSchemaWrappedConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonConnectorTest {
    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testCreatePaimonConnector() {
        Map<String, String> properties = new HashMap<>();

        Assert.assertThrows("The property paimon.catalog.type must be set.", StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)));

        properties.put("paimon.catalog.type", "filesystem");

        Assert.assertThrows("The property paimon.catalog.warehouse must be set.", StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)));
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");

        new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
    }

    @Test
    public void testCreateHivePaimonConnectorWithoutUris() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "hive");
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");

        Assert.assertThrows("The property hive.metastore.uris must be set if paimon catalog is hive.",
                StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)));

        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");

        new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
    }

    @Test
    public void testCreatePaimonTable(@Mocked Catalog paimonNativeCatalog,
                                      @Mocked AbstractFileStoreTable paimonNativeTable) throws Catalog.TableNotExistException {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");
        properties.put("paimon.catalog.type", "filesystem");
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        List<DataField> fields = new ArrayList<>();
        fields.add(new DataField(1, "col2", new IntType()));
        new Expectations(connector) {
            {
                connector.getPaimonNativeCatalog();
                result = paimonNativeCatalog;
                paimonNativeCatalog.getTable((Identifier) any);
                result = paimonNativeTable;
                paimonNativeTable.rowType().getFields();
                result = fields;
                paimonNativeTable.location().toString();
                result = "hdfs://127.0.0.1:10000/paimon";
                paimonNativeTable.partitionKeys();
                result = new ArrayList<>(Collections.singleton("col1"));
            }
        };

        ConnectorMetadata metadata = connector.getMetadata();
        Assert.assertTrue(metadata instanceof InfoSchemaWrappedConnectorMetadata);
        com.starrocks.catalog.Table table = metadata.getTable("db1", "tbl1");
        PaimonTable paimonTable = (PaimonTable) table;
        Assert.assertEquals("db1", paimonTable.getDbName());
        Assert.assertEquals("tbl1", paimonTable.getTableName());
        Assert.assertEquals(Lists.newArrayList("col1"), paimonTable.getPartitionColumnNames());
        Assert.assertEquals("hdfs://127.0.0.1:10000/paimon", paimonTable.getTableLocation());
        Assert.assertEquals(ScalarType.INT, paimonTable.getBaseSchema().get(0).getType());
        Assert.assertEquals("paimon_catalog", paimonTable.getCatalogName());
    }
}
