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
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.type.IntegerType;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.IntType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PaimonConnectorTest {

    @Test
    public void testCreatePaimonConnector() {
        Map<String, String> properties = new HashMap<>();

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)),
                "The property paimon.catalog.type must be set.");

        properties.put("paimon.catalog.type", "filesystem");

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)),
                "The property paimon.catalog.warehouse must be set.");
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");

        new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
    }

    @Test
    public void testCreateHivePaimonConnectorWithoutUris() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "hive");
        properties.put("paimon.catalog.warehouse", "hdfs://127.0.0.1:9999/warehouse");

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties)),
                "The property hive.metastore.uris must be set if paimon catalog is hive.");

        properties.put("hive.metastore.uris", "thrift://127.0.0.1:9083");

        new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
    }

    @Test
    public void testCreateDLFPaimonConnector() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.type", "dlf");
        properties.put("dlf.catalog.id", "dlf_test");

        new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
    }

    @Test
    public void testCreatePaimonTable(@Mocked Catalog paimonNativeCatalog,
                                      @Mocked FileStoreTable paimonNativeTable) throws Catalog.TableNotExistException {
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
        Assertions.assertTrue(metadata instanceof PaimonMetadata);
        com.starrocks.catalog.Table table = metadata.getTable(new ConnectContext(), "db1", "tbl1");
        PaimonTable paimonTable = (PaimonTable) table;
        Assertions.assertEquals("db1", paimonTable.getCatalogDBName());
        Assertions.assertEquals("tbl1", paimonTable.getCatalogTableName());
        Assertions.assertEquals(Lists.newArrayList("col1"), paimonTable.getPartitionColumnNames());
        Assertions.assertEquals("hdfs://127.0.0.1:10000/paimon", paimonTable.getTableLocation());
        Assertions.assertEquals(IntegerType.INT, paimonTable.getBaseSchema().get(0).getType());
        Assertions.assertEquals("paimon_catalog", paimonTable.getCatalogName());
    }

    @Test
    public void testCreatePaimonConnectorWithS3() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.warehouse", "s3://bucket/warehouse");
        properties.put("paimon.catalog.type", "filesystem");
        String accessKeyValue = "s3_access_key";
        String secretKeyValue = "s3_secret_key";
        String endpointValue = "s3_endpoint";
        properties.put("aws.s3.access_key", accessKeyValue);
        properties.put("aws.s3.secret_key", secretKeyValue);
        properties.put("aws.s3.endpoint", endpointValue);
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Options paimonOptions = connector.getPaimonOptions();
        String accessKeyOption = paimonOptions.get("s3.access-key");
        String secretKeyOption = paimonOptions.get("s3.secret-key");
        String endpointOption = paimonOptions.get("s3.endpoint");
        Assertions.assertEquals(accessKeyOption, accessKeyValue);
        Assertions.assertEquals(secretKeyOption, secretKeyValue);
        Assertions.assertEquals(endpointOption, endpointValue);
    }

    @Test
    public void testCreatePaimonConnectorWithOSS() {
        Map<String, String> properties = new HashMap<>();
        properties.put("paimon.catalog.warehouse", "oss://bucket/warehouse");
        properties.put("paimon.catalog.type", "filesystem");
        String accessKeyValue = "oss_access_key";
        String secretKeyValue = "oss_secret_key";
        String endpointValue = "oss_endpoint";
        properties.put("aliyun.oss.access_key", accessKeyValue);
        properties.put("aliyun.oss.secret_key", secretKeyValue);
        properties.put("aliyun.oss.endpoint", endpointValue);
        PaimonConnector connector = new PaimonConnector(new ConnectorContext("paimon_catalog", "paimon", properties));
        Options paimonOptions = connector.getPaimonOptions();
        String accessKeyOption = paimonOptions.get("fs.oss.accessKeyId");
        String secretKeyOption = paimonOptions.get("fs.oss.accessKeySecret");
        String endpointOption = paimonOptions.get("fs.oss.endpoint");
        Assertions.assertEquals(accessKeyOption, accessKeyValue);
        Assertions.assertEquals(secretKeyOption, secretKeyValue);
        Assertions.assertEquals(endpointOption, endpointValue);
    }
}
