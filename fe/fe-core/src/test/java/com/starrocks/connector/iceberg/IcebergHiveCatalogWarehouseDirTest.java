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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREWAREHOUSE;

/**
 * {@code hive.metastore.warehouse.dir} given in the catalog properties must reach the Hadoop
 * Configuration the Hive catalog is built with.
 *
 * <p>The Configuration is built by {@code IcebergConnector} via {@code HdfsEnvironment}, which only
 * applies cloud-storage credentials to it — catalog properties such as the warehouse directory are
 * not propagated. So the constructor cannot treat "absent from the Configuration" as "the user did
 * not configure it", or a configured warehouse dir is silently replaced by the built-in default and
 * CREATE DATABASE fails against a path the user never asked for.
 */
public class IcebergHiveCatalogWarehouseDirTest {

    private static Map<String, String> properties(String warehouseDir) {
        Map<String, String> properties = new HashMap<>();
        properties.put("hive.metastore.uris", "thrift://129.1.2.3:9876");
        if (warehouseDir != null) {
            properties.put(METASTOREWAREHOUSE.varname, warehouseDir);
        }
        return properties;
    }

    /** Trigger: configured in the catalog properties, absent from the Configuration. */
    @Test
    public void warehouseDirFromCatalogProperties(@Mocked HiveCatalog hiveCatalog) {
        Configuration conf = new Configuration();
        new IcebergHiveCatalog("iceberg_hive", conf, properties("hdfs://ns1/iceberg"));
        Assertions.assertEquals("hdfs://ns1/iceberg", conf.get(METASTOREWAREHOUSE.varname));
    }

    /** Control: nothing configured anywhere still falls back to the default. */
    @Test
    public void warehouseDirFallsBackToDefault(@Mocked HiveCatalog hiveCatalog) {
        Configuration conf = new Configuration();
        new IcebergHiveCatalog("iceberg_hive", conf, properties(null));
        Assertions.assertEquals(METASTOREWAREHOUSE.getDefaultValue(), conf.get(METASTOREWAREHOUSE.varname));
    }

    /** Control: a value already on the Configuration keeps precedence. */
    @Test
    public void warehouseDirOnConfigurationWins(@Mocked HiveCatalog hiveCatalog) {
        Configuration conf = new Configuration();
        conf.set(METASTOREWAREHOUSE.varname, "hdfs://ns1/from-configuration");
        new IcebergHiveCatalog("iceberg_hive", conf, properties("hdfs://ns1/from-properties"));
        Assertions.assertEquals("hdfs://ns1/from-configuration", conf.get(METASTOREWAREHOUSE.varname));
    }
}
