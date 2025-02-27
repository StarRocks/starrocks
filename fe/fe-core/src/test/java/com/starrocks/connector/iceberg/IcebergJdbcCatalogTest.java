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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.connector.iceberg.jdbc.IcebergJdbcCatalog;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergJdbcCatalogTest {
    @Test
    public void testNewIcebergJdbcCatalog(@Mocked JdbcCatalog jdbcCatalog)  {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", "s3://path/to/warehouse")));
        assertEquals(e.getMessage(), String.format("Iceberg jdbc catalog must set uri  (\"%s\" = \"jdbc:mysql://host:port/db_name\").",
                ICEBERG_CUSTOM_PROPERTIES_PREFIX + CatalogProperties.URI));

        IllegalArgumentException e1 = assertThrows(IllegalArgumentException.class, () -> new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.uri", "jdbc:mysql://host:port/db_name")));
        assertEquals(e1.getMessage(), "Iceberg jdbc catalog must set warehouse location " +
                "(\"iceberg.catalog.warehouse\" = \"s3://path/to/warehouse\").");

        assertDoesNotThrow(() -> new IcebergJdbcCatalog("catalog", new Configuration(),
                ImmutableMap.of("iceberg.catalog.warehouse", "s3://path/to/warehouse",
                        "iceberg.catalog.uri", "jdbc:mysql://host:port/db_name")));
    }


    @Test
    public void testListAllDatabases(@Mocked JdbcCatalog jdbcCatalog) {
        new Expectations() {
            {
                jdbcCatalog.listNamespaces();
                result = ImmutableList.of(Namespace.of("db1"), Namespace.of("db2"));
                minTimes = 0;
            }
        };

        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", "s3://path/to/warehouse",
                "iceberg.catalog.uri", "jdbc:mysql://host:port/db_name"));
        List<String> dbs = icebergJdbcCatalog.listAllDatabases();
        assertEquals(Arrays.asList("db1", "db2"), dbs);
    }

    @Test
    public void testTableExists(@Mocked JdbcCatalog jdbcCatalog) {
        new Expectations() {
            {
                jdbcCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", "s3://path/to/warehouse",
                "iceberg.catalog.uri", "jdbc:mysql://host:port/db_name"));
        assertTrue(icebergJdbcCatalog.tableExists("db1", "tbl1"));
    }

    @Test
    public void testRenameTable(@Mocked JdbcCatalog jdbcCatalog) {
        new Expectations() {
            {
                jdbcCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", "s3://path/to/warehouse",
                "iceberg.catalog.uri", "jdbc:mysql://host:port/db_name"));
        icebergJdbcCatalog.renameTable("db", "tb1", "tb2");
        boolean exists = icebergJdbcCatalog.tableExists("db", "tbl2");
        Assert.assertTrue(exists);
    }
}
