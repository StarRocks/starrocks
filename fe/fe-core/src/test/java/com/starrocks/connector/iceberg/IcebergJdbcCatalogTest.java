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
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.jdbc.IcebergJdbcCatalog;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergJdbcCatalogTest {

    private static String LOCATION = "s3://path/to/warehouse";
    private static String URI = "jdbc:mysql://host:port/db_name";

    @Test
    public void testNewIcebergJdbcCatalog(@Mocked JdbcCatalog jdbcCatalog) {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> new IcebergJdbcCatalog("catalog",
                        new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION)));
        assertEquals(e.getMessage(),
                String.format("Iceberg jdbc catalog must set uri  (\"%s\" = \"jdbc:[mysql|postgresql]://host:port/db_name\").",
                        ICEBERG_CUSTOM_PROPERTIES_PREFIX + CatalogProperties.URI));

        IllegalArgumentException e1 =
                assertThrows(IllegalArgumentException.class, () -> new IcebergJdbcCatalog("catalog",
                        new Configuration(), ImmutableMap.of("iceberg.catalog.uri", URI)));
        assertEquals(e1.getMessage(), "Iceberg jdbc catalog must set warehouse location " +
                "(\"iceberg.catalog.warehouse\" = \"s3://path/to/warehouse\").");

        assertDoesNotThrow(() -> new IcebergJdbcCatalog("catalog", new Configuration(),
                ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                        "iceberg.catalog.uri", URI)));
    }

    @Test
    public void testGetIcebergCatalogType(@Mocked JdbcCatalog jdbcCatalog) {
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog", new Configuration(),
                ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                        "iceberg.catalog.uri", URI));

        assertEquals(IcebergCatalogType.JDBC_CATALOG, icebergJdbcCatalog.getIcebergCatalogType());
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
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
        List<String> dbs = icebergJdbcCatalog.listAllDatabases();
        assertEquals(Arrays.asList("db1", "db2"), dbs);
    }

    @Test
    public void testNormalCreateAndDropDBTable(@Mocked JdbcCatalog jdbcCatalog)
            throws MetaNotFoundException {
        new Expectations() {
            {
                jdbcCatalog.loadNamespaceMetadata(Namespace.of("db"));
                result = ImmutableMap.of(LOCATION_PROPERTY, LOCATION);
                minTimes = 0;

                jdbcCatalog.dropNamespace(Namespace.of("db"));
                result = true;
                minTimes = 0;

                jdbcCatalog.dropTable(TableIdentifier.of("db", "table"));
                result = true;
                minTimes = 0;
            }
        };
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
        icebergJdbcCatalog.createDB("db", new HashMap<>());
        Database db = icebergJdbcCatalog.getDB("db");
        assertEquals("db", db.getFullName());
        icebergJdbcCatalog.dropTable("db", "table", true);
        icebergJdbcCatalog.dropDB("db");
    }

    @Test
    public void testCreateDB(@Mocked JdbcCatalog jdbcCatalog, @Mocked FileSystem fileSystem)
            throws MetaNotFoundException {
        new Expectations() {
            {
                jdbcCatalog.loadNamespaceMetadata(Namespace.of("db"));
                result = ImmutableMap.of(LOCATION_PROPERTY, LOCATION);
                minTimes = 0;

                try {
                    fileSystem.exists((Path) any);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                result = true;
                minTimes = 0;
            }
        };
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
        icebergJdbcCatalog.createDB("db", ImmutableMap.of(LOCATION_PROPERTY, LOCATION));
    }

    @Test
    public void testCreateDBWithException(@Mocked JdbcCatalog jdbcCatalog)
            throws MetaNotFoundException {
        new Expectations() {
            {
                jdbcCatalog.loadNamespaceMetadata(Namespace.of("db"));
                result = ImmutableMap.of(LOCATION_PROPERTY, LOCATION);
                minTimes = 0;
            }
        };
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
        StarRocksConnectorException e =
                assertThrows(StarRocksConnectorException.class,
                        () -> icebergJdbcCatalog.createDB("db", ImmutableMap.of(LOCATION_PROPERTY, LOCATION)));
        String msg = String.format("Invalid location URI: %s. msg: No FileSystem for scheme \"s3\"", LOCATION);
        assertEquals(msg, e.getMessage());
    }

    @Test
    public void testListTableNames(@Mocked JdbcCatalog jdbcCatalog) {
        String db1 = "db1";
        String tbl1 = "tbl1";
        String tbl2 = "tbl2";

        new Expectations() {
            {
                jdbcCatalog.listTables(Namespace.of(db1));
                result = Lists.newArrayList(TableIdentifier.of(tbl1), TableIdentifier.of(tbl2));
                minTimes = 0;
            }
        };
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
        List<String> expectResult = Lists.newArrayList("tbl1", "tbl2");
        Assert.assertEquals(expectResult, icebergJdbcCatalog.listTables(db1));
    }

    @Test
    public void testGetTable(@Mocked JdbcCatalog jdbcCatalog,
                             @Mocked BaseMetastoreTableOperations jdbcTableOperations) {
        new Expectations() {
            {
                jdbcCatalog.loadTable(TableIdentifier.of("db", "tbl1"));
                result = new BaseTable(jdbcTableOperations, "tbl1");
            }
        };
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
        Table table = icebergJdbcCatalog.getTable("db", "tbl1");
        assertEquals("tbl1", table.name());
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
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
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
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
        icebergJdbcCatalog.renameTable("db", "tb1", "tb2");
        boolean exists = icebergJdbcCatalog.tableExists("db", "tbl2");
        Assert.assertTrue(exists);

        boolean createTableFlag = icebergJdbcCatalog.createTable("db", "tb2", null, null, LOCATION, new HashMap<>());
        assertEquals(true, createTableFlag);
    }

    @Test
    public void testCreateTable(@Mocked JdbcCatalog jdbcCatalog) {
        new Expectations() {
            {
            }
        };
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
        boolean createTableFlag = icebergJdbcCatalog.createTable("db", "tb2", null, null, LOCATION, new HashMap<>());
        assertEquals(true, createTableFlag);
    }

    @Test
    public void testDeleteUncommittedDataFiles(@Mocked JdbcCatalog jdbcCatalog, @Mocked FileSystem fileSystem) {
        new Expectations() {
            {
                try {
                    fileSystem.delete((Path) any, false);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                result = true;
                minTimes = 0;
            }
        };
        IcebergJdbcCatalog icebergJdbcCatalog = new IcebergJdbcCatalog("catalog",
                new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", LOCATION,
                "iceberg.catalog.uri", URI));
        icebergJdbcCatalog.deleteUncommittedDataFiles(
                Arrays.asList("/tmp/iceberg/db/table/part-00000", "/tmp/iceberg/db/table/part-00001"));
    }

}
