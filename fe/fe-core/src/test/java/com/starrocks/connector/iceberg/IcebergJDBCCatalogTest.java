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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.connector.iceberg.jdbc.IcebergJDBCCatalog;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.junit.After;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_CUSTOM_PROPERTIES_PREFIX;
import static org.apache.iceberg.CatalogProperties.URI;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;

public class IcebergJDBCCatalogTest {

    private static final String MOCKED_URI = "jdbc:sqlite:file::memory:?icebergDB";

    private final Configuration configuration = new Configuration();

    private File dir;


    @Test
    public void testCreateTable() throws Exception {
        dir = Files.createTempDirectory(Paths.get("."), "location").toFile();
        Map<String, String> properties = new HashMap<>();
        properties.put(ICEBERG_CUSTOM_PROPERTIES_PREFIX + WAREHOUSE_LOCATION, dir.getAbsolutePath());
        properties.put(ICEBERG_CUSTOM_PROPERTIES_PREFIX + URI, MOCKED_URI);
        IcebergJDBCCatalog jdbcCatalog = new IcebergJDBCCatalog("icebergDB", configuration, properties);

        // get type
        Assertions.assertEquals(jdbcCatalog.getIcebergCatalogType(), IcebergCatalogType.JDBC_CATALOG);

        // create db
        jdbcCatalog.createDb("db1", null);
        jdbcCatalog.createDb("db2", null);
        jdbcCatalog.createDb("db3", ImmutableMap.of("location", dir.getAbsolutePath() + "/db3"));
        List<String> dbs = jdbcCatalog.listAllDatabases();
        Assertions.assertEquals(ImmutableList.of("db1", "db2", "db3"), dbs);

        // create table
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("c1", Type.BOOLEAN));
        columns.add(new Column("c2", Type.INT));
        columns.add(new Column("c3", Type.BIGINT));
        Schema schema = IcebergApiConverter.toIcebergApiSchema(columns);
        boolean createSuccess = jdbcCatalog.createTable("db1", "tbl1", schema, null, null, null);
        jdbcCatalog.createTable("db1", "tbl2", schema, null, null, null);
        Assertions.assertTrue(createSuccess);
        Assertions.assertTrue(jdbcCatalog.tableExists("db1", "tbl1"));

        // get table
        Assertions.assertEquals(schema.toString(), jdbcCatalog.getTable("db1", "tbl1").schema().toString());

        // rename table
        jdbcCatalog.renameTable("db1", "tbl1", "copy1");
        Assertions.assertFalse(jdbcCatalog.tableExists("db1", "tbl1"));
        Assertions.assertTrue(jdbcCatalog.tableExists("db1", "copy1"));

        // drop table
        boolean dropSuccess = jdbcCatalog.dropTable("db1", "tbl2", false);
        Assertions.assertTrue(dropSuccess);
        Assertions.assertFalse(jdbcCatalog.tableExists("db1", "tbl2"));

        // drop db
        jdbcCatalog.dropTable("db1", "copy1", false);
        jdbcCatalog.dropDb("db1");
        Assertions.assertThrows(NoSuchNamespaceException.class, () -> jdbcCatalog.getDB("db1"));

        Assertions.assertEquals("JdbcCatalog{}", jdbcCatalog.toString());
    }

    @After
    public void cleanup() throws Exception {
        if (dir != null) {
            FileUtils.deleteDirectory(dir);
        }
    }
}
