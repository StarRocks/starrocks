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

package com.starrocks.connector.lance;

import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.Table;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.qe.ConnectContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class LanceMetadataTest {
    @Test
    public void testDirectoryCatalogListing() throws Exception {
        Path warehouse = Files.createTempDirectory("lance_warehouse");
        Files.createDirectories(warehouse.resolve("root_table.lance"));
        Files.createDirectories(warehouse.resolve("default").resolve("default_table.lance"));
        Files.createDirectories(warehouse.resolve("db1").resolve("tbl1.lance"));
        Files.createDirectories(warehouse.resolve("db1").resolve("not_lance"));

        Map<String, String> properties = ImmutableMap.of(
                LanceConnector.LANCE_CATALOG_WAREHOUSE, warehouse.toUri().toString());
        LanceMetadata metadata = new LanceMetadata("lance_catalog", properties, new HdfsEnvironment());
        ConnectContext context = new ConnectContext();

        Assertions.assertEquals(Table.TableType.LANCE, metadata.getTableType());
        Assertions.assertTrue(metadata.dbExists(context, "db1"));
        Assertions.assertFalse(metadata.dbExists(context, ""));
        Assertions.assertIterableEquals(List.of("default", "db1"), metadata.listDbNames(context));
        Assertions.assertIterableEquals(List.of("root_table", "default_table"),
                metadata.listTableNames(context, LanceConnector.DEFAULT_DB));
        Assertions.assertIterableEquals(List.of("tbl1"), metadata.listTableNames(context, "db1"));
    }
}
