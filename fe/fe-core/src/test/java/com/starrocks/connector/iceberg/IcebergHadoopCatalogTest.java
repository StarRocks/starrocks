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
import com.starrocks.connector.iceberg.hadoop.IcebergHadoopCatalog;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergHadoopCatalogTest {
    @Test
    public void testNewIcebergHadoopCatalog(@Mocked HadoopCatalog hadoopCatalog) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new IcebergHadoopCatalog("catalog",
                new Configuration(), ImmutableMap.of()));
        assertEquals(e.getMessage(), "Iceberg hadoop catalog must set warehouse location " +
                "(\"iceberg.catalog.warehouse\" = \"s3://path/to/warehouse\").");
        assertDoesNotThrow(() -> new IcebergHadoopCatalog("catalog", new Configuration(),
                ImmutableMap.of("iceberg.catalog.warehouse", "s3://path/to/warehouse")));
    }


    @Test
    public void testListAllDatabases(@Mocked HadoopCatalog hadoopCatalog) {
        new Expectations() {
            {
                hadoopCatalog.listNamespaces();
                result = ImmutableList.of(Namespace.of("db1"), Namespace.of("db2"));
                minTimes = 0;
            }
        };

        IcebergHadoopCatalog icebergHadoopCatalog = new IcebergHadoopCatalog(
                "catalog", new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", "s3://path/to/warehouse"));
        List<String> dbs = icebergHadoopCatalog.listAllDatabases();
        assertEquals(Arrays.asList("db1", "db2"), dbs);
    }

    @Test
    public void testTableExists(@Mocked HadoopCatalog hadoopCatalog) {
        new Expectations() {
            {
                hadoopCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        IcebergHadoopCatalog icebergHadoopCatalog = new IcebergHadoopCatalog(
                "catalog", new Configuration(), ImmutableMap.of("iceberg.catalog.warehouse", "s3://path/to/warehouse"));
        assertTrue(icebergHadoopCatalog.tableExists("db1", "tbl1"));
    }
}
