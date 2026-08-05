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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableMap;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class LanceTableTest {
    @Test
    public void testToThrift() {
        List<Column> schema = Arrays.asList(
                new Column("id", IntegerType.INT, true),
                new Column("name", VarcharType.VARCHAR, true));
        Map<String, String> properties = ImmutableMap.of(LanceTable.DATASET_URI, "s3://bucket/db/tbl.lance");
        LanceTable table = new LanceTable("lance_catalog", "db1", "tbl1", schema, properties);

        TTableDescriptor descriptor = table.toThrift(List.of());

        Assertions.assertEquals(TTableType.LANCE_TABLE, descriptor.getTableType());
        Assertions.assertEquals("db1", descriptor.getDbName());
        Assertions.assertEquals("tbl1", descriptor.getTableName());
        Assertions.assertTrue(descriptor.isSetLanceTable());
        Assertions.assertEquals("s3://bucket/db/tbl.lance", descriptor.getLanceTable().getDataset_uri());
        Assertions.assertTrue(descriptor.isSetHdfsTable());
        Assertions.assertEquals("s3://bucket/db/tbl.lance", descriptor.getHdfsTable().getHdfs_base_dir());
        Assertions.assertEquals(2, descriptor.getHdfsTable().getColumnsSize());
        Assertions.assertFalse(descriptor.getHdfsTable().isSetPartition_columns());
    }

    @Test
    public void testTableIdentity() {
        Map<String, String> properties = ImmutableMap.of(LanceTable.DATASET_URI, "file:///tmp/tbl.lance");
        LanceTable table = new LanceTable("lance_catalog", "default", "tbl", List.of(), properties);
        LanceTable same = new LanceTable("lance_catalog", "default", "tbl", List.of(), properties);
        LanceTable other = new LanceTable("lance_catalog", "default", "other", List.of(), properties);

        Assertions.assertEquals("file:///tmp/tbl.lance", table.getTableLocation());
        Assertions.assertEquals("lance_catalog.default.tbl.file:///tmp/tbl.lance", table.getUUID());
        Assertions.assertTrue(table.isUnPartitioned());
        Assertions.assertTrue(table.getPartitionColumns().isEmpty());
        Assertions.assertEquals(table, same);
        Assertions.assertNotEquals(table, other);
    }
}
