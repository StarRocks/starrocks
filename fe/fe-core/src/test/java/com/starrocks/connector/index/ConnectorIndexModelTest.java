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

package com.starrocks.connector.index;

import com.starrocks.catalog.Table;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectorIndexModelTest {
    @Test
    public void testInclusiveShardValueObject() {
        ConnectorIndexShard shard = new ConnectorIndexShard(10L, 20L);
        Assertions.assertEquals(10L, shard.getFrom());
        Assertions.assertEquals(20L, shard.getTo());
        Assertions.assertEquals(new ConnectorIndexShard(10L, 20L), shard);
        Assertions.assertEquals(new ConnectorIndexShard(10L, 20L).hashCode(), shard.hashCode());
        Assertions.assertNotEquals(new ConnectorIndexShard(10L, 21L), shard);
        Assertions.assertEquals("[10, 20]", shard.toString());
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConnectorIndexShard(-1L, 0L));
        Assertions.assertThrows(IllegalArgumentException.class, () -> new ConnectorIndexShard(2L, 1L));
    }

    @Test
    public void testIndexMetadataIsImmutable() {
        ConnectorIndexMetadata empty = ConnectorIndexMetadata.of(Collections.emptyMap());
        Assertions.assertSame(ConnectorIndexMetadata.empty(), empty);
        Assertions.assertTrue(empty.isEmpty());

        ConnectorIndexMetadata metadata = ConnectorIndexMetadata.of(
                Map.of("id", Set.of(ConnectorIndexType.RANGE, ConnectorIndexType.BITMAP)));
        Assertions.assertFalse(metadata.isEmpty());
        Assertions.assertTrue(metadata.supports("id", ConnectorIndexType.RANGE));
        Assertions.assertFalse(metadata.supports("missing", ConnectorIndexType.RANGE));
        Assertions.assertEquals(Set.of(ConnectorIndexType.RANGE, ConnectorIndexType.BITMAP),
                metadata.getIndexTypes("id"));
        Assertions.assertEquals(Map.of("id", Set.of(ConnectorIndexType.RANGE, ConnectorIndexType.BITMAP)),
                metadata.getColumnIndexes());
        Assertions.assertTrue(metadata.toString().contains("id"));
    }

    @Test
    public void testVirtualIndexTableDelegatesConnectorIdentity(@Mocked Table innerTable) {
        TTableDescriptor descriptor = new TTableDescriptor();
        new Expectations() {
            {
                innerTable.getId();
                result = 101L;
                innerTable.getName();
                result = "orders";
                innerTable.getCatalogName();
                result = "paimon_catalog";
                innerTable.toThrift((List) any);
                result = descriptor;
            }
        };

        IndexTable table = new IndexTable(innerTable);
        Assertions.assertSame(innerTable, table.getInnerTable());
        Assertions.assertTrue(table.isSupported());
        Assertions.assertEquals("orders$global_index", table.getName());
        Assertions.assertEquals("paimon_catalog", table.getCatalogName());
        Assertions.assertEquals(2, table.getBaseSchema().size());
        Assertions.assertSame(table.getBaseSchema(), table.getFullSchema());
        Assertions.assertEquals(VarbinaryType.VARBINARY,
                table.getBaseSchema().get(0).getType());
        Assertions.assertEquals(IndexTable.INDEX_RESULT_COLUMN_NAME, table.getBaseSchema().get(0).getName());
        Assertions.assertEquals(VarcharType.VARCHAR, table.getBaseSchema().get(1).getType());
        Assertions.assertEquals(IndexTable.ARGS_COLUMN_NAME, table.getBaseSchema().get(1).getName());
        Assertions.assertEquals(2, table.getIdToColumn().size());

        Assertions.assertSame(descriptor, table.toThrift(Collections.emptyList()));
        Assertions.assertEquals(101L, descriptor.getId());
        Assertions.assertEquals(2, descriptor.getNumCols());
        Assertions.assertEquals("orders$global_index", descriptor.getTableName());
        Assertions.assertThrows(NullPointerException.class, () -> new IndexTable(null));
    }
}
