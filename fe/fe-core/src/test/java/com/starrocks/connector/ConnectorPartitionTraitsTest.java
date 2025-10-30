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

package com.starrocks.connector;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.connector.paimon.Partition;
import com.starrocks.connector.partitiontraits.DefaultTraits;
import com.starrocks.connector.partitiontraits.DeltaLakePartitionTraits;
import com.starrocks.connector.partitiontraits.HivePartitionTraits;
import com.starrocks.connector.partitiontraits.HudiPartitionTraits;
import com.starrocks.connector.partitiontraits.IcebergPartitionTraits;
import com.starrocks.connector.partitiontraits.JDBCPartitionTraits;
import com.starrocks.connector.partitiontraits.KuduPartitionTraits;
import com.starrocks.connector.partitiontraits.OdpsPartitionTraits;
import com.starrocks.connector.partitiontraits.OlapPartitionTraits;
import com.starrocks.connector.partitiontraits.PaimonPartitionTraits;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ConnectorPartitionTraitsTest {

    @Test
    public void testMaxPartitionRefreshTs() {

        Map<String, PartitionInfo> fakePartitionInfo = new HashMap<>();
        Partition p1 = new Partition("p1", 100, null, null, null);
        Partition p2 = new Partition("p2", 200, null, null, null);
        fakePartitionInfo.put("p1", p1);
        fakePartitionInfo.put("p2", p2);
        new MockUp<DefaultTraits>() {
            @Mock
            public Map<String, PartitionInfo> getPartitionNameWithPartitionInfo() {
                return fakePartitionInfo;
            }
        };

        Optional<Long> result = new PaimonPartitionTraits().maxPartitionRefreshTs();
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(200L, result.get().longValue());
    }

    @Test
    public void testisSupportPCTRefresh() {
        Assertions.assertTrue(new OlapPartitionTraits().isSupportPCTRefresh());
        Assertions.assertTrue(new HivePartitionTraits().isSupportPCTRefresh());
        Assertions.assertTrue(new IcebergPartitionTraits().isSupportPCTRefresh());
        Assertions.assertTrue(new PaimonPartitionTraits().isSupportPCTRefresh());
        Assertions.assertTrue(new JDBCPartitionTraits().isSupportPCTRefresh());
        Assertions.assertFalse(new HudiPartitionTraits().isSupportPCTRefresh());
        Assertions.assertFalse(new OdpsPartitionTraits().isSupportPCTRefresh());
        Assertions.assertFalse(new KuduPartitionTraits().isSupportPCTRefresh());
        Assertions.assertFalse(new DeltaLakePartitionTraits().isSupportPCTRefresh());

        final Set<Table.TableType> supportedTableTypes = ImmutableSet.of(
                Table.TableType.OLAP,
                Table.TableType.MATERIALIZED_VIEW,
                Table.TableType.CLOUD_NATIVE,
                Table.TableType.CLOUD_NATIVE_MATERIALIZED_VIEW,
                Table.TableType.HIVE,
                Table.TableType.ICEBERG,
                Table.TableType.PAIMON,
                Table.TableType.JDBC
        );
        for (Table.TableType tableType : Table.TableType.values()) {
            Assertions.assertEquals(supportedTableTypes.contains(tableType),
                    ConnectorPartitionTraits.isSupportPCTRefresh(tableType));
        }
    }

    @Test
    public void testHiveResourceTableName() {
        HiveTable hiveTable = new HiveTable(0, "name", Lists.newArrayList(), "resource_name", "hive_catalog",
                "hiveDb", "hiveTable", "location", "", 0,
                Lists.newArrayList(), Lists.newArrayList(), Maps.newHashMap(), Maps.newHashMap(), null,
                HiveTable.HiveTableType.MANAGED_TABLE);
        ConnectorPartitionTraits connectorPartitionTraits = ConnectorPartitionTraits.build(hiveTable);
        Assertions.assertEquals(connectorPartitionTraits.getTableName(), "hiveTable");
    }

    @Test
    public void testHudiResourceTableName() {
        HudiTable hudiTable = new HudiTable(0, "name", "hdui_catalog", "hudiDb",
                "hudiTable",  "resource_name", "",
                Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList(), 0,
                Maps.newHashMap(), HudiTable.HudiTableType.COW);
        ConnectorPartitionTraits connectorPartitionTraits = ConnectorPartitionTraits.build(hudiTable);
        Assertions.assertEquals(connectorPartitionTraits.getTableName(), "hudiTable");
    }

    @Test
    public void testIcebergTable(@Mocked org.apache.iceberg.Table nativeTable) {
        IcebergTable icebergTable = new IcebergTable(0, "name", "iceberg_catalog", "resource_name", "icebergDb",
                "icebergTable", "",
                Lists.newArrayList(), nativeTable,
                Maps.newHashMap());
        ConnectorPartitionTraits connectorPartitionTraits = ConnectorPartitionTraits.build(icebergTable);
        Assertions.assertEquals(connectorPartitionTraits.getTableName(), "icebergTable");
        try {
            PartitionKey key = connectorPartitionTraits.createPartitionKeyWithType(Lists.newArrayList("123.3"), 
                    Lists.newArrayList(ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6)));
            Assertions.assertEquals(key.getKeys().get(0).getType(), 
                    ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 6));
        } catch (Exception e) {
            throw new RuntimeException("createPartitionKeyWithType failed", e);
        }
    }
    
}
