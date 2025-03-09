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
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

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
        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(200L, result.get().longValue());
    }

    @Test
    public void testisSupportPCTRefresh() {
        Assert.assertTrue(new OlapPartitionTraits().isSupportPCTRefresh());
        Assert.assertTrue(new HivePartitionTraits().isSupportPCTRefresh());
        Assert.assertTrue(new IcebergPartitionTraits().isSupportPCTRefresh());
        Assert.assertTrue(new PaimonPartitionTraits().isSupportPCTRefresh());
        Assert.assertTrue(new JDBCPartitionTraits().isSupportPCTRefresh());
        Assert.assertFalse(new HudiPartitionTraits().isSupportPCTRefresh());
        Assert.assertFalse(new OdpsPartitionTraits().isSupportPCTRefresh());
        Assert.assertFalse(new KuduPartitionTraits().isSupportPCTRefresh());
        Assert.assertFalse(new DeltaLakePartitionTraits().isSupportPCTRefresh());

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
            Assert.assertEquals(supportedTableTypes.contains(tableType),
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
        Assert.assertEquals(connectorPartitionTraits.getTableName(), "hiveTable");
    }

    @Test
    public void testHudiResourceTableName() {
        HudiTable hudiTable = new HudiTable(0, "name", "hdui_catalog", "hudiDb",
                "hudiTable",  "resource_name", "",
                Lists.newArrayList(), Lists.newArrayList(), Lists.newArrayList(), 0,
                Maps.newHashMap(), HudiTable.HudiTableType.COW);
        ConnectorPartitionTraits connectorPartitionTraits = ConnectorPartitionTraits.build(hudiTable);
        Assert.assertEquals(connectorPartitionTraits.getTableName(), "hudiTable");
    }
}
