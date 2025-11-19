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

package com.starrocks.alter.dynamictablet;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.SplitTabletClause;
import com.starrocks.sql.ast.TabletList;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

public class SplitTabletJobFactoryTest {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;
    private static OlapTable table;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");

        String sql = "create table test_table (key1 int, key2 varchar(10))\n" +
                "distributed by hash(key1) buckets 1\n" +
                "properties('replication_num' = '1'); ";
        starRocksAssert.withTable(sql);
        table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "test_table");
    }

    @Test
    public void testCreateDynamicTabletJob() throws Exception {
        {
            Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_DYNAMIC_TABLET_SPLIT_SIZE, "-1");
            SplitTabletClause clause = new SplitTabletClause(null, null, properties);
            clause.setDynamicTabletSplitSize(-1);

            DynamicTabletJobFactory factory = new SplitTabletJobFactory(db, table, clause);
            Assertions.assertThrows(IllegalStateException.class, () -> factory.createDynamicTabletJob());
        }

        {
            Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_DYNAMIC_TABLET_SPLIT_SIZE, "1");
            SplitTabletClause clause = new SplitTabletClause(null, null, properties);
            clause.setDynamicTabletSplitSize(1);

            DynamicTabletJobFactory factory = new SplitTabletJobFactory(db, table, clause);
            Assertions.assertThrows(StarRocksException.class, () -> factory.createDynamicTabletJob());
        }

        {
            PhysicalPartition physicalPartition = table.getAllPhysicalPartitions().iterator().next();
            MaterializedIndex materializedIndex = physicalPartition.getBaseIndex();
            long tabletId = materializedIndex.getTablets().get(0).getId();
            TabletList tabletList = new TabletList(List.of(tabletId));

            Map<String, String> properties = Map.of(PropertyAnalyzer.PROPERTIES_DYNAMIC_TABLET_SPLIT_SIZE, "-2");
            SplitTabletClause clause = new SplitTabletClause(null, tabletList, properties);
            clause.setDynamicTabletSplitSize(-2);

            DynamicTabletJobFactory factory = new SplitTabletJobFactory(db, table, clause);
            DynamicTabletJob dynamicTabletJob = factory.createDynamicTabletJob();
            Assertions.assertNotNull(dynamicTabletJob);
        }
    }
}
