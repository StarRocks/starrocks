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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TStorageMedium;
import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeFail;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.analyzeSuccess;

public class AnalyzeUpdateTest {
    @BeforeClass
    public static void beforeClass() throws Exception {
        AnalyzeTestUtil.init();
    }

    @Test
    public void testSingle() {
        analyzeFail("update tjson set v_json = '' where v_int = 1",
                "does not support update");

        analyzeFail("update tprimary set pk = 2 where pk = 1",
                "primary key column cannot be updated:");

        analyzeFail("update tprimary set v1 = 'aaa', v2 = 100",
                "must specify where clause to prevent full table update");

        analyzeSuccess("update tprimary set v1 = 'aaa'");
        analyzeSuccess("update tprimary set v1 = 'aaa' where pk = 1");
        analyzeSuccess("update tprimary set v2 = v2 + 1 where pk = 1");
        analyzeSuccess("update tprimary set v1 = 'aaa', v2 = v2 + 1 where pk = 1");
        analyzeSuccess("update tprimary set v2 = v2 + 1 where v1 = 'aaa'");

        analyzeSuccess("update tprimary set v3 = [231,4321,42] where pk = 1");
    }

    @Test
    public void testMulti() {
        analyzeSuccess("update tprimary set v2 = tp2.v2 from tprimary2 tp2 where tprimary.pk = tp2.pk");

        analyzeSuccess(
                "update tprimary set v2 = tp2.v2 from tprimary2 tp2 join t0 where tprimary.pk = tp2.pk " +
                        "and tp2.pk = t0.v1 and t0.v2 > 0");
    }

    @Test
    public void testCTE() {
        analyzeSuccess(
                "with tp2cte as (select * from tprimary2 where v2 < 10) update tprimary set v2 = tp2cte.v2 " +
                        "from tp2cte where tprimary.pk = tp2cte.pk");
    }

    @Test
    public void testSelectBeSchemaTable() {
        analyzeSuccess("select * from information_schema.be_tablets");
    }

    @Test
    public void testColumnWithRowUpdate() {
        analyzeFail("update tmcwr set name = 22",
                "column with row table must specify where clause for update");
    }

    @Test
    public void testLake() {
        new MockUp<GlobalStateMgr>() {

        };

        new MockUp<MetaUtils>() {
            @Mock
            public Table getTable(ConnectContext session, TableName tableName) {
                long dbId = 1L;
                long tableId = 2L;
                long partitionId = 3L;
                long indexId = 4L;
                long tabletId = 10L;
                // Schema
                List<Column> columns = Lists.newArrayList();
                Column k1 = new Column("k1", Type.INT, true, null, "0", "");
                columns.add(k1);
                columns.add(new Column("k2", Type.BIGINT, true, null, "0", ""));
                columns.add(new Column("v2", Type.BIGINT, false, AggregateType.SUM, "0", ""));

                // Tablet
                Tablet tablet = new LakeTablet(tabletId);

                // Index
                MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD, true);
                index.addTablet(tablet, tabletMeta);

                // Partition
                DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
                PartitionInfo partitionInfo = new SinglePartitionInfo();
                partitionInfo.setReplicationNum(partitionId, (short) 3);

                // Lake table
                LakeTable table = new LakeTable(tableId, "t1", columns, KeysType.PRIMARY_KEYS, partitionInfo, distributionInfo);
                return table;
            }
        };

        String sql = "update t1 set v2 = v2 + 1";
        analyzeFail(sql, "must specify where clause to prevent full table update");
    }
}
