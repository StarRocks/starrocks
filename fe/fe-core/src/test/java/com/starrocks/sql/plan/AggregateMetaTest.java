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

package com.starrocks.sql.plan;

import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TabletStatMgr;
import com.starrocks.sql.optimizer.base.ColumnIdentifier;
import com.starrocks.sql.optimizer.statistics.ColumnMinMaxMgr;
import com.starrocks.sql.optimizer.statistics.IMinMaxStatsMgr;
import com.starrocks.sql.optimizer.statistics.StatsVersion;
import com.starrocks.statistic.StatisticUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Optional;

public class AggregateMetaTest extends PlanTestBase {

    @Test
    public void testAggregateMinMaxMeta() throws Exception {
        new MockUp<ColumnMinMaxMgr>() {
            @Mock
            public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
                return Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "200"));
            }
        };

        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        String sql = "SELECT MAX(v2), MIN(v3) FROM t0";
        String plan = getFragmentPlan(sql);

        assertContains(plan, "  1:Project\n"
                + "  |  <slot 4> : 200\n"
                + "  |  <slot 5> : 1\n"
                + "  |  \n"
                + "  0:UNION\n"
                + "     constant exprs: \n"
                + "         1");
        new MockUp<ColumnMinMaxMgr>() {
            @Mock
            public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
                if (identifier.getColumnName().getId().equals("v3")) {
                    return Optional.of(new IMinMaxStatsMgr.ColumnMinMax("1", "200"));
                } else {
                    return Optional.empty();
                }
            }
        };
        plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 4> : 4: max\n" +
                "  |  <slot 5> : 1\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: max(2: v2)\n" +
                "  |  group by:");
    }

    @Test
    public void testAggregateMinMaxMetaDatetimeTypeFallback() throws Exception {
        new MockUp<StatisticUtils>() {
            @Mock
            public LocalDateTime getTableLastUpdateTime(Table table) {
                return LocalDateTime.now();
            }
        };
        new MockUp<ColumnMinMaxMgr>() {
            @Mock
            public Optional<IMinMaxStatsMgr.ColumnMinMax> getStats(ColumnIdentifier identifier, StatsVersion version) {
                if ("t1b".equals(identifier.getColumnName().getId())) {
                    return Optional.of(new IMinMaxStatsMgr.ColumnMinMax("0", "100"));
                }
                return Optional.empty();
            }
        };

        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        connectContext.getSessionVariable().setScanOlapPartitionNumLimit(0);
        String sql = "SELECT min(id_datetime), max(t1b) FROM test_all_type_not_null";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  2:Project\n" +
                "  |  <slot 11> : 11: min\n" +
                "  |  <slot 12> : 100\n" +
                "  |  \n" +
                "  1:AGGREGATE (update finalize)\n" +
                "  |  output: min(8: id_datetime)\n" +
                "  |  group by: \n" +
                "  |  \n" +
                "  0:OlapScanNode");
    }

    @Test
    public void testAggregateCountMeta() throws Exception {
        new MockUp<MaterializedIndex>() {
            @Mock
            public long getRowCount() {
                return 3;
            }
        };
        new MockUp<TabletStatMgr>() {
            @Mock
            public boolean workTimeIsMustAfter(LocalDateTime time) {
                return true;
            }
        };
        connectContext.getSessionVariable().setEnableRewriteSimpleAggToMetaScan(true);
        String sql = "SELECT COUNT() FROM t0";
        String plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 4> : 3\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         1");
        sql = "SELECT cast(9 as INT), cast(226237 as BIGINT), cast(COUNT(1) as BIGINT), cast(COUNT(1)\n" +
                "* 1024 as BIGINT), hex(hll_serialize(hll_empty())), cast(0 as BIGINT), '', '' , cast(-1 as BIGINT) FROM " +
                "(select `v1` as column_key from `t0` partition `t0`) tt;";
        plan = getFragmentPlan(sql);
        assertContains(plan, "  1:Project\n" +
                "  |  <slot 4> : 3\n" +
                "  |  <slot 5> : 9\n" +
                "  |  <slot 6> : 226237\n" +
                "  |  <slot 7> : 3072\n" +
                "  |  <slot 8> : hex(hll_serialize(hll_empty()))\n" +
                "  |  <slot 9> : 0\n" +
                "  |  <slot 11> : ''\n" +
                "  |  <slot 12> : -1\n" +
                "  |  \n" +
                "  0:UNION\n" +
                "     constant exprs: \n" +
                "         1");
        starRocksAssert.withTable("CREATE TABLE `t_bug` (\n" +
                "  `k1` varchar(65533) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "\"compression\" = \"LZ4\",\n" +
                "\"fast_schema_evolution\" = \"true\",\n" +
                "\"replicated_storage\" = \"true\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        sql = "SELECT COUNT(), hex(1) FROM t_bug";
        String thriftPlan = getThriftPlan(sql);
        assertContains(thriftPlan, "union_node:TUnionNode(tuple_id:0,");
        String descTbl = getDescTbl(sql);
        assertContains(descTbl, "TSlotDescriptor(id:4, parent:0, " +
                "slotType:TTypeDesc(types:[TTypeNode(type:SCALAR, scalar_type:TScalarType(type:BIGINT))])");
    }
}
