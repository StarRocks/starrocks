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

package com.starrocks.planner;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.AggregateType;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TCacheParam;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class GeneratedPartitionQueryCachePlanTest extends PlanTestBase {
    private static final String DATABASE = "generated_partition_query_cache";

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        starRocksAssert.withDatabase(DATABASE);
        createTable("only_internal_replace", false);
        createTable("user_replace", true);
    }

    private static void createTable(String name, boolean userReplace) throws Exception {
        String qualifiedName = DATABASE + "." + name;
        starRocksAssert.withTable("CREATE TABLE " + qualifiedName + " ("
                + "dt DATETIME NOT NULL,city VARCHAR(16) NOT NULL,pv BIGINT SUM"
                + (userReplace ? ",tag VARCHAR(16) REPLACE" : "") + ") "
                + "AGGREGATE KEY(dt,city) PARTITION BY date_trunc('week',dt) "
                + "DISTRIBUTED BY HASH(city) BUCKETS 1 PROPERTIES('replication_num'='1')");
        // An automatic expression-partitioned table initially has only a shadow partition. Populate
        // real partition metadata so the planner cannot replace this scan with an empty relation.
        starRocksAssert.alterTable("ALTER TABLE " + qualifiedName
                + " ADD PARTITION p_week VALUES IN ('2026-07-13 00:00:00')");
        OlapTable table = (OlapTable) starRocksAssert.getTable(DATABASE, name);
        PhysicalPartition partition = table.getPartition("p_week").getDefaultPhysicalPartition();
        partition.updateVisibleVersion(2);
        partition.setDataVersion(2);
        partition.setNextVersion(3);
        partition.setNextDataVersion(3);
        partition.getLatestBaseIndex().setRowCount(10);
        for (Tablet tablet : partition.getLatestBaseIndex().getTablets()) {
            for (Replica replica : ((LocalTablet) tablet).getImmutableReplicas()) {
                replica.updateVersionInfo(2, -1, 2);
                Assertions.assertEquals(partition.getVisibleVersion(), replica.getVersion());
            }
        }
        Assertions.assertTrue(table.getPartition("p_week").hasData());
        List<Column> routingColumns = table.getFullSchema().stream()
                .filter(Column::isInternalGeneratedPartitionColumn).collect(Collectors.toList());
        Assertions.assertEquals(1, routingColumns.size());
        Assertions.assertEquals(AggregateType.REPLACE, routingColumns.get(0).getAggregationType());
        Assertions.assertFalse(routingColumns.get(0).isKey());
        if (userReplace) {
            Assertions.assertEquals(AggregateType.REPLACE, table.getColumn("tag").getAggregationType());
            Assertions.assertFalse(table.getColumn("tag").isInternalGeneratedPartitionColumn());
        }
    }

    private TCacheParam cacheParam(String table) throws Exception {
        SessionVariable original = connectContext.getSessionVariable();
        SessionVariable variables = original.clone();
        connectContext.setSessionVariable(variables);
        try {
            variables.setEnablePipelineEngine(true);
            variables.setEnableQueryCache(true);
            variables.setEnableRewriteSimpleAggToMetaScan(false);
            variables.setEnableGlobalLateMaterialization(false);
            String sql = "SELECT city,SUM(pv) FROM " + DATABASE + "." + table + " GROUP BY city";
            ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
            TCacheParam parameter = plan.getFragments().stream().map(PlanFragment::getCacheParam)
                    .filter(p -> p != null).findFirst()
                    .orElseThrow(() -> new AssertionError("Expected a cache parameter for " + table));
            Assertions.assertFalse(parameter.getRegion_map().isEmpty());
            return parameter;
        } finally {
            connectContext.setSessionVariable(original);
        }
    }

    @Test
    public void testTrustedGeneratedPartitionColumnEnablesMultiVersionCache() throws Exception {
        Assertions.assertTrue(cacheParam("only_internal_replace").isCan_use_multiversion());
    }

    @Test
    public void testUserReplaceKeepsSingleVersionCache() throws Exception {
        // A real REPLACE value column still permits a single-version cache, but never the merge of a
        // cached snapshot with delta rowsets. Check the actual emitted flag rather than cache absence.
        Assertions.assertFalse(cacheParam("user_replace").isCan_use_multiversion());
    }
}
