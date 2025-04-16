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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/planner/HashDistributionPrunerTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Type;
import com.starrocks.sql.plan.PlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HashDistributionPrunerTest extends PlanTestBase {

    @Test
    public void test() {
        List<Long> tabletIds = Lists.newArrayListWithExpectedSize(300);
        for (long i = 0; i < 300; i++) {
            tabletIds.add(i);
        }

        // distribution columns
        Column dealDate = new Column("dealDate", Type.DATE, false);
        Column mainBrandId = new Column("main_brand_id", Type.CHAR, false);
        Column itemThirdCateId = new Column("item_third_cate_id", Type.CHAR, false);
        Column channel = new Column("channel", Type.CHAR, false);
        Column shopType = new Column("shop_type", Type.CHAR, false);
        List<Column> columns = Lists.newArrayList(dealDate, mainBrandId, itemThirdCateId, channel, shopType);

        // filters
        PartitionColumnFilter dealDatefilter = new PartitionColumnFilter();
        dealDatefilter.setLowerBound(new StringLiteral("2019-08-22"), true);
        dealDatefilter.setUpperBound(new StringLiteral("2019-08-22"), true);

        PartitionColumnFilter mainBrandFilter = new PartitionColumnFilter();
        List<Expr> inList = Lists.newArrayList();
        inList.add(new StringLiteral("1323"));
        inList.add(new StringLiteral("2528"));
        inList.add(new StringLiteral("9610"));
        inList.add(new StringLiteral("3893"));
        inList.add(new StringLiteral("6121"));
        mainBrandFilter.setInPredicate(new InPredicate(new SlotRef(null, "main_brand_id"), inList, false));

        PartitionColumnFilter itemThirdFilter = new PartitionColumnFilter();
        List<Expr> inList2 = Lists.newArrayList();
        inList2.add(new StringLiteral("9719"));
        inList2.add(new StringLiteral("11163"));
        itemThirdFilter.setInPredicate(new InPredicate(new SlotRef(null, "item_third_cate_id"), inList2, false));

        PartitionColumnFilter channelFilter = new PartitionColumnFilter();
        List<Expr> inList3 = Lists.newArrayList();
        inList3.add(new StringLiteral("1"));
        inList3.add(new StringLiteral("3"));
        channelFilter.setInPredicate(new InPredicate(new SlotRef(null, "channel"), inList3, false));

        PartitionColumnFilter shopTypeFilter = new PartitionColumnFilter();
        List<Expr> inList4 = Lists.newArrayList();
        inList4.add(new StringLiteral("2"));
        shopTypeFilter.setInPredicate(new InPredicate(new SlotRef(null, "shop_type"), inList4, false));

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("dealDate", dealDatefilter);
        filters.put("main_brand_id", mainBrandFilter);
        filters.put("item_third_cate_id", itemThirdFilter);
        filters.put("channel", channelFilter);
        filters.put("shop_type", shopTypeFilter);

        HashDistributionPruner pruner = new HashDistributionPruner(tabletIds, columns, filters, tabletIds.size());

        Collection<Long> results = pruner.prune();
        // 20 = 1 * 5 * 2 * 2 * 1 (element num of each filter)
        Assert.assertEquals(20, results.size());

        filters.get("shop_type").getInPredicate().addChild(new StringLiteral("4"));
        filters.get("shop_type").setInPredicate(filters.get("shop_type").getInPredicate());
        results = pruner.prune();
        // 40 = 1 * 5 * 2 * 2 * 2 (element num of each filter)
        // 39 is because these is hash conflict
        Assert.assertEquals(39, results.size());

        filters.get("shop_type").getInPredicate().addChild(new StringLiteral("5"));
        filters.get("shop_type").getInPredicate().addChild(new StringLiteral("6"));
        filters.get("shop_type").getInPredicate().addChild(new StringLiteral("7"));
        filters.get("shop_type").getInPredicate().addChild(new StringLiteral("8"));
        filters.get("shop_type").setInPredicate(filters.get("shop_type").getInPredicate());
        results = pruner.prune();
        // 120 = 1 * 5 * 2 * 2 * 6 (element num of each filter) > 100
        Assert.assertEquals(300, results.size());

        // check hash conflict
        inList4.add(new StringLiteral("4"));
        HashDistributionKey hashKey = new HashDistributionKey();
        Set<Long> tablets = Sets.newHashSet();
        hashKey.pushColumn(new StringLiteral("2019-08-22"), Type.DATE);
        for (Expr inLiteral : inList) {
            hashKey.pushColumn((StringLiteral) inLiteral, Type.CHAR);
            for (Expr inLiteral2 : inList2) {
                hashKey.pushColumn((StringLiteral) inLiteral2, Type.CHAR);
                for (Expr inLiteral3 : inList3) {
                    hashKey.pushColumn((StringLiteral) inLiteral3, Type.CHAR);
                    for (Expr inLiteral4 : inList4) {
                        hashKey.pushColumn((StringLiteral) inLiteral4, Type.CHAR);
                        long hashValue = hashKey.getHashValue();
                        tablets.add(tabletIds.get((int) ((hashValue & 0xffffffff) % tabletIds.size())));
                        hashKey.popColumn();
                    }
                    hashKey.popColumn();
                }
                hashKey.popColumn();
            }
            hashKey.popColumn();
        }

        Assert.assertEquals(39, tablets.size());
    }

    @Test
    public void test2() {
        List<Long> tabletIds = Lists.newArrayListWithExpectedSize(300);
        for (long i = 0; i < 300; i++) {
            tabletIds.add(i);
        }

        // distribution columns
        Column dealDate = new Column("dealDate", Type.DATE, false);
        Column mainBrandId = new Column("main_brand_id", Type.CHAR, false);
        Column itemThirdCateId = new Column("item_third_cate_id", Type.CHAR, false);
        Column channel = new Column("channel", Type.CHAR, false);
        Column shopType = new Column("shop_type", Type.CHAR, false);
        List<Column> columns = Lists.newArrayList(dealDate, mainBrandId, itemThirdCateId, channel, shopType);

        // filters
        PartitionColumnFilter mainBrandFilter = new PartitionColumnFilter();
        List<Expr> inList = Lists.newArrayList();
        inList.add(new StringLiteral("1323"));
        inList.add(new StringLiteral("2528"));
        inList.add(new StringLiteral("9610"));
        inList.add(new StringLiteral("3893"));
        inList.add(new StringLiteral("6121"));
        mainBrandFilter.setInPredicate(
                new InPredicate(new FunctionCallExpr("abs", Lists.newArrayList(new SlotRef(null, "main_brand_id"))),
                        inList, false));

        Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
        filters.put("main_brand_id", mainBrandFilter);

        HashDistributionPruner pruner = new HashDistributionPruner(tabletIds, columns, filters, tabletIds.size());

        Collection<Long> results = pruner.prune();
        Assert.assertEquals(tabletIds.size(), results.size());
    }

    @Test
    public void testPruneTablet() throws Exception {
        new MockUp<Partition>() {
            @Mock
            public boolean hasData() {
                return true;
            }
        };

        starRocksAssert.withTable("CREATE TABLE `test_bucket_prune` (\n" +
                "  `dim_dt` date NOT NULL COMMENT \"\",\n" +
                "  `dim_class_id` int(11) NOT NULL COMMENT \"\",\n" +
                "  `dim_week_start_time` bigint(20) NOT NULL COMMENT \"\",\n" +
                "  `deleted` int(11) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP \n" +
                "PRIMARY KEY(`dim_dt`, `dim_class_id`, `dim_week_start_time`)\n" +
                "PARTITION BY RANGE(`dim_dt`)\n" +
                "(PARTITION p20241227 VALUES [(\"2024-12-27\"), (\"2024-12-28\")),\n" +
                "PARTITION p20241228 VALUES [(\"2024-12-28\"), (\"2024-12-29\")),\n" +
                "PARTITION p20241229 VALUES [(\"2024-12-29\"), (\"2024-12-30\")),\n" +
                "PARTITION p20241230 VALUES [(\"2024-12-30\"), (\"2024-12-31\")),\n" +
                "PARTITION p20241231 VALUES [(\"2024-12-31\"), (\"2025-01-01\")),\n" +
                "PARTITION p20250101 VALUES [(\"2025-01-01\"), (\"2025-01-02\")),\n" +
                "PARTITION p20250102 VALUES [(\"2025-01-02\"), (\"2025-01-03\")),\n" +
                "PARTITION p20250103 VALUES [(\"2025-01-03\"), (\"2025-01-04\")))\n" +
                "DISTRIBUTED BY HASH(`dim_dt`, `dim_class_id`) BUCKETS 8 \n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.query("select * from test_bucket_prune where dim_dt = '2024-12-29' and dim_class_id = 1")
                .explainContains("tabletRatio=1/8");
    }
}
