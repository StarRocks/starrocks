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

package com.starrocks.sql.automv.generator;

import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PartitionedIcebergTest extends MVTestBase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    @AfterClass
    public static void afterClass() throws Exception {
    }

    @Before
    public void before() {
    }

    @After
    public void after() throws Exception {
    }

    @Test
    public void testSingleColumnTransformPartition() {
        String sql = "SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.`t0_year` as a";
        List<String> mvs = AutoMVUtil.recommendOneOneMV(connectContext, sql);
        Assert.assertEquals(1, mvs.size());
        String mv = mvs.get(0);
        Assert.assertTrue(mv, mv.contains("COMMENT \"11-MV recommended by AutoMV\"\n" +
                "PARTITION BY date_trunc(\"year\", ts)\n" +
                "DISTRIBUTED BY HASH (id, data, ts) BUCKETS 64\n" +
                "ORDER BY (id, data, ts)"));
    }

    @Test
    public void testMultiColumnTransformPartition() {
        String sql = "SELECT id, data, ts  FROM `iceberg0`.`partitioned_transforms_db`.t0_multi_hour as a";
        List<String> mvs = AutoMVUtil.recommendOneOneMV(connectContext, sql);
        Assert.assertEquals(1, mvs.size());
        String mv = mvs.get(0);
        Assert.assertTrue(mv, mv.contains("COMMENT \"11-MV recommended by AutoMV\"\n" +
                "PARTITION BY (id,data,date_trunc(\"hour\", ts))\n" +
                "DISTRIBUTED BY HASH (id, data, ts) BUCKETS 64\n" +
                "ORDER BY (id, data, ts)"));
    }

    @Test
    public void testMultiColumnTransformPartitionMissingColumns() {
        String sql = "SELECT id FROM `iceberg0`.`partitioned_transforms_db`.t0_multi_hour as a";
        List<String> mvs = AutoMVUtil.recommendOneOneMV(connectContext, sql);
        Assert.assertEquals(1, mvs.size());
        String mv = mvs.get(0);
        Assert.assertTrue(mv, mv.contains("COMMENT \"11-MV recommended by AutoMV\"\n" +
                "PARTITION BY (id,data,date_trunc(\"hour\", ts))\n" +
                "DISTRIBUTED BY HASH (id, data, ts) BUCKETS 64\n" +
                "ORDER BY (id, data, ts)"));
    }
}