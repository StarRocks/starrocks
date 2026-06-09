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

package com.starrocks.sql.automv.lattice;

import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.automv.pn.TimeGranule;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class AutoMVPartitionPolicyIcebergTest extends MVTestBase {
    @BeforeClass
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    @Test
    public void testPreferRange() {
        String q = "SELECT id, sum(data) FROM `iceberg0`.`partitioned_db`.`t1` group by id";
        Object[][] testCases = new Object[][] {
                {q, TimeGranule.Unit.DAY, new String[] {
                        "(str2date(`iceberg0`.`partitioned_db`.`t1`.date, \"%Y-%m-%d\")) AS _ca0002",
                        "PARTITION BY _ca0002"
                }}
        };
        testPartitionHelper(testCases, true);
    }

    @Test
    public void testPreferList() {
        String q = "SELECT id, sum(data) FROM `iceberg0`.`partitioned_db`.`t1` group by id";
        Object[][] testCases = new Object[][] {
                {q, TimeGranule.Unit.DAY, new String[] {
                        "PARTITION BY date"
                }}
        };
        testPartitionHelper(testCases, false);
    }

    @Test
    public void testPreferRange1() {
        String q = "SELECT id, sum(data) FROM `iceberg0`.`unpartitioned_db`.`t0` group by id";
        Object[][] testCases = new Object[][] {
                {q, TimeGranule.Unit.DAY, new String[] {
                        "SELECT\n" +
                                "  `iceberg0`.`unpartitioned_db`.`t0`.id\n" +
                                "  ,(sum(`iceberg0`.`unpartitioned_db`.`t0`.data)) AS _ca0002\n" +
                                "FROM"
                }}
        };
        testPartitionHelper(testCases, true);
    }

    @Test
    public void testPreferList1() {
        String q = "SELECT id, sum(data) FROM `iceberg0`.`unpartitioned_db`.`t0` group by id";
        Object[][] testCases = new Object[][] {
                {q, TimeGranule.Unit.DAY, new String[] {
                        "SELECT\n" +
                                "  `iceberg0`.`unpartitioned_db`.`t0`.id\n" +
                                "  ,(sum(`iceberg0`.`unpartitioned_db`.`t0`.data)) AS _ca0002\n" +
                                "FROM"
                }}
        };
        testPartitionHelper(testCases, false);
    }

    @Test
    public void testPreferRange2() {
        String q = "SELECT id, sum(data) FROM `iceberg0`.`partitioned_db`.`t1` where date = '2020-01-01' group by id";
        Object[][] testCases = new Object[][] {
                {q, TimeGranule.Unit.DAY, new String[] {
                        "(str2date(`iceberg0`.`partitioned_db`.`t1`.date, \"%Y-%m-%d\")) AS _ca0002",
                        "PARTITION BY _ca0002",
                }}
        };
        testPartitionHelper(testCases, true);
    }

    @Test
    public void testPreferList2() {
        String q = "SELECT id, sum(data) FROM `iceberg0`.`partitioned_db`.`t1` where date = '2020-01-01' group by id";
        Object[][] testCases = new Object[][] {
                {q, TimeGranule.Unit.DAY, new String[] {
                        "PARTITION BY date"
                }}
        };
        testPartitionHelper(testCases, false);
    }

    private void testPartitionHelper(Object[][] testCases, boolean preferRange) {

        Map<String, Object> vars = AutoMVUtil.saveGlobalVariable();
        try {
            GlobalVariable.setAutoMVPreferRangePartition(preferRange);
            AutoMVUtil.testPartitionHelper(starRocksAssert, testCases);
        } finally {
            AutoMVUtil.restoreGlobalVariable(vars);
        }
    }
}
