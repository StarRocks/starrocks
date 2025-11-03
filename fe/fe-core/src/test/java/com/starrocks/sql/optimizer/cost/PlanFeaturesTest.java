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

package com.starrocks.sql.optimizer.cost;

import com.google.common.base.Splitter;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.cost.feature.FeatureExtractor;
import com.starrocks.sql.optimizer.cost.feature.OperatorFeatures;
import com.starrocks.sql.optimizer.cost.feature.PlanFeatures;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

class PlanFeaturesTest extends PlanTestBase {

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "select count(*) from t0 where v1 < 100 limit 100 " +
                    "| tables=[0,0,10003] " +
                    "| 41,1,0,8,0,2,0,3;42,1,0,8,2,2,4,0,0,1,1;46,1,0,9,0,2,0,0,0,1,1",
            "select max(v1) from t0 where v1 < 100 limit 100" +
                    "|tables=[0,0,10003] " +
                    "| 41,1,0,8,0,2,0,3;42,1,0,8,2,2,4,0,0,1,1;46,1,0,8,0,2,0,0,0,1,1",
            "select v1, count(*) from t0 group by v1 " +
                    "| tables=[0,0,10003] " +
                    "| 42,1,0,16,2,2,0,0,1,1,1;46,1,0,8,0,2,0,0,0,0,0",
            "select count(*) from t0 a join t0 b on a.v1 = b.v2" +
                    "| tables=[0,0,10003] " +
                    "| 41,2,0,16,2,4,0,4;42,2,0,16,2,2,0,0,0,2,2;46,2,0,16,0,4,0,0,0,2,0",

            // mysql external table
            "select * from ods_order where org_order_no" +
                    "| tables=[0,0,test.ods_order]" +
                    "| 41,0,0,0,0,0,0,0,42,0,0,0,0,0,0,0,0,0,0,43,0,0,0,0,0,0,0,0,0,45,0,0,0,0,0",
            "select * from (select * from ods_order join mysql_table where k1  = 'a' and order_dt = 'c') t1 where t1.k2 = 'c'" +
                    "| tables=[0,test.ods_order,db1.tbl1] " +
                    "| 41,1,0,8,2,2,0,1,42,0,0,0,0,0,0,0,0,0,0,43,0,0,0,0,0,0,0,0,0,45,",
            "select * from ods_order join mysql_table where k1  = 'a' and order_dt = 'c'" +
                    "| tables=[0,test.ods_order,db1.tbl1] " +
                    "| 41,1,0,8,2,2,0,1,42,0,0,0,0,0,0,0,0,0,0,43,0,0,0,0,0,0,0,0,0,45,",

    })
    public void testBasic(String query, String expectedTables, String expected) throws Exception {
        expectedTables = StringUtils.trim(expectedTables);
        expected = StringUtils.trim(expected);

        ExecPlan execPlan = getExecPlan(query);
        OptExpression physicalPlan = execPlan.getPhysicalPlan();
        PlanFeatures planFeatures = FeatureExtractor.extractFeatures(physicalPlan);

        // feature string
        String string = planFeatures.toFeatureString();
        Assertions.assertTrue(string.startsWith(expectedTables), string);
        Splitter.on(";").splitToList(expected).forEach(slice -> {
            Assertions.assertTrue(string.contains(slice), "slice is " + slice + ", feature is " + string);
        });

        // feature csv
        String csv = planFeatures.toFeatureCsv();
        Splitter.on(";").splitToList(expected).forEach(slice -> {
            Assertions.assertTrue(csv.contains(slice), "slice is " + slice + ", feature is " + string);
        });
    }

    @Test
    public void testHeader() {
        String header = PlanFeatures.featuresHeader();
        List<String> strings = Splitter.on(",").splitToList(header);
        long numTables = strings.stream().filter(x -> x.startsWith("tables")).count();
        long numEnvs = strings.stream().filter(x -> x.startsWith("env")).count();
        long numVars = strings.stream().filter(x -> x.startsWith("var")).count();
        long numOperators = strings.stream().filter(x -> x.startsWith("operators")).count();
        Assertions.assertEquals(3, numTables);
        Assertions.assertEquals(3, numEnvs);
        Assertions.assertEquals(1, numVars);
        Assertions.assertEquals(391, numOperators);
    }

    @Test
    public void testTableFeatures1() {
        OlapTable t0 = (OlapTable) starRocksAssert.getTable("test", "t0");
        Assertions.assertTrue(t0 != null);
        OlapTable t00 = (OlapTable) starRocksAssert.getTable("test", "t0");
        Assertions.assertTrue(t00 != null);
        OlapTable t1 = (OlapTable) starRocksAssert.getTable("test", "t1");

        OperatorFeatures.TableFeature f0 = new OperatorFeatures.TableFeature(t0, Statistics.builder().build());
        Assertions.assertTrue(f0.getTable().equals(t0));
        Assertions.assertTrue(f0.getRowCount() == t0.getRowCount());
        Assertions.assertTrue(f0.getTableIdentifier().equals(String.valueOf(t0.getId())));

        OperatorFeatures.TableFeature f00 = new OperatorFeatures.TableFeature(t00, Statistics.builder().build());
        Assertions.assertTrue(f00.getTable().equals(t00));

        OperatorFeatures.TableFeature f1 = new OperatorFeatures.TableFeature(t1, Statistics.builder().build());
        Assertions.assertTrue(f1.getTable().equals(t1));
        Assertions.assertFalse(f1.getTable().equals(t0));
        Assertions.assertTrue(f1.getRowCount() == t1.getRowCount());
        Assertions.assertFalse(f1.getTableIdentifier().equals(t0.getId()));

        Assertions.assertTrue(f0.equals(f00));
    }

    @Test
    public void testTableFeatures2() {
        Table t0 = starRocksAssert.getTable("test", "ods_order");
        Assertions.assertTrue(t0 != null);
        Statistics statistics = Statistics.builder().build();
        OperatorFeatures.TableFeature f0 = new OperatorFeatures.TableFeature(t0, statistics);
        Assertions.assertTrue(f0.getTable().equals(t0));
        Assertions.assertTrue(f0.getRowCount() == (long) statistics.getOutputRowCount());
        String expect = String.format("%s.%s", t0.getCatalogDBName(), t0.getCatalogTableName());
        Assertions.assertTrue(f0.getTableIdentifier().equals(expect));
        Table t00 = starRocksAssert.getTable("test", "ods_order");

        OperatorFeatures.TableFeature f00 = new OperatorFeatures.TableFeature(t00, statistics);
        Assertions.assertTrue(f00.equals(f0));
    }
}