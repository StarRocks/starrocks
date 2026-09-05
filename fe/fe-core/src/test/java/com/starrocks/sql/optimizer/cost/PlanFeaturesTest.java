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
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

class PlanFeaturesTest extends PlanTestBase {

    @ParameterizedTest
    @MethodSource("basicCases")
    public void testBasic(String query, String expectedTables, List<ExpectedOperatorSlice> expectedSlices)
            throws Exception {
        expectedTables = StringUtils.trim(expectedTables);

        ExecPlan execPlan = getExecPlan(query);
        OptExpression physicalPlan = execPlan.getPhysicalPlan();
        PlanFeatures planFeatures = FeatureExtractor.extractFeatures(physicalPlan);

        // feature string
        String featureString = planFeatures.toFeatureString();
        Assertions.assertTrue(featureString.startsWith(expectedTables), featureString);
        Map<OperatorType, List<Long>> featureSlices = operatorSlicesFromFeatureString(featureString);

        // feature csv
        String csv = planFeatures.toFeatureCsv();
        Map<OperatorType, List<Long>> csvSlices = operatorSlicesFromCsv(csv);

        for (ExpectedOperatorSlice expectedSlice : expectedSlices) {
            assertOperatorSlice(featureSlices, expectedSlice, "feature string");
            assertOperatorSlice(csvSlices, expectedSlice, "feature csv");
        }
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
        Assertions.assertEquals(expectedOperatorHeaderCount(), numOperators);
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

    private static Stream<Arguments> basicCases() {
        return Stream.of(
                Arguments.of(
                        "select count(*) from t0 where v1 < 100 limit 100 ",
                        "tables=[0,0,10003]",
                        List.of(
                                slice(OperatorType.PHYSICAL_DISTRIBUTION, 1, 0, 8, 0, 2, 0, 3),
                                slice(OperatorType.PHYSICAL_HASH_AGG, 1, 0, 8, 2, 2, 4, 0, 0, 1, 1),
                                slice(OperatorType.PHYSICAL_OLAP_SCAN, 1, 0, 9, 0, 2, 0, 0, 0, 1, 1))),
                Arguments.of(
                        "select max(v1) from t0 where v1 < 100 limit 100",
                        "tables=[0,0,10003]",
                        List.of(
                                slice(OperatorType.PHYSICAL_DISTRIBUTION, 1, 0, 8, 0, 2, 0, 3),
                                slice(OperatorType.PHYSICAL_HASH_AGG, 1, 0, 8, 2, 2, 4, 0, 0, 1, 1),
                                slice(OperatorType.PHYSICAL_OLAP_SCAN, 1, 0, 8, 0, 2, 0, 0, 0, 1, 1))),
                Arguments.of(
                        "select v1, count(*) from t0 group by v1 ",
                        "tables=[0,0,10003]",
                        List.of(
                                slice(OperatorType.PHYSICAL_HASH_AGG, 1, 0, 16, 2, 2, 0, 0, 1, 1, 1),
                                slice(OperatorType.PHYSICAL_OLAP_SCAN, 1, 0, 8, 0, 2, 0, 0, 0, 0, 0))),
                Arguments.of(
                        "select count(*) from t0 a join t0 b on a.v1 = b.v2",
                        "tables=[0,0,10003]",
                        List.of(
                                slice(OperatorType.PHYSICAL_DISTRIBUTION, 2, 0, 16, 2, 4, 0, 4),
                                slice(OperatorType.PHYSICAL_HASH_AGG, 2, 0, 16, 2, 2, 0, 0, 0, 2, 2),
                                slice(OperatorType.PHYSICAL_OLAP_SCAN, 2, 0, 16, 0, 4, 0, 0, 0, 2, 0))),
                // mysql external table
                Arguments.of(
                        "select * from ods_order where org_order_no",
                        "tables=[0,0,test.ods_order]",
                        List.of(
                                sliceZeros(OperatorType.PHYSICAL_DISTRIBUTION, 0),
                                sliceZeros(OperatorType.PHYSICAL_HASH_AGG, 0),
                                sliceZeros(OperatorType.PHYSICAL_HASH_JOIN, 0),
                                sliceZeros(OperatorType.PHYSICAL_NESTLOOP_JOIN, 0))),
                Arguments.of(
                        "select * from (select * from ods_order join mysql_table where k1  = 'a' and order_dt = 'c') " +
                                "t1 where t1.k2 = 'c'",
                        "tables=[0,test.ods_order,db1.tbl1]",
                        List.of(
                                slice(OperatorType.PHYSICAL_DISTRIBUTION, 1, 0, 8, 2, 2, 0, 1),
                                sliceZeros(OperatorType.PHYSICAL_HASH_AGG, 0),
                                sliceZeros(OperatorType.PHYSICAL_HASH_JOIN, 0),
                                slice(OperatorType.PHYSICAL_NESTLOOP_JOIN, 1))),
                Arguments.of(
                        "select * from ods_order join mysql_table where k1  = 'a' and order_dt = 'c'",
                        "tables=[0,test.ods_order,db1.tbl1]",
                        List.of(
                                slice(OperatorType.PHYSICAL_DISTRIBUTION, 1, 0, 8, 2, 2, 0, 1),
                                sliceZeros(OperatorType.PHYSICAL_HASH_AGG, 0),
                                sliceZeros(OperatorType.PHYSICAL_HASH_JOIN, 0),
                                slice(OperatorType.PHYSICAL_NESTLOOP_JOIN, 1)))
        );
    }

    private static ExpectedOperatorSlice slice(OperatorType type, long count, long... vectorPrefix) {
        return new ExpectedOperatorSlice(type, count, vectorPrefix);
    }

    private static ExpectedOperatorSlice sliceZeros(OperatorType type, long count) {
        return new ExpectedOperatorSlice(type, count, zeroVector(type));
    }

    private static long[] zeroVector(OperatorType type) {
        int length = OperatorFeatures.vectorLength(type);
        long[] zeros = new long[length];
        for (int i = 0; i < length; i++) {
            zeros[i] = 0L;
        }
        return zeros;
    }

    private static Map<OperatorType, List<Long>> operatorSlicesFromFeatureString(String featureString) {
        String operators = StringUtils.substringBetween(featureString, "operators=[", "]");
        Assertions.assertNotNull(operators, "Missing operators in feature string: " + featureString);
        List<Long> values = parseLongList(Splitter.on(",").trimResults().splitToList(operators));
        return decodeOperatorVectors(values);
    }

    private static Map<OperatorType, List<Long>> operatorSlicesFromCsv(String csv) {
        List<String> parts = Splitter.on(",").trimResults().splitToList(csv);
        int offset = operatorVectorOffset();
        List<Long> values = parseLongList(parts.subList(offset, parts.size()));
        return decodeOperatorVectors(values);
    }

    private static int operatorVectorOffset() {
        List<String> headers = Splitter.on(",").splitToList(PlanFeatures.featuresHeader());
        int offset = 0;
        for (String header : headers) {
            if (header.startsWith("operators_")) {
                break;
            }
            offset++;
        }
        return offset;
    }

    private static List<Long> parseLongList(List<String> values) {
        List<Long> result = new ArrayList<>(values.size());
        for (String value : values) {
            if (value.isEmpty()) {
                continue;
            }
            result.add(Long.parseLong(value));
        }
        return result;
    }

    private static Map<OperatorType, List<Long>> decodeOperatorVectors(List<Long> values) {
        Map<OperatorType, List<Long>> slices = new EnumMap<>(OperatorType.class);
        int index = 0;
        for (int start = OperatorType.PHYSICAL.ordinal() + 1; start < OperatorType.SCALAR.ordinal(); start++) {
            OperatorType opType = OperatorType.values()[start];
            if (PlanFeatures.skipOperator(opType)) {
                continue;
            }
            int length = OperatorFeatures.vectorLength(opType) + PlanFeatures.AggregatedFeature.numExtraFeatures(opType);
            List<Long> slice = values.subList(index, index + length);
            slices.put(opType, new ArrayList<>(slice));
            index += length;
        }
        Assertions.assertEquals(values.size(), index, "operator feature vector length mismatch");
        return slices;
    }

    private static void assertOperatorSlice(Map<OperatorType, List<Long>> slices, ExpectedOperatorSlice expected,
                                            String source) {
        List<Long> actual = slices.get(expected.type);
        Assertions.assertNotNull(actual, "Missing operator slice for " + expected.type + " in " + source);
        int requiredSize = 2 + expected.vectorPrefix.length;
        Assertions.assertTrue(actual.size() >= requiredSize,
                "Slice length is " + actual.size() + " for " + expected.type + " in " + source);
        Assertions.assertEquals((long) expected.type.ordinal(), actual.get(0),
                "Operator ordinal mismatch for " + expected.type + " in " + source);
        Assertions.assertEquals(expected.count, actual.get(1),
                "Operator count mismatch for " + expected.type + " in " + source);
        for (int i = 0; i < expected.vectorPrefix.length; i++) {
            Assertions.assertEquals(expected.vectorPrefix[i], actual.get(2 + i),
                    "Vector mismatch for " + expected.type + " index " + i + " in " + source);
        }
    }

    private static int expectedOperatorHeaderCount() {
        int num = 0;
        for (int start = OperatorType.PHYSICAL.ordinal() + 1; start < OperatorType.SCALAR.ordinal(); start++) {
            OperatorType opType = OperatorType.values()[start];
            if (PlanFeatures.skipOperator(opType)) {
                continue;
            }
            num += OperatorFeatures.vectorLength(opType);
            num += PlanFeatures.AggregatedFeature.numExtraFeatures(opType);
        }
        return num;
    }

    private static final class ExpectedOperatorSlice {
        private final OperatorType type;
        private final long count;
        private final long[] vectorPrefix;

        private ExpectedOperatorSlice(OperatorType type, long count, long[] vectorPrefix) {
            this.type = type;
            this.count = count;
            this.vectorPrefix = vectorPrefix;
        }
    }
}
