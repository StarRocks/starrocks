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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.analyzer.PartitionDescAnalyzer;
import com.starrocks.sql.ast.PartitionKeyDesc;
import com.starrocks.sql.ast.PartitionKeyDesc.PartitionRangeType;
import com.starrocks.sql.ast.PartitionValue;
import com.starrocks.sql.ast.SingleRangePartitionDesc;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class RangePartitionInfoTest {

    private List<Column> partitionColumns;
    private RangePartitionInfo partitionInfo;

    private List<SingleRangePartitionDesc> singleRangePartitionDescs;

    @BeforeEach
    public void setUp() {
        partitionColumns = new LinkedList<Column>();
        singleRangePartitionDescs = new LinkedList<SingleRangePartitionDesc>();
    }

    @Test
    public void testTinyInt() {
        assertThrows(DdlException.class, () -> {
            Column k1 = new Column("k1", new ScalarType(PrimitiveType.TINYINT), true, null, "", "");
            partitionColumns.add(k1);

            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                    new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("-128"))),
                    null));

            partitionInfo = new RangePartitionInfo(partitionColumns);
            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, 1, null);
                partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                        singleRangePartitionDesc, 20000L, false);
            }
        });
    }

    @Test
    public void testSmallInt() {
        assertThrows(DdlException.class, () -> {
            Column k1 = new Column("k1", new ScalarType(PrimitiveType.SMALLINT), true, null, "", "");
            partitionColumns.add(k1);

            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                    new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("-32768"))),
                    null));

            partitionInfo = new RangePartitionInfo(partitionColumns);
            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, 1, null);
                partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                        singleRangePartitionDesc, 20000L, false);
            }
        });
    }

    @Test
    public void testInt() {
        assertThrows(DdlException.class, () -> {
            Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
            partitionColumns.add(k1);

            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1",
                    new PartitionKeyDesc(Lists.newArrayList(new PartitionValue("-2147483648"))),
                    null));

            partitionInfo = new RangePartitionInfo(partitionColumns);
            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, 1, null);
                partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                        singleRangePartitionDesc, 20000L, false);
            }
        });
    }

    @Test
    public void testBigInt() {
        assertThrows(DdlException.class, () -> {
            Column k1 = new Column("k1", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
            partitionColumns.add(k1);

            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                    .newArrayList(new PartitionValue("-9223372036854775808"))), null));
            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", new PartitionKeyDesc(Lists
                    .newArrayList(new PartitionValue("-9223372036854775806"))), null));
            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", new PartitionKeyDesc(Lists
                    .newArrayList(new PartitionValue("0"))), null));
            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p4", new PartitionKeyDesc(Lists
                    .newArrayList(new PartitionValue("9223372036854775806"))), null));

            partitionInfo = new RangePartitionInfo(partitionColumns);

            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, 1, null);
                partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                        singleRangePartitionDesc, 20000L, false);
            }
        });
    }

    @Test
    public void testBigIntNormal() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("-9223372036854775806"))), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("-9223372036854775805"))), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("0"))), null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p4", new PartitionKeyDesc(Lists
                .newArrayList(new PartitionValue("9223372036854775806"))), null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, 1, null);
            partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                    singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p0 VALUES  [("20190101", "100"),("20190101", "200")),
     * PARTITION p1 VALUES  [("20190105", "10"),("20190107", "10")),
     * PARTITION p2 VALUES  [("20181231", "10"),("20190101", "100")),
     * PARTITION p3 VALUES  [("20190105", "100"),("20190120", MAXVALUE))
     * )
     */
    @Test
    public void testFixedRange() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("200")));
        PartitionKeyDesc p2 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190105"), new PartitionValue("10")),
                Lists.newArrayList(new PartitionValue("20190107"), new PartitionValue("10")));
        PartitionKeyDesc p3 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20181231"), new PartitionValue("10")),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")));
        PartitionKeyDesc p4 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190105"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190120"), new PartitionValue("10000000000")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", p2, null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", p3, null));
        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p4", p4, null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, columns, null);
            partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                    singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * failed cases less than && fixed
     * partition by range(k1,k2,k3) (
     * partition p1 values less than("2019-02-01", "100", "200"),
     * partition p2 values [("2020-02-01", "100", "200"), (MAXVALUE)),
     * partition p3 values less than("2021-02-01")
     * )
     */
    @Test
    public void testFixedRange1() {
        assertThrows(AnalysisException.class, () -> {
            //add columns
            Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
            Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
            Column k3 = new Column("k3", new ScalarType(PrimitiveType.INT), true, null, "", "");
            partitionColumns.add(k1);
            partitionColumns.add(k2);
            partitionColumns.add(k3);

            //add RangePartitionDescs
            PartitionKeyDesc p1 = new PartitionKeyDesc(
                    Lists.newArrayList(new PartitionValue("2019-02-01"), new PartitionValue("100"),
                            new PartitionValue("200")));
            PartitionKeyDesc p2 = new PartitionKeyDesc(
                    Lists.newArrayList(new PartitionValue("2020-02-01"), new PartitionValue("100"),
                            new PartitionValue("200")),
                    Lists.newArrayList(new PartitionValue("10000000000")));
            PartitionKeyDesc p3 = new PartitionKeyDesc(
                    Lists.newArrayList(new PartitionValue("2021-02-01")));

            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));
            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p2", p2, null));
            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p3", p3, null));
            partitionInfo = new RangePartitionInfo(partitionColumns);
            PartitionRangeType partitionType = PartitionRangeType.INVALID;
            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                // check partitionType
                if (partitionType == PartitionRangeType.INVALID) {
                    partitionType = singleRangePartitionDesc.getPartitionKeyDesc().getPartitionType();
                } else if (partitionType != singleRangePartitionDesc.getPartitionKeyDesc().getPartitionType()) {
                    throw new AnalysisException("You can only use one of these methods to create partitions");
                }
                PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, partitionColumns.size(), null);
                partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                        singleRangePartitionDesc, 20000L, false);
            }
        });
    }

    /**
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p1 VALUES  [(), ("20190301", "400"))
     * )
     */
    @Test
    public void testFixedRange2() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(new ArrayList<>(),
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("200")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, columns, null);
            partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                    singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * failed cases
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p1 VALUES  [("20190301", "400"), ())
     * )
     */
    @Test
    public void testFixedRange3() {
        assertThrows(AnalysisException.class, () -> {
            //add columns
            int columns = 2;
            Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
            Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
            partitionColumns.add(k1);
            partitionColumns.add(k2);

            //add RangePartitionDescs
            PartitionKeyDesc p1 = new PartitionKeyDesc(
                    Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("200")),
                    new ArrayList<>());

            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

            partitionInfo = new RangePartitionInfo(partitionColumns);

            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, columns, null);
                partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                        singleRangePartitionDesc, 20000L, false);
            }
        });
    }

    /**
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p0 VALUES  [("20190101", "100"),("20190201"))
     * )
     */
    @Test
    public void testFixedRange4() throws DdlException, AnalysisException {
        //add columns
        int columns = 2;
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
        partitionColumns.add(k1);
        partitionColumns.add(k2);

        //add RangePartitionDescs
        PartitionKeyDesc p1 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")),
                Lists.newArrayList(new PartitionValue("20190201")));

        singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

        partitionInfo = new RangePartitionInfo(partitionColumns);

        for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
            PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, columns, null);
            partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                    singleRangePartitionDesc, 20000L, false);
        }
    }

    /**
     * failed cases
     * PARTITION BY RANGE(`k1`, `k2`) (
     * PARTITION p0 VALUES  [("20190101", "100"),("20190101", "100"))
     * )
     */
    @Test
    public void testFixedRange5() {
        assertThrows(DdlException.class, () -> {
            //add columns
            int columns = 2;
            Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
            Column k2 = new Column("k2", new ScalarType(PrimitiveType.BIGINT), true, null, "", "");
            partitionColumns.add(k1);
            partitionColumns.add(k2);

            //add RangePartitionDescs
            PartitionKeyDesc p1 = new PartitionKeyDesc(
                    Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")),
                    Lists.newArrayList(new PartitionValue("20190101"), new PartitionValue("100")));

            singleRangePartitionDescs.add(new SingleRangePartitionDesc(false, "p1", p1, null));

            partitionInfo = new RangePartitionInfo(partitionColumns);

            for (SingleRangePartitionDesc singleRangePartitionDesc : singleRangePartitionDescs) {
                PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(singleRangePartitionDesc, columns, null);
                partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns),
                        singleRangePartitionDesc, 20000L, false);
            }
        });
    }

    /**
     * Regression test: when adding a fixed range partition with an explicitly specified lower bound, we must validate
     * intersection against both the predecessor and successor partitions. Only checking the successor can miss overlaps
     * with earlier partitions when the lower bound is much smaller than expected.
     */
    @Test
    public void testFixedRangeOverlapShouldCheckPreviousRange() {
        assertThrows(DdlException.class, () -> {
            // Single-column INT range partitioning.
            int columns = 1;
            Column partDt = new Column("part_dt", new ScalarType(PrimitiveType.INT), true, null, "", "");
            partitionColumns.add(partDt);
            partitionInfo = new RangePartitionInfo(partitionColumns);

            // Existing partitions are non-overlapping.
            PartitionKeyDesc p1 = new PartitionKeyDesc(
                    Lists.newArrayList(new PartitionValue("20220901")),
                    Lists.newArrayList(new PartitionValue("20220902")));
            SingleRangePartitionDesc d1 = new SingleRangePartitionDesc(false, "p1", p1, null);
            PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(d1, columns, null);
            partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns), d1, 1L, false);

            PartitionKeyDesc p2 = new PartitionKeyDesc(
                    Lists.newArrayList(new PartitionValue("20260110")),
                    Lists.newArrayList(new PartitionValue("20260111")));
            SingleRangePartitionDesc d2 = new SingleRangePartitionDesc(false, "p2", p2, null);
            PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(d2, columns, null);
            partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns), d2, 2L, false);

            // Buggy fixed-range: upper bound equals successor's lower bound (so it doesn't intersect the successor),
            // but its lower bound is far smaller and intersects earlier partitions.
            PartitionKeyDesc bad = new PartitionKeyDesc(
                    Lists.newArrayList(new PartitionValue("2020109")),
                    Lists.newArrayList(new PartitionValue("20260110")));
            SingleRangePartitionDesc dBad = new SingleRangePartitionDesc(false, "p_bad", bad, null);
            PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(dBad, columns, null);
            partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns), dBad, 3L, false);
        });
    }

    @Test
    public void testGetEnclosingPartitionId() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        partitionColumns.add(k1);
        partitionInfo = new RangePartitionInfo(partitionColumns);

        PartitionKeyDesc yearlyRange = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2022-01-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2023-01-01 00:00:00")));
        SingleRangePartitionDesc yearlyDesc = new SingleRangePartitionDesc(false, "p_yearly", yearlyRange, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(yearlyDesc, 1, null);
        partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns), yearlyDesc, 1L, false);

        PartitionKeyDesc monthlyRange = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2022-09-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2022-10-01 00:00:00")));
        SingleRangePartitionDesc monthlyDesc = new SingleRangePartitionDesc(false, "p_monthly", monthlyRange, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(monthlyDesc, 1, null);
        Assertions.assertEquals(1L, partitionInfo.getEnclosingPartitionId(
                MetaUtils.buildIdToColumn(partitionColumns), monthlyDesc, false));

        PartitionKeyDesc exactRange = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2022-01-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2023-01-01 00:00:00")));
        SingleRangePartitionDesc exactDesc = new SingleRangePartitionDesc(false, "p_exact", exactRange, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(exactDesc, 1, null);
        Assertions.assertEquals(1L, partitionInfo.getEnclosingPartitionId(
                MetaUtils.buildIdToColumn(partitionColumns), exactDesc, false));

        PartitionKeyDesc outsideRange = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2023-01-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2023-02-01 00:00:00")));
        SingleRangePartitionDesc outsideDesc = new SingleRangePartitionDesc(false, "p_outside", outsideRange, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(outsideDesc, 1, null);
        Assertions.assertEquals(-1L, partitionInfo.getEnclosingPartitionId(
                MetaUtils.buildIdToColumn(partitionColumns), outsideDesc, false));

        PartitionKeyDesc partialOverlapRange = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2022-06-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2023-06-01 00:00:00")));
        SingleRangePartitionDesc partialDesc = new SingleRangePartitionDesc(false, "p_partial", partialOverlapRange, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(partialDesc, 1, null);
        Assertions.assertEquals(-1L, partitionInfo.getEnclosingPartitionId(
                MetaUtils.buildIdToColumn(partitionColumns), partialDesc, false));
    }

    @Test
    public void testGetEnclosingPartitionIdWithMultiplePartitions() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        partitionColumns.add(k1);
        partitionInfo = new RangePartitionInfo(partitionColumns);

        PartitionKeyDesc year2021 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2021-01-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2022-01-01 00:00:00")));
        SingleRangePartitionDesc desc2021 = new SingleRangePartitionDesc(false, "p2021", year2021, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(desc2021, 1, null);
        partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns), desc2021, 1L, false);

        PartitionKeyDesc year2022 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2022-01-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2023-01-01 00:00:00")));
        SingleRangePartitionDesc desc2022 = new SingleRangePartitionDesc(false, "p2022", year2022, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(desc2022, 1, null);
        partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns), desc2022, 2L, false);

        PartitionKeyDesc month202209 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2022-09-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2022-10-01 00:00:00")));
        SingleRangePartitionDesc descMonthly = new SingleRangePartitionDesc(false, "p202209", month202209, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(descMonthly, 1, null);
        Assertions.assertEquals(2L, partitionInfo.getEnclosingPartitionId(
                MetaUtils.buildIdToColumn(partitionColumns), descMonthly, false));

        PartitionKeyDesc month202106 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2021-06-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2021-07-01 00:00:00")));
        SingleRangePartitionDesc descMonth202106 = new SingleRangePartitionDesc(false, "p202106", month202106, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(descMonth202106, 1, null);
        Assertions.assertEquals(1L, partitionInfo.getEnclosingPartitionId(
                MetaUtils.buildIdToColumn(partitionColumns), descMonth202106, false));

        PartitionKeyDesc month202303 = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2023-03-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2023-04-01 00:00:00")));
        SingleRangePartitionDesc descMonth202303 = new SingleRangePartitionDesc(false, "p202303", month202303, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(descMonth202303, 1, null);
        Assertions.assertEquals(-1L, partitionInfo.getEnclosingPartitionId(
                MetaUtils.buildIdToColumn(partitionColumns), descMonth202303, false));

        PartitionKeyDesc crossYear = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2021-12-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2022-02-01 00:00:00")));
        SingleRangePartitionDesc descCrossYear = new SingleRangePartitionDesc(false, "p_cross", crossYear, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(descCrossYear, 1, null);
        Assertions.assertEquals(-1L, partitionInfo.getEnclosingPartitionId(
                MetaUtils.buildIdToColumn(partitionColumns), descCrossYear, false));
    }

    @Test
    public void testAutoPartitionEnclosedByMergedPartitionShouldNotThrow() throws DdlException, AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        partitionColumns.add(k1);
        partitionInfo = new RangePartitionInfo(partitionColumns);

        PartitionKeyDesc yearlyRange = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2022-01-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2023-01-01 00:00:00")));
        SingleRangePartitionDesc yearlyDesc = new SingleRangePartitionDesc(false, "p_yearly", yearlyRange, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(yearlyDesc, 1, null);
        partitionInfo.handleNewSinglePartitionDesc(MetaUtils.buildIdToColumn(partitionColumns), yearlyDesc, 1L, false);

        PartitionKeyDesc monthlyRange = new PartitionKeyDesc(
                Lists.newArrayList(new PartitionValue("2022-09-01 00:00:00")),
                Lists.newArrayList(new PartitionValue("2022-10-01 00:00:00")));
        SingleRangePartitionDesc monthlyDesc = new SingleRangePartitionDesc(false, "p_monthly", monthlyRange, null);
        PartitionDescAnalyzer.analyzeSingleRangePartitionDesc(monthlyDesc, 1, null);
        assertThrows(DdlException.class, () -> {
            partitionInfo.checkAndCreateRange(MetaUtils.buildIdToColumn(partitionColumns), monthlyDesc, false);
        });

        Assertions.assertEquals(1L, partitionInfo.getEnclosingPartitionId(
                MetaUtils.buildIdToColumn(partitionColumns), monthlyDesc, false));
    }

    @Test
    public void testCopyPartitionInfo() throws Exception {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.INT), true, null, "", "");
        partitionColumns.add(k1);
        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(partitionColumns);
        PartitionKey partitionKey = PartitionKey.createInfinityPartitionKey(partitionColumns, true);
        Range<PartitionKey> range = Range.atLeast(partitionKey);
        rangePartitionInfo.addPartition(123L, false, range, null, (short) 1, null);
        RangePartitionInfo copyInfo = (RangePartitionInfo) rangePartitionInfo.clone();
        rangePartitionInfo.dropPartition(123L);
        Assertions.assertTrue(copyInfo.getRange(123L) != null);
        Assertions.assertTrue(rangePartitionInfo.getRange(123L) == null);
    }

}
