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

package com.starrocks.sql.ast;

import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.type.DateType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.List;
import java.util.Locale;

public class MultiRangePartitionDescTest {

    private static List<SingleRangePartitionDesc> convert(String begin, String end, long step, String timeUnit,
                                                          Type columnType) throws AnalysisException {
        MultiRangePartitionDesc desc =
                new MultiRangePartitionDesc(begin, end, step, timeUnit, NodePosition.ZERO);
        PartitionConvertContext context = new PartitionConvertContext();
        context.setFirstPartitionColumnType(columnType);
        return desc.convertToSingle(context);
    }

    private static void assertPartition(SingleRangePartitionDesc desc, String name, String lower, String upper) {
        Assertions.assertEquals(name, desc.getPartitionName());
        Assertions.assertEquals(lower, desc.getPartitionKeyDesc().getLowerValues().get(0).getStringValue());
        Assertions.assertEquals(upper, desc.getPartitionKeyDesc().getUpperValues().get(0).getStringValue());
    }

    /** Year 0000 used to be formatted as 0001, collapsing the range and failing the DDL. */
    @Test
    public void testBuildYearPartitionOnYearZero() throws AnalysisException {
        List<SingleRangePartitionDesc> descs = convert("0000-01-01", "0002-01-01", 1, "year", DateType.DATE);
        Assertions.assertEquals(2, descs.size());
        assertPartition(descs.get(0), "p0000", "0000-01-01", "0001-01-01");
        assertPartition(descs.get(1), "p0001", "0001-01-01", "0002-01-01");
    }

    @Test
    public void testBuildDayPartitionOnYearZero() throws AnalysisException {
        List<SingleRangePartitionDesc> descs = convert("0000-01-01", "0000-01-03", 1, "day", DateType.DATE);
        Assertions.assertEquals(2, descs.size());
        assertPartition(descs.get(0), "p00000101", "0000-01-01", "0000-01-02");
        assertPartition(descs.get(1), "p00000102", "0000-01-02", "0000-01-03");
    }

    @Test
    public void testBuildMonthPartitionOnYearZeroDatetime() throws AnalysisException {
        List<SingleRangePartitionDesc> descs =
                convert("0000-01-01 00:00:00", "0000-03-01 00:00:00", 1, "month", DateType.DATETIME);
        Assertions.assertEquals(2, descs.size());
        assertPartition(descs.get(0), "p000001", "0000-01-01 00:00:00", "0000-02-01 00:00:00");
        assertPartition(descs.get(1), "p000002", "0000-02-01 00:00:00", "0000-03-01 00:00:00");
    }

    @Test
    public void testBuildYearPartition() throws AnalysisException {
        List<SingleRangePartitionDesc> descs = convert("2023-01-01", "2025-01-01", 1, "year", DateType.DATE);
        Assertions.assertEquals(2, descs.size());
        assertPartition(descs.get(0), "p2023", "2023-01-01", "2024-01-01");
        assertPartition(descs.get(1), "p2024", "2024-01-01", "2025-01-01");
    }

    @Test
    public void testBuildDayPartition() throws AnalysisException {
        List<SingleRangePartitionDesc> descs = convert("2024-02-28", "2024-03-02", 1, "day", DateType.DATE);
        Assertions.assertEquals(3, descs.size());
        assertPartition(descs.get(0), "p20240228", "2024-02-28", "2024-02-29");
        assertPartition(descs.get(1), "p20240229", "2024-02-29", "2024-03-01");
        assertPartition(descs.get(2), "p20240301", "2024-03-01", "2024-03-02");
    }

    @Test
    public void testBuildHourPartition() throws AnalysisException {
        List<SingleRangePartitionDesc> descs =
                convert("2024-03-05 00:00:00", "2024-03-05 02:00:00", 1, "hour", DateType.DATETIME);
        Assertions.assertEquals(2, descs.size());
        assertPartition(descs.get(0), "p2024030500", "2024-03-05 00:00:00", "2024-03-05 01:00:00");
        assertPartition(descs.get(1), "p2024030501", "2024-03-05 01:00:00", "2024-03-05 02:00:00");
    }

    @Test
    public void testBuildWeekPartition() throws AnalysisException {
        // 2024-01-01 is a Monday.
        List<SingleRangePartitionDesc> descs = convert("2024-01-01", "2024-01-15", 1, "week", DateType.DATE);
        Assertions.assertEquals(2, descs.size());
        assertPartition(descs.get(0), "p2024_01", "2024-01-01", "2024-01-08");
        assertPartition(descs.get(1), "p2024_02", "2024-01-08", "2024-01-15");
    }

    @Test
    public void testBuildWeekPartitionOnYearZero() throws AnalysisException {
        // 0000-01-03 is a Monday. The week number follows the locale's week rules read off the
        // proleptic date, so derive it the same way rather than hard-coding one locale's answer.
        WeekFields weekFields = WeekFields.of(Locale.getDefault(Locale.Category.FORMAT));
        String firstWeek = String.format("p0000_%02d", LocalDate.of(0, 1, 3).get(weekFields.weekOfWeekBasedYear()));
        String secondWeek = String.format("p0000_%02d", LocalDate.of(0, 1, 10).get(weekFields.weekOfWeekBasedYear()));

        List<SingleRangePartitionDesc> descs = convert("0000-01-03", "0000-01-17", 1, "week", DateType.DATE);
        Assertions.assertEquals(2, descs.size());
        assertPartition(descs.get(0), firstWeek, "0000-01-03", "0000-01-10");
        assertPartition(descs.get(1), secondWeek, "0000-01-10", "0000-01-17");
    }

    @Test
    public void testBuildWeekPartitionRejectsStartBeforeFirstWeek() {
        // 0000-01-01 is a Saturday, so its week begins at -0001-12-27, which no DATE can hold.
        SemanticException e = Assertions.assertThrows(SemanticException.class,
                () -> convert("0000-01-01", "0000-02-01", 1, "week", DateType.DATE));
        Assertions.assertTrue(e.getMessage().contains("week begins before 0000-01-01"), e.getMessage());
    }

    @Test
    public void testUnsupportedTimeUnit() {
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> convert("2024-03-05 00:00:00", "2024-03-05 00:10:00", 1, "minute", DateType.DATETIME));
        Assertions.assertTrue(e.getMessage().contains("Batch build partition does not support time interval type"),
                e.getMessage());
    }
}
