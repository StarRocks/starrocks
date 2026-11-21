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

package com.starrocks.persist.image;

import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.proto.BasicStatsMetaPB;
import com.starrocks.persist.proto.ColumnStatsMetaPB;
import com.starrocks.persist.proto.ExternalBasicStatsMetaPB;
import com.starrocks.persist.proto.ExternalHistogramStatsMetaPB;
import com.starrocks.persist.proto.HistogramStatsMetaPB;
import com.starrocks.persist.proto.MultiColumnStatsMetaPB;
import com.starrocks.persist.proto.StatisticsTypeListPB;
import com.starrocks.sql.ast.StatisticsType;
import com.starrocks.statistic.BasicStatsMeta;
import com.starrocks.statistic.ColumnStatsMeta;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.ExternalHistogramStatsMeta;
import com.starrocks.statistic.HistogramStatsMeta;
import com.starrocks.statistic.MultiColumnStatsMeta;
import com.starrocks.statistic.StatsConstants;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.common.jmockit.Deencapsulation.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatsMetaProtoTest {
    static ColumnStatsMeta column(String name) {
        return new ColumnStatsMeta(name, StatsConstants.AnalyzeType.SAMPLE, AnalyzeJobProtoTest.TIME,
                new HashSet<>(Set.of(-7L, 101L)), 23);
    }

    static BasicStatsMeta basic(long table) {
        BasicStatsMeta meta = new BasicStatsMeta(10, table, List.of("legacy"), StatsConstants.AnalyzeType.FULL,
                AnalyzeJobProtoTest.TIME, new HashMap<>(Map.of("sample", "17")), 12345);
        meta.increaseDeltaRows(321L);
        meta.addColumnStatsMeta(column("a"));
        setField(meta, "tabletStatsReportTime", AnalyzeJobProtoTest.TIME.minusDays(1));
        return meta;
    }

    static HistogramStatsMeta histogram() {
        return new HistogramStatsMeta(10, 20, "a", StatsConstants.AnalyzeType.HISTOGRAM,
                AnalyzeJobProtoTest.TIME, new HashMap<>(Map.of("buckets", "64")));
    }

    static ExternalBasicStatsMeta externalBasic() {
        ExternalBasicStatsMeta meta = new ExternalBasicStatsMeta("hive", "db", "table", List.of("legacy"),
                StatsConstants.AnalyzeType.FULL, AnalyzeJobProtoTest.TIME, new HashMap<>(Map.of("sample", "17")));
        meta.setTableUUID("hive.db.table.123");
        meta.addColumnStatsMeta(column("a"));
        return meta;
    }

    static ExternalHistogramStatsMeta externalHistogram() {
        ExternalHistogramStatsMeta meta = new ExternalHistogramStatsMeta("hive", "db", "table", "a",
                StatsConstants.AnalyzeType.HISTOGRAM, AnalyzeJobProtoTest.TIME, Map.of("buckets", "64"));
        meta.setTableUUID("hive.db.table.123");
        return meta;
    }

    static MultiColumnStatsMeta multiColumn() {
        return new MultiColumnStatsMeta(10, 20, new HashSet<>(Set.of(2, 4)), StatsConstants.AnalyzeType.FULL,
                new ArrayList<>(List.of(StatisticsType.MCDISTINCT)), AnalyzeJobProtoTest.TIME,
                new HashMap<>(Map.of("sample", "17")));
    }

    @Test
    public void testColumnSampleState() throws Exception {
        ColumnStatsMeta source = column("a");
        ColumnStatsMeta loaded = ColumnStatsMeta.fromProto(ColumnStatsMetaPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, ColumnStatsMeta.class), loaded);
        assertEquals(Set.of(-7L, 101L), loaded.getSampledPartitionsHashValue());
        assertEquals(23, loaded.getAllPartitionSize());
        assertEquals(AnalyzeJobProtoTest.TIME.withNano(0), loaded.getUpdateTime());
        assertEquals(source.simpleString(true), loaded.simpleString(true));
        loaded.getSampledPartitionsHashValue().clear();
        assertEquals(2, source.getSampledPartitionsHashValue().size());
    }

    @Test
    public void testColumnNullAndEmptyFields() {
        ColumnStatsMeta source = new ColumnStatsMeta(null, null, null, null, 0);
        ColumnStatsMeta loaded = ColumnStatsMeta.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, ColumnStatsMeta.class), loaded);
        assertNull(loaded.getSampledPartitionsHashValue());
        setField(source, "sampledPartitionsHashValue", new HashSet<>());
        assertTrue(ColumnStatsMeta.fromProto(source.toProto()).getSampledPartitionsHashValue().isEmpty());
    }

    @Test
    public void testBasicRowsAndColumnMetadata() throws Exception {
        BasicStatsMeta source = basic(20);
        BasicStatsMeta loaded = BasicStatsMeta.fromProto(BasicStatsMetaPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, BasicStatsMeta.class), loaded);
        assertEquals(12666, loaded.getTotalRows());
        assertEquals(321L, (long) getField(loaded, "deltaRows"));
        assertEquals(List.of("legacy"), getField(loaded, "columns"));
        assertEquals(List.of("a"), loaded.getColumns());
        assertEquals(AnalyzeJobProtoTest.TIME.minusDays(1).withNano(0), loaded.getTabletStatsReportTime());
        assertEquals(Set.of(-7L, 101L), loaded.getAnalyzedColumns().get("a").getSampledPartitionsHashValue());
        loaded.increaseDeltaRows(10L);
        assertEquals(321L, (long) getField(source, "deltaRows"));
    }

    @Test
    public void testBasicMissingFieldsRetainV2Defaults() {
        BasicStatsMeta empty = BasicStatsMeta.fromProto(BasicStatsMetaPB.getDefaultInstance());
        GsonEquivalence.assertEquivalent(GsonUtils.GSON.fromJson("{}", BasicStatsMeta.class), empty);
        assertEquals(LocalDateTime.MIN, empty.getTabletStatsReportTime());
        assertNotNull(getField(empty, "columnStatsMetaMap"));
        assertTrue(empty.getColumns().isEmpty());
    }

    @Test
    public void testBasicNullAndEmptyCollections() {
        BasicStatsMeta source = basic(20);
        for (String field : List.of("columns", "properties", "type", "updateTime", "tabletStatsReportTime",
                "columnStatsMetaMap")) {
            setField(source, field, null);
        }
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, BasicStatsMeta.class),
                BasicStatsMeta.fromProto(source.toProto()));
        setField(source, "columns", new ArrayList<>());
        setField(source, "properties", new HashMap<>());
        setField(source, "columnStatsMetaMap", new HashMap<>());
        BasicStatsMeta loaded = BasicStatsMeta.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, BasicStatsMeta.class), loaded);
        assertNotNull(loaded.getProperties());
    }

    @Test
    public void testBasicColumnMapPreservesStoredKey() {
        BasicStatsMeta source = basic(20);
        Map<String, ColumnStatsMeta> columns = new HashMap<>();
        columns.put("stored-key", column("actual-name"));
        columns.put("absent", null);
        setField(source, "columnStatsMetaMap", columns);
        BasicStatsMeta loaded = BasicStatsMeta.fromProto(source.toProto());
        assertEquals(Set.of("legacy", "stored-key"), loaded.getAnalyzedColumns().keySet());
        assertEquals("actual-name", loaded.getAnalyzedColumns().get("stored-key").getColumnName());
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, BasicStatsMeta.class), loaded);
    }

    @Test
    public void testHistogram() throws Exception {
        HistogramStatsMeta source = histogram();
        HistogramStatsMeta loaded = HistogramStatsMeta.fromProto(
                HistogramStatsMetaPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, HistogramStatsMeta.class), loaded);
        assertEquals(10, loaded.getDbId());
        assertEquals(20, loaded.getTableId());
        assertEquals("a", loaded.getColumn());
        assertEquals("64", loaded.getProperties().get("buckets"));
    }

    @Test
    public void testExternalBasic() throws Exception {
        ExternalBasicStatsMeta source = externalBasic();
        ExternalBasicStatsMeta loaded = ExternalBasicStatsMeta.fromProto(
                ExternalBasicStatsMetaPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, ExternalBasicStatsMeta.class), loaded);
        assertEquals("hive.db.table.123", loaded.getTableUUID());
        assertEquals(Set.of("a"), loaded.getColumnStatsMetaMap().keySet());
        assertEquals(23, loaded.getColumnStatsMeta("a").getAllPartitionSize());
        loaded.getColumnStatsMetaMap().clear();
        assertEquals(1, source.getColumnStatsMetaMap().size());
    }

    @Test
    public void testExternalBasicMissingFields() {
        ExternalBasicStatsMeta loaded = ExternalBasicStatsMeta.fromProto(ExternalBasicStatsMetaPB.getDefaultInstance());
        GsonEquivalence.assertEquivalent(GsonUtils.GSON.fromJson("{}", ExternalBasicStatsMeta.class), loaded);
        assertNotNull(loaded.getColumnStatsMetaMap());
        assertNull(loaded.getTableUUID());
    }

    @Test
    public void testExternalHistogram() throws Exception {
        ExternalHistogramStatsMeta source = externalHistogram();
        ExternalHistogramStatsMeta loaded = ExternalHistogramStatsMeta.fromProto(
                ExternalHistogramStatsMetaPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, ExternalHistogramStatsMeta.class), loaded);
        assertEquals("hive", loaded.getCatalogName());
        assertEquals("db", loaded.getDbName());
        assertEquals("table", loaded.getTableName());
        assertEquals("hive.db.table.123", loaded.getTableUUID());
    }

    @Test
    public void testMultiColumnCompositeKeyFields() throws Exception {
        MultiColumnStatsMeta source = multiColumn();
        MultiColumnStatsMeta loaded = MultiColumnStatsMeta.fromProto(
                MultiColumnStatsMetaPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, MultiColumnStatsMeta.class), loaded);
        assertEquals(Set.of(2, 4), loaded.getColumnIds());
        assertEquals(List.of(StatisticsType.MCDISTINCT), loaded.getStatsTypes());
        loaded.getColumnIds().add(9);
        loaded.getStatsTypes().clear();
        assertEquals(Set.of(2, 4), source.getColumnIds());
        assertEquals(1, source.getStatsTypes().size());
    }

    @Test
    public void testEveryStatisticsType() {
        MultiColumnStatsMeta source = multiColumn();
        setField(source, "statisticsTypes", Arrays.asList(StatisticsType.values()));
        assertEquals(Arrays.asList(StatisticsType.values()), MultiColumnStatsMeta.fromProto(source.toProto()).getStatsTypes());
    }

    @Test
    public void testNullAndEmptyMultiColumnFields() {
        MultiColumnStatsMeta source = new MultiColumnStatsMeta(1, 2, null, null, null, null, null);
        MultiColumnStatsMeta loaded = MultiColumnStatsMeta.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, MultiColumnStatsMeta.class), loaded);
        assertNull(loaded.getColumnIds());
        assertNull(loaded.getStatsTypes());
        source = new MultiColumnStatsMeta(1, 2, Set.of(), null, List.of(), null, Map.of());
        loaded = MultiColumnStatsMeta.fromProto(source.toProto());
        assertTrue(loaded.getColumnIds().isEmpty());
        assertTrue(loaded.getStatsTypes().isEmpty());
    }

    @Test
    public void testHistogramNullsAndMinTime() {
        HistogramStatsMeta histogram = new HistogramStatsMeta(1, 2, null, null, LocalDateTime.MIN, null);
        ExternalHistogramStatsMeta external = new ExternalHistogramStatsMeta(null, null, null, null,
                null, LocalDateTime.MIN, null);
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(histogram, HistogramStatsMeta.class),
                HistogramStatsMeta.fromProto(histogram.toProto()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(external, ExternalHistogramStatsMeta.class),
                ExternalHistogramStatsMeta.fromProto(external.toProto()));
        assertEquals(LocalDateTime.MIN, ExternalHistogramStatsMeta.fromProto(external.toProto()).getUpdateTime());
    }

    @Test
    public void testUnknownFieldsAreIgnored() {
        BasicStatsMeta source = basic(20);
        BasicStatsMetaPB withUnknown = source.toProto().toBuilder()
                .setUnknownFields(StatisticsSecurityTestSupport.unknownFields()).build();
        GsonEquivalence.assertEquivalent(BasicStatsMeta.fromProto(source.toProto()), BasicStatsMeta.fromProto(withUnknown));
        MultiColumnStatsMeta multi = multiColumn();
        GsonEquivalence.assertEquivalent(MultiColumnStatsMeta.fromProto(multi.toProto()), MultiColumnStatsMeta.fromProto(
                multi.toProto().toBuilder().setUnknownFields(StatisticsSecurityTestSupport.unknownFields()).build()));
    }

    @Test
    public void testInvalidEnumAndTime() {
        assertThrows(IllegalArgumentException.class, () -> ColumnStatsMeta.fromProto(
                column("a").toProto().toBuilder().setTypeValue(999).build()));
        assertThrows(IllegalArgumentException.class, () -> BasicStatsMeta.fromProto(
                basic(20).toProto().toBuilder().setTabletStatsReportTime(Long.MAX_VALUE).build()));
        assertThrows(IllegalArgumentException.class, () -> ExternalBasicStatsMeta.fromProto(
                externalBasic().toProto().toBuilder().setTypeValue(999).build()));
        assertThrows(IllegalArgumentException.class, () -> HistogramStatsMeta.fromProto(
                histogram().toProto().toBuilder().setTypeValue(999).build()));
        assertThrows(IllegalArgumentException.class, () -> ExternalHistogramStatsMeta.fromProto(
                externalHistogram().toProto().toBuilder().setUpdateTime(Long.MIN_VALUE).build()));
        assertThrows(IllegalArgumentException.class, () -> MultiColumnStatsMeta.fromProto(
                multiColumn().toProto().toBuilder().setStatisticsTypes(
                        StatisticsTypeListPB.newBuilder().addValuesValue(999)).build()));
    }
}
