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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/catalog/OlapTableTest.java

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
import com.google.common.collect.Sets;
import com.starrocks.analysis.IndexDef;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.FeConstants;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.io.FastByteArrayOutputStream;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;
import org.threeten.extra.PeriodDuration;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class OlapTableTest {

    @Test
    public void testSetIdForRestore() {
        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7, KeysType.AGG_KEYS);
        List<Table> tables = db.getTables();
        final long id = 0;
        new MockUp<GlobalStateMgr>() {
            @Mock
            long getNextId() {
                return id;
            }
        };

        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            ((OlapTable) table).resetIdsForRestore(GlobalStateMgr.getCurrentState(), db, 3, new MvRestoreContext());
        }
    }

    @Test
    public void testTableWithLocalTablet() throws IOException {
        new MockUp<GlobalStateMgr>() {
            @Mock
            int getCurrentStateJournalVersion() {
                return FeConstants.META_VERSION;
            }
        };

        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7, KeysType.AGG_KEYS);
        List<Table> tables = db.getTables();

        for (Table table : tables) {
            if (table.getType() != TableType.OLAP) {
                continue;
            }
            OlapTable tbl = (OlapTable) table;
            tbl.setIndexes(Lists.newArrayList(new Index("index", Lists.newArrayList("col"),
                    IndexDef.IndexType.BITMAP, "xxxxxx")));
            System.out.println("orig table id: " + tbl.getId());

            FastByteArrayOutputStream byteArrayOutputStream = new FastByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteArrayOutputStream);
            tbl.write(out);

            out.flush();
            out.close();

            DataInputStream in = new DataInputStream(byteArrayOutputStream.getInputStream());
            Table copiedTbl = OlapTable.read(in);
            System.out.println("copied table id: " + copiedTbl.getId());

            Assert.assertTrue(copiedTbl instanceof OlapTable);
            Partition partition = ((OlapTable) copiedTbl).getPartition(3L);
            MaterializedIndex newIndex = partition.getIndex(4L);
            for (Tablet tablet : newIndex.getTablets()) {
                Assert.assertTrue(tablet instanceof LocalTablet);
            }
            MvId mvId1 = new MvId(db.getId(), 10L);
            tbl.addRelatedMaterializedView(mvId1);
            MvId mvId2 = new MvId(db.getId(), 20L);
            tbl.addRelatedMaterializedView(mvId2);
            MvId mvId3 = new MvId(db.getId(), 30L);
            tbl.addRelatedMaterializedView(mvId3);
            Assert.assertEquals(Sets.newHashSet(10L, 20L, 30L),
                    tbl.getRelatedMaterializedViews().stream().map(mvId -> mvId.getId()).collect(Collectors.toSet()));
            tbl.removeRelatedMaterializedView(mvId1);
            tbl.removeRelatedMaterializedView(mvId2);
            Assert.assertEquals(Sets.newHashSet(30L),
                    tbl.getRelatedMaterializedViews().stream().map(mvId -> mvId.getId()).collect(Collectors.toSet()));
            tbl.removeRelatedMaterializedView(mvId3);
            Assert.assertEquals(Sets.newHashSet(), tbl.getRelatedMaterializedViews());
        }
    }

    @Test
    public void testCopyOnlyForQuery() {
        OlapTable olapTable = new OlapTable();
        olapTable.setHasDelete();

        OlapTable copied = new OlapTable();
        olapTable.copyOnlyForQuery(copied);

        Assert.assertEquals(olapTable.hasDelete(), copied.hasDelete());
        Assert.assertEquals(olapTable.hasForbitGlobalDict(), copied.hasForbitGlobalDict());
        Assert.assertEquals(olapTable, copied);
    }

    @Test
    public void testFilePathInfo() {
        OlapTable olapTable = new OlapTable();
        Assert.assertNull(olapTable.getDefaultFilePathInfo());
        Assert.assertNull(olapTable.getPartitionFilePathInfo(10));
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Assert.assertNull(olapTable.getDefaultFilePathInfo());
        Assert.assertNull(olapTable.getPartitionFilePathInfo(10));
    }

    @Test
    public void testMVPartitionDurationTimeUintMismatch1() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);

        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(partitionColumns);
        PeriodDuration duration1 = TimeUtils.parseHumanReadablePeriodOrDuration("2 day");

        PartitionKey p1 = new PartitionKey();
        p1.pushColumn(LiteralExpr.create(LocalDate.now().minus(duration1).toString(), Type.DATE),
                PrimitiveType.DATE);

        PartitionKey p2 = new PartitionKey();
        p2.pushColumn(LiteralExpr.create(LocalDate.now().toString(), Type.DATE), PrimitiveType.DATE);
        rangePartitionInfo.setRange(1, false, Range.openClosed(p1, p2));

        OlapTable olapTable = new OlapTable(1, "test", new ArrayList<>(), KeysType.AGG_KEYS,
                (PartitionInfo) rangePartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));

        Partition partition = new Partition(1, "p1", null, null);
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition));

        new MockUp<Range<PartitionKey>>() {
            @Mock
            boolean isConnected(Range<PartitionKey> range) throws Exception {
                throw new Exception("Error");
            }
        };

        Assert.assertFalse(olapTable.isEnableFillDataCache(partition));
    }

    @Test
    public void testMVPartitionDurationTimeUintMismatch2() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);

        RangePartitionInfo rangePartitionInfo = new RangePartitionInfo(partitionColumns);
        PeriodDuration duration1 = TimeUtils.parseHumanReadablePeriodOrDuration("4 day");
        PeriodDuration duration2 = TimeUtils.parseHumanReadablePeriodOrDuration("2 day");

        PartitionKey p1 = new PartitionKey();
        p1.pushColumn(LiteralExpr.create(LocalDate.now().minus(duration1).toString(), Type.DATE),
                PrimitiveType.DATE);

        PartitionKey p2 = new PartitionKey();
        p2.pushColumn(LiteralExpr.create(LocalDate.now().minus(duration2).toString(), Type.DATE),
                PrimitiveType.DATE);
        rangePartitionInfo.setRange(1, false, Range.openClosed(p1, p2));

        OlapTable olapTable = new OlapTable(1, "test", new ArrayList<>(), KeysType.AGG_KEYS,
                (PartitionInfo) rangePartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));

        Partition partition = new Partition(1, "p1", null, null);
        Assert.assertFalse(olapTable.isEnableFillDataCache(partition));
    }

    @Test
    public void testNullDataCachePartitionDuration() {
        OlapTable olapTable = new OlapTable();
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Assert.assertNull(olapTable.getTableProperty() == null ? null :
                olapTable.getTableProperty().getDataCachePartitionDuration());
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestDateColumn() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionColumns.add(k3);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> multiValues1 = new ArrayList<>(Arrays.asList("abcd", "2023-01-01", "2024-01-01"));
        List<List<String>> multiValuesList1 = new ArrayList<>(Arrays.asList(multiValues1));
        listPartitionInfo.setMultiValues(1L, multiValuesList1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition1 = new Partition(1L, "p1", null, null);

        // Datacache.partition_duration is not set, cache is valid
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition1));

        // cache is invalid, because we only take k2 into consideration
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assert.assertFalse(olapTable.isEnableFillDataCache(partition1));

        List<String> multiValues2 = new ArrayList<>(Arrays.asList("abcd", LocalDate.now().toString(), "2024-01-01"));
        List<List<String>> multiValuesList2 = new ArrayList<>(Arrays.asList(multiValues2));
        listPartitionInfo.setMultiValues(2L, multiValuesList2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28 hour"));
        Partition partition2 = new Partition(2L, "p2", null, null);

        // cache is valid
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition2));

        new MockUp<DateUtils>() {
            @Mock
            LocalDateTime parseDatTimeString(String text) throws Exception {
                throw new AnalysisException("Error");
            }
        };
        Assert.assertFalse(olapTable.isEnableFillDataCache(partition1));
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestSingleDateColumn() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> multiValues1 = new ArrayList<>(Arrays.asList("2023-01-01"));
        List<List<String>> multiValuesList1 = new ArrayList<>(Arrays.asList(multiValues1));
        listPartitionInfo.setMultiValues(1L, multiValuesList1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition1 = new Partition(1L, "p1", null, null);

        // Datacache.partition_duration is not set, cache is valid
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition1));

        // cache is invalid
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assert.assertFalse(olapTable.isEnableFillDataCache(partition1));

        List<String> multiValues2 = new ArrayList<>(Arrays.asList(LocalDate.now().toString()));
        List<List<String>> multiValuesList2 = new ArrayList<>(Arrays.asList(multiValues2));
        listPartitionInfo.setMultiValues(2L, multiValuesList2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28  hour"));
        Partition partition2 = new Partition(2L, "p2", null, null);

        // cache is valid
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition2));

        new MockUp<DateUtils>() {
            @Mock
            LocalDateTime parseDatTimeString(String text) throws Exception {
                throw new AnalysisException("Error");
            }
        };
        Assert.assertFalse(olapTable.isEnableFillDataCache(partition1));
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestIdToValues() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> values1 = new ArrayList<>(Arrays.asList("2023-01-01", "2023-10-01"));
        listPartitionInfo.setValues(1L, values1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition1 = new Partition(1L, "p1", null, null);

        // Datacache.partition_duration is not set, cache is valid
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition1));

        // cache is invalid
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assert.assertFalse(olapTable.isEnableFillDataCache(partition1));

        List<String> values2 = new ArrayList<>(Arrays.asList(LocalDate.now().toString()));
        listPartitionInfo.setValues(2L, values2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28 hour"));
        Partition partition2 = new Partition(2L, "p2", null, null);

        // cache is valid
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition2));

        new MockUp<DateUtils>() {
            @Mock
            LocalDateTime parseDatTimeString(String text) throws Exception {
                throw new AnalysisException("Error");
            }
        };
        Assert.assertFalse(olapTable.isEnableFillDataCache(partition1));
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestDateTimeColumn() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionColumns.add(k3);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> multiValues1 = new ArrayList<>(Arrays.asList("abcd", "2023-01-01 10:00:00", "2024-01-01"));
        List<List<String>> multiValuesList1 = new ArrayList<>(Arrays.asList(multiValues1));
        listPartitionInfo.setMultiValues(1L, multiValuesList1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition = new Partition(1L, "p1", null, null);

        // Datacache.partition_duration is not set, cache is valid
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition));

        // cache is invalid, because we only take k2 into consideration
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assert.assertFalse(olapTable.isEnableFillDataCache(partition));

        List<String> multiValues2 = new ArrayList<>(Arrays.asList("abcd",
                DateUtils.formatDateTimeUnix(LocalDateTime.now()), "2024-01-01"));
        List<List<String>> multiValuesList2 = new ArrayList<>(Arrays.asList(multiValues2));
        listPartitionInfo.setMultiValues(2L, multiValuesList2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28 hour"));
        Partition partition2 = new Partition(2L, "p2", null, null);

        // cache is valid
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition2));
    }

    @Test
    public void testListPartitionSupportPeriodDurationTestNoneDateTypeColumn() throws AnalysisException {
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.INT), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.DOUBLE), true, null, "", "");
        List<Column> partitionColumns = new LinkedList<Column>();
        partitionColumns.add(k1);
        partitionColumns.add(k2);
        partitionColumns.add(k3);

        ListPartitionInfo listPartitionInfo = new ListPartitionInfo(PartitionType.LIST, partitionColumns);
        List<String> multiValues1 = new ArrayList<>(Arrays.asList("abcd", "1", "1.1"));
        List<List<String>> multiValuesList1 = new ArrayList<>(Arrays.asList(multiValues1));
        listPartitionInfo.setMultiValues(1L, multiValuesList1);
        OlapTable olapTable = new OlapTable(1L, "tb1", partitionColumns, null, (PartitionInfo) listPartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Partition partition1 = new Partition(1L, "p1", null, null);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));

        // cache is valid
        Assert.assertTrue(olapTable.isEnableFillDataCache(partition1));
    }
}