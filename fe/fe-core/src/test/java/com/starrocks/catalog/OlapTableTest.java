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
import com.starrocks.backup.mv.MvRestoreContext;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.common.util.UnitTestUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.ast.expression.LiteralExpr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.threeten.extra.PeriodDuration;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
            tbl.setIndexes(Lists.newArrayList(new Index("index", Lists.newArrayList(ColumnId.create("col")),
                    IndexDef.IndexType.BITMAP, "xxxxxx")));
            System.out.println("orig table id: " + tbl.getId());
            MvId mvId1 = new MvId(db.getId(), 10L);
            tbl.addRelatedMaterializedView(mvId1);
            MvId mvId2 = new MvId(db.getId(), 20L);
            tbl.addRelatedMaterializedView(mvId2);
            MvId mvId3 = new MvId(db.getId(), 30L);
            tbl.addRelatedMaterializedView(mvId3);
            Assertions.assertEquals(Sets.newHashSet(10L, 20L, 30L),
                    tbl.getRelatedMaterializedViews().stream().map(mvId -> mvId.getId()).collect(Collectors.toSet()));
            tbl.removeRelatedMaterializedView(mvId1);
            tbl.removeRelatedMaterializedView(mvId2);
            Assertions.assertEquals(Sets.newHashSet(30L),
                    tbl.getRelatedMaterializedViews().stream().map(mvId -> mvId.getId()).collect(Collectors.toSet()));
            tbl.removeRelatedMaterializedView(mvId3);
            Assertions.assertEquals(Sets.newHashSet(), tbl.getRelatedMaterializedViews());
        }
    }

    @Test
    public void testCopyOnlyForQuery() {
        OlapTable olapTable = new OlapTable();
        olapTable.setHasDelete();

        OlapTable copied = new OlapTable();
        olapTable.copyOnlyForQuery(copied);

        Assertions.assertEquals(olapTable.hasDelete(), copied.hasDelete());
        Assertions.assertEquals(olapTable.hasForbiddenGlobalDict(), copied.hasForbiddenGlobalDict());
        Assertions.assertEquals(olapTable, copied);
    }

    @Test
    public void testFilePathInfo() {
        OlapTable olapTable = new OlapTable();
        Assertions.assertNull(olapTable.getDefaultFilePathInfo());
        Assertions.assertNull(olapTable.getPartitionFilePathInfo(10));
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Assertions.assertNull(olapTable.getDefaultFilePathInfo());
        Assertions.assertNull(olapTable.getPartitionFilePathInfo(10));
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

        OlapTable olapTable = new OlapTable(1, "test", partitionColumns, KeysType.AGG_KEYS,
                (PartitionInfo) rangePartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));

        Partition partition = new Partition(1, 11, "p1", null, null);
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition));

        new MockUp<Range<PartitionKey>>() {
            @Mock
            boolean isConnected(Range<PartitionKey> range) throws Exception {
                throw new Exception("Error");
            }
        };

        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition));
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

        OlapTable olapTable = new OlapTable(1, "test", partitionColumns, KeysType.AGG_KEYS,
                (PartitionInfo) rangePartitionInfo, null);
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));

        Partition partition = new Partition(1, 11, "p1", null, null);
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition));
    }

    @Test
    public void testNullDataCachePartitionDuration() {
        OlapTable olapTable = new OlapTable();
        olapTable.setTableProperty(new TableProperty(new HashMap<>()));
        Assertions.assertNull(olapTable.getTableProperty() == null ? null :
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
        Partition partition1 = new Partition(1L, 11, "p1", null, null);

        // Datacache.partition_duration is not set, cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition1));

        // cache is invalid, because we only take k2 into consideration
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));

        List<String> multiValues2 = new ArrayList<>(Arrays.asList("abcd", LocalDate.now().toString(), "2024-01-01"));
        List<List<String>> multiValuesList2 = new ArrayList<>(Arrays.asList(multiValues2));
        listPartitionInfo.setMultiValues(2L, multiValuesList2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28 hour"));
        Partition partition2 = new Partition(2L, 21, "p2", null, null);

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition2));

        new MockUp<DateUtils>() {
            @Mock
            LocalDateTime parseDatTimeString(String text) throws Exception {
                throw new AnalysisException("Error");
            }
        };
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));
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
        Partition partition1 = new Partition(1L, 11, "p1", null, null);

        // Datacache.partition_duration is not set, cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition1));

        // cache is invalid
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));

        List<String> multiValues2 = new ArrayList<>(Arrays.asList(LocalDate.now().toString()));
        List<List<String>> multiValuesList2 = new ArrayList<>(Arrays.asList(multiValues2));
        listPartitionInfo.setMultiValues(2L, multiValuesList2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28  hour"));
        Partition partition2 = new Partition(2L, 21, "p2", null, null);

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition2));

        new MockUp<DateUtils>() {
            @Mock
            LocalDateTime parseDatTimeString(String text) throws Exception {
                throw new AnalysisException("Error");
            }
        };
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));
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
        Partition partition1 = new Partition(1L, 11, "p1", null, null);

        // Datacache.partition_duration is not set, cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition1));

        // cache is invalid
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));

        List<String> values2 = new ArrayList<>(Arrays.asList(LocalDate.now().toString()));
        listPartitionInfo.setValues(2L, values2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28 hour"));
        Partition partition2 = new Partition(2L, 21, "p2", null, null);

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition2));

        new MockUp<DateUtils>() {
            @Mock
            LocalDateTime parseDatTimeString(String text) throws Exception {
                throw new AnalysisException("Error");
            }
        };
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition1));
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
        Partition partition = new Partition(1L, 11, "p1", null, null);

        // Datacache.partition_duration is not set, cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition));

        // cache is invalid, because we only take k2 into consideration
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));
        Assertions.assertFalse(olapTable.isEnableFillDataCache(partition));

        List<String> multiValues2 = new ArrayList<>(Arrays.asList("abcd",
                DateUtils.formatDateTimeUnix(LocalDateTime.now()), "2024-01-01"));
        List<List<String>> multiValuesList2 = new ArrayList<>(Arrays.asList(multiValues2));
        listPartitionInfo.setMultiValues(2L, multiValuesList2);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("28 hour"));
        Partition partition2 = new Partition(2L, 21, "p2", null, null);

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition2));
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
        Partition partition1 = new Partition(1L, 11, "p1", null, null);
        olapTable.setDataCachePartitionDuration(TimeUtils.parseHumanReadablePeriodOrDuration("25 hour"));

        // cache is valid
        Assertions.assertTrue(olapTable.isEnableFillDataCache(partition1));
    }

    @Test
    public void testGetPhysicalPartitionByName() {
        Database db = UnitTestUtil.createDb(1, 2, 3, 4, 5, 6, 7, KeysType.AGG_KEYS);
        List<Table> tables = db.getTables();
        for (Table table : tables) {
            OlapTable olapTable = (OlapTable) table;
            PhysicalPartition partition = olapTable.getPhysicalPartition("not_existed_name");
            Assertions.assertNull(partition);
        }
    }

    @Test
    public void testGetIndexesBySchema() {
        List<Index> indexesInTable = Lists.newArrayList();
        Column k1 = new Column("k1", new ScalarType(PrimitiveType.VARCHAR), true, null, "", "");
        Column k2 = new Column("k2", new ScalarType(PrimitiveType.DATETIME), true, null, "", "");
        Column k3 = new Column("k3", new ScalarType(PrimitiveType.DATE), true, null, "", "");
        List<Column> schema = new LinkedList<Column>();
        schema.add(k1);
        schema.add(k2);
        schema.add(k3);

        Index index1 = new Index(1L, "index1", Lists.newArrayList(ColumnId.create("k1"), ColumnId.create("k2")),
                                 IndexDef.IndexType.BITMAP, "comment", null);

        Index index2 = new Index(2L, "index2", Lists.newArrayList(ColumnId.create("k2"), ColumnId.create("k3")),
                                 IndexDef.IndexType.BITMAP, "comment", null);

        Index index3 = new Index(3L, "index3", Lists.newArrayList(ColumnId.create("k4")), IndexDef.IndexType.BITMAP,
                                 "comment", null);
        indexesInTable.add(index1);
        indexesInTable.add(index2);
        indexesInTable.add(index3);

        List<Index> result = OlapTable.getIndexesBySchema(indexesInTable, schema);
        Assertions.assertTrue(result.size() == 2);
        Assertions.assertTrue(result.get(0).getIndexName().equals("index1"));
        Assertions.assertTrue(result.get(1).getIndexName().equals("index2"));
    }

    @Test
    public void testGetUniquePropertiesWithFlatJsonConfig() {
        // Test case 1: Flat JSON enabled with all properties set
        OlapTable table1 = new OlapTable();
        TableProperty tableProperty1 = new TableProperty();
        Map<String, String> properties1 = new HashMap<>();
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.1");
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.8");
        properties1.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "50");
        tableProperty1.modifyTableProperties(properties1);
        table1.setTableProperty(tableProperty1);

        Map<String, String> result1 = table1.getUniqueProperties();
        Assertions.assertEquals("true", result1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertEquals("0.1", result1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertEquals("0.8", result1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertEquals("50", result1.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Test case 2: Flat JSON enabled but only some properties set
        OlapTable table2 = new OlapTable();
        TableProperty tableProperty2 = new TableProperty();
        Map<String, String> properties2 = new HashMap<>();
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        properties2.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.2");
        // Don't set sparsity factor and column max
        tableProperty2.modifyTableProperties(properties2);
        table2.setTableProperty(tableProperty2);

        Map<String, String> result2 = table2.getUniqueProperties();
        Assertions.assertEquals("true", result2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertEquals("0.2", result2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertNull(result2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertNull(result2.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Test case 3: Flat JSON disabled - other properties should not be included
        OlapTable table3 = new OlapTable();
        TableProperty tableProperty3 = new TableProperty();
        Map<String, String> properties3 = new HashMap<>();
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "false");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "0.3");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, "0.9");
        properties3.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX, "100");
        tableProperty3.modifyTableProperties(properties3);
        table3.setTableProperty(tableProperty3);

        Map<String, String> result3 = table3.getUniqueProperties();
        Assertions.assertEquals("false", result3.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertNull(result3.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertNull(result3.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertNull(result3.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Test case 4: No flat JSON properties set
        OlapTable table4 = new OlapTable();
        TableProperty tableProperty4 = new TableProperty();
        table4.setTableProperty(tableProperty4);

        Map<String, String> result4 = table4.getUniqueProperties();
        Assertions.assertNull(result4.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertNull(result4.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertNull(result4.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
        Assertions.assertNull(result4.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_COLUMN_MAX));

        // Test case 5: Flat JSON enabled with null/empty values
        OlapTable table5 = new OlapTable();
        TableProperty tableProperty5 = new TableProperty();
        Map<String, String> properties5 = new HashMap<>();
        properties5.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE, "true");
        properties5.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR, "");
        properties5.put(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR, null);
        tableProperty5.modifyTableProperties(properties5);
        table5.setTableProperty(tableProperty5);

        Map<String, String> result5 = table5.getUniqueProperties();
        Assertions.assertEquals("true", result5.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_ENABLE));
        Assertions.assertNull(result5.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_NULL_FACTOR));
        Assertions.assertNull(result5.get(PropertyAnalyzer.PROPERTIES_FLAT_JSON_SPARSITY_FACTOR));
    }

}
