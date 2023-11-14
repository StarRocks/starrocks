// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/PropertyAnalyzerTest.java

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

package com.starrocks.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.thrift.TStorageMedium;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PropertyAnalyzerTest {

    @Test
    public void testBfColumns() throws AnalysisException {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", Type.INT));
        columns.add(new Column("k2", Type.TINYINT));
        columns.add(new Column("v1", Type.VARCHAR, false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("v2", Type.BIGINT, false, AggregateType.SUM, "0", ""));
        columns.get(0).setIsKey(true);
        columns.get(1).setIsKey(true);

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k1");

        Set<String> bfColumns = PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, false);
        Assert.assertEquals(Sets.newHashSet("k1"), bfColumns);
    }

    private void assertBloomFilterNotSupport(Map<String, String> properties, List<Column> columns, String columnName) {
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, columnName);
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, false);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage(),
                    e.getMessage().contains("Invalid bloom filter column '" + columnName + "'"));
        }
    }

    @Test
    public void testBfColumnsError() {
        List<Column> columns = Lists.newArrayList();
        columns.add(new Column("k1", Type.INT));
        columns.add(new Column("k2", Type.TINYINT));
        columns.add(new Column("k3", Type.BOOLEAN));
        columns.add(new Column("v1", Type.VARCHAR, false, AggregateType.REPLACE, "", ""));
        columns.add(new Column("v2", Type.BIGINT, false, AggregateType.SUM, "0", ""));
        columns.add(new Column("kjson", Type.JSON));
        columns.add(new Column("khll", Type.HLL));
        columns.get(0).setIsKey(true);
        columns.get(1).setIsKey(true);

        Map<String, String> properties = Maps.newHashMap();

        // no bf columns
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "");
        try {
            Assert.assertEquals(Sets.newHashSet(),
                    PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, false));
        } catch (AnalysisException e) {
            Assert.fail();
        }

        // k4 not exist
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k4");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, false);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid bloom filter column 'k4'"));
        }

        // not supported
        assertBloomFilterNotSupport(properties, columns, "k2");
        assertBloomFilterNotSupport(properties, columns, "k3");
        assertBloomFilterNotSupport(properties, columns, "kjson");
        assertBloomFilterNotSupport(properties, columns, "khll");

        // not replace value
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "v2");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, false);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Bloom filter index only used in"));
        }

        // reduplicated column
        properties.put(PropertyAnalyzer.PROPERTIES_BF_COLUMNS, "k1,K1");
        try {
            PropertyAnalyzer.analyzeBloomFilterColumns(properties, columns, false);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Duplicate bloom filter column 'K1'"));
        }
    }

    @Test
    public void testBfFpp() throws AnalysisException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_BF_FPP, "0.05");
        Assert.assertEquals(0.05, PropertyAnalyzer.analyzeBloomFilterFpp(properties), 0.0001);
    }

    @Test
    public void testStorageMedium() throws AnalysisException {
        long tomorrowTs = System.currentTimeMillis() / 1000 + 86400;
        String tomorrowTimeStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(tomorrowTs * 1000);

        Map<String, String> properties = Maps.newHashMap();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME, tomorrowTimeStr);
        DataProperty dataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties, new DataProperty(TStorageMedium.SSD), false);
        // avoid UT fail because time zone different
        DateLiteral dateLiteral = new DateLiteral(tomorrowTimeStr, Type.DATETIME);
        Assert.assertEquals(dateLiteral.unixTimestamp(TimeUtils.getTimeZone()), dataProperty.getCooldownTimeMs());

        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "HDD");
        Config.tablet_sched_storage_cooldown_second = 60;
        dataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties1, new DataProperty(TStorageMedium.SSD), false);
        // Use specified storage medium even if SSD is inferred.
        Assert.assertEquals(TStorageMedium.HDD, dataProperty.getStorageMedium());

        Map<String, String> properties2 = Maps.newHashMap();
        Config.tablet_sched_storage_cooldown_second = 60;
        DataProperty defaultDP = new DataProperty(TStorageMedium.SSD, DataProperty.getSsdCooldownTimeMs());
        dataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties2, defaultDP, false);
        // If not specified, the default value should be used
        Assert.assertEquals(dataProperty, defaultDP);
    }

    @Test
    public void testCoolDownTime() throws AnalysisException {
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        DataProperty dataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties1, new DataProperty(TStorageMedium.SSD), false);
        // Cooldown is disabled(with maximum cooldown timestamp) by default
        Assert.assertEquals(DataProperty.MAX_COOLDOWN_TIME_MS, dataProperty.getCooldownTimeMs());

        Config.tablet_sched_storage_cooldown_second = -2;
        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        dataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties2, new DataProperty(TStorageMedium.SSD), false);
        Assert.assertEquals(DataProperty.MAX_COOLDOWN_TIME_MS, dataProperty.getCooldownTimeMs());

        Config.tablet_sched_storage_cooldown_second = 253402271999L;
        Map<String, String> properties3 = Maps.newHashMap();
        properties3.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        dataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties3, new DataProperty(TStorageMedium.SSD), false);
        Assert.assertEquals(DataProperty.MAX_COOLDOWN_TIME_MS, dataProperty.getCooldownTimeMs());

        Map<String, String> properties4 = Maps.newHashMap();
        properties4.put(PropertyAnalyzer.PROPERTIES_STORAGE_MEDIUM, "SSD");
        Config.tablet_sched_storage_cooldown_second = 600;
        long start = System.currentTimeMillis();
        dataProperty =
                PropertyAnalyzer.analyzeDataProperty(properties4, new DataProperty(TStorageMedium.SSD), false);
        long end = System.currentTimeMillis();
        Assert.assertTrue(dataProperty.getCooldownTimeMs() >= start + 600 * 1000L &&
                dataProperty.getCooldownTimeMs() <= end + 600 * 1000L);
    }
}
