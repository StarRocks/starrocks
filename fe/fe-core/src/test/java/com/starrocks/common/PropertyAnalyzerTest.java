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

import com.google.common.collect.ImmutableMap;
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
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.thrift.TCompressionType;
import com.starrocks.thrift.TPersistentIndexType;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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

    @Test
    public void testEnablePersistentIndex() throws AnalysisException {
        // empty property
        Map<String, String> property = new HashMap<>();
        boolean enablePeristentIndex = PropertyAnalyzer.analyzeEnablePersistentIndex(property);
        Assert.assertEquals(true, enablePeristentIndex);
        // with property
        Map<String, String> property2 = new HashMap<>();
        property2.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "true");
        enablePeristentIndex = PropertyAnalyzer.analyzeEnablePersistentIndex(property2);
        Assert.assertEquals(true, enablePeristentIndex);

        Map<String, String> property3 = new HashMap<>();
        property3.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "false");
        enablePeristentIndex = PropertyAnalyzer.analyzeEnablePersistentIndex(property3);
        Assert.assertEquals(false, enablePeristentIndex);

        // change config
        Config.enable_persistent_index_by_default = false;

        // empty property
        Map<String, String> property4 = new HashMap<>();
        enablePeristentIndex = PropertyAnalyzer.analyzeEnablePersistentIndex(property4);
        Assert.assertEquals(true, enablePeristentIndex);
        // with property
        Map<String, String> property5 = new HashMap<>();
        property5.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "true");
        enablePeristentIndex = PropertyAnalyzer.analyzeEnablePersistentIndex(property5);
        Assert.assertEquals(true, true);

        Map<String, String> property6 = new HashMap<>();
        property6.put(PropertyAnalyzer.PROPERTIES_ENABLE_PERSISTENT_INDEX, "false");
        enablePeristentIndex = PropertyAnalyzer.analyzeEnablePersistentIndex(property6);
        Assert.assertEquals(false, enablePeristentIndex);
        Config.enable_persistent_index_by_default = true;
    }

    @Test
    public void testDefaultTableCompression() throws AnalysisException {
        // No session
        Assert.assertEquals(TCompressionType.LZ4_FRAME, (PropertyAnalyzer.analyzeCompressionType(ImmutableMap.of()).first));

        // Default in the session
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setThreadLocalInfo();
        Assert.assertEquals(TCompressionType.LZ4_FRAME, (PropertyAnalyzer.analyzeCompressionType(ImmutableMap.of()).first));

        // Set in the session
        ctx.getSessionVariable().setDefaultTableCompression("zstd");
        Assert.assertEquals(TCompressionType.ZSTD, (PropertyAnalyzer.analyzeCompressionType(ImmutableMap.of()).first));

        // Set in the property
        Map<String, String> property = new HashMap<>();
        property.put(PropertyAnalyzer.PROPERTIES_COMPRESSION, "zlib");
        Assert.assertEquals(TCompressionType.ZLIB, (PropertyAnalyzer.analyzeCompressionType(property).first));
    }

    @Test
    public void testPersistentIndexType() throws AnalysisException {
        // empty property
        Map<String, String> property = new HashMap<>();
        Assert.assertEquals(TPersistentIndexType.CLOUD_NATIVE, PropertyAnalyzer.analyzePersistentIndexType(property));

        Map<String, String> property2 = new HashMap<>();
        property2.put(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE, "LOCAL");
        Assert.assertEquals(TPersistentIndexType.LOCAL, PropertyAnalyzer.analyzePersistentIndexType(property2));

        Map<String, String> property3 = new HashMap<>();
        property3.put(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE, "local");
        Assert.assertEquals(TPersistentIndexType.LOCAL, PropertyAnalyzer.analyzePersistentIndexType(property3));

        try {
            Map<String, String> property4 = new HashMap<>();
            property4.put(PropertyAnalyzer.PROPERTIES_PERSISTENT_INDEX_TYPE, "LOCAL2");
            TPersistentIndexType type = PropertyAnalyzer.analyzePersistentIndexType(property4);
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid persistent index type: LOCAL2"));
        }
    }

    @Test
    public void testSchemaChangeProperties() throws AnalysisException {
        Map<String, String> props = new HashMap<>();
        props.put(PropertyAnalyzer.PROPERTIES_USE_FAST_SCHEMA_EVOLUTION, "true");
        Assert.assertEquals(PropertyAnalyzer.analyzeUseFastSchemaEvolution(props), true);
    }

    @Test
    public void testSingleLocationLabel() throws AnalysisException {
        String[] testLocs = {"*", "a:*", "bcd_123:*", "123bcd_:val_123", "invalidFormat",
                ":", "aa_123:*", "*:123", "a:b,c:d", "a: b", "  a  :  b  ", "   ", "a:b*"};
        Boolean[] analyzeSuccess = {true, true, true, true, false, false, true, false, false, true, true, false, false};
        int i = 0;
        for (String loc : testLocs) {
            String regex = PropertyAnalyzer.SINGLE_LOCATION_LABEL_REGEX;
            Assert.assertEquals(Pattern.compile(regex).matcher(loc).matches(), analyzeSuccess[i++]);
        }
    }

    @Test
    public void testAnalyzeVersionInfo() {
        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_VERSION_INFO, "1000");
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Does not support the table property \"version_info\" in share data mode, please remove " +
                        "it from the statement", () -> {
                    PropertyAnalyzer.analyzeVersionInfo(properties);
                });
    }
}
