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

package com.starrocks.catalog;

import com.starrocks.common.DdlException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableFunctionTableTest {

    Map<String, String> newProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("path", "fake://some_bucket/some_path/*");
        properties.put("format", "ORC");
        properties.put("columns_from_path", "col_path1, col_path2,   col_path3");
        properties.put("auto_detect_sample_files", "10");
        properties.put("csv.column_separator", ",");
        properties.put("csv.row_delimiter", "\n");
        properties.put("csv.enclose", "\\");
        properties.put("csv.escape", "'");
        properties.put("csv.skip_header", "2");
        properties.put("csv.trim_space", "true");
        return properties;
    }

    @Test
    public void testNormal() {
        Assertions.assertDoesNotThrow(() -> {
            TableFunctionTable table = new TableFunctionTable(newProperties());
            List<Column> schema = table.getFullSchema();
            Assertions.assertEquals(5, schema.size());
            Assertions.assertEquals(new Column("col_int", Type.INT), schema.get(0));
            Assertions.assertEquals(new Column("col_string", Type.VARCHAR), schema.get(1));
            Assertions.assertEquals(new Column("col_path1", ScalarType.createDefaultString(), true), schema.get(2));
            Assertions.assertEquals(new Column("col_path2", ScalarType.createDefaultString(), true), schema.get(3));
            Assertions.assertEquals(new Column("col_path3", ScalarType.createDefaultString(), true), schema.get(4));
        });
    }

    @Test
    public void testGetFileSchema(@Mocked GlobalStateMgr globalStateMgr,
                                  @Mocked SystemInfoService systemInfoService) throws Exception {
        new Expectations() {
            {
                globalStateMgr.getCurrentSystemInfo();
                result = systemInfoService;
                minTimes = 0;

                systemInfoService.getBackendIds(anyBoolean);
                result = new ArrayList<>();
                minTimes = 0;
            }
        };

        TableFunctionTable t = new TableFunctionTable(newProperties());

        Method method = TableFunctionTable.class.getDeclaredMethod("getFileSchema", null);
        method.setAccessible(true);

        try {
            method.invoke(t, null);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("Failed to send proxy request. No alive backends"));
        }

        new MockUp<RunMode>() {
            @Mock
            public RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        try {
            method.invoke(t, null);
        } catch (Exception e) {
            Assert.assertTrue(e.getCause().getMessage().
                    contains("Failed to send proxy request. No alive backends or compute nodes"));
        }

        Backend backend = new Backend(1L, "192.168.1.1", 9050);
        backend.setBrpcPort(8050);

        List<Long> nodeList = new ArrayList<>();
        nodeList.add(1L);

        new Expectations() {
            {
                systemInfoService.getBackendIds(anyBoolean);
                result = nodeList;
                minTimes = 0;

                systemInfoService.getComputeNodeIds(anyBoolean);
                result = new ArrayList<>();
                minTimes = 0;

                systemInfoService.getBackendOrComputeNode(anyLong);
                result = backend;
                minTimes = 0;
            }
        };

        try {
            method.invoke(t, null);
        } catch (Exception e) {
            Assert.assertFalse(false);
        }
    }

    @Test
    public void testProperties() {
        // normal case.
        Assertions.assertDoesNotThrow(() -> {
            TableFunctionTable table = new TableFunctionTable(newProperties());
            Assert.assertEquals("fake://some_bucket/some_path/*", Deencapsulation.getField(table, "path"));
            Assert.assertEquals("ORC", Deencapsulation.getField(table, "format"));
            Assert.assertEquals(Arrays.asList("col_path1", "col_path2", "col_path3"),
                    Deencapsulation.getField(table, "columnsFromPath"));
            Assert.assertEquals(10, (int) Deencapsulation.getField(table, "autoDetectSampleFiles"));
            Assert.assertEquals("\n", table.getCsvRowDelimiter());
            Assert.assertEquals(",", table.getCsvColumnSeparator());
            Assert.assertEquals('\\', table.getCsvEnclose());
            Assert.assertEquals('\'', table.getCsvEscape());
            Assert.assertEquals(2, table.getCsvSkipHeader());
            Assert.assertEquals(true, table.getCsvTrimSpace());
        });

        // abnormal case.
        Assertions.assertThrows(DdlException.class, () -> {
            Map<String, String> properties = newProperties();
            properties.put("auto_detect_sample_files", "not_a_number");
            new TableFunctionTable(properties);
        });
    }
}
