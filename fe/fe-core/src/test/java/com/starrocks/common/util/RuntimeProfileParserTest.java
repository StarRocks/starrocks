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
package com.starrocks.common.util;

import com.starrocks.thrift.TUnit;
import org.junit.Assert;
import org.junit.Test;
import org.sparkproject.guava.collect.Maps;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

public class RuntimeProfileParserTest {

    private static void addCounter(RuntimeProfile profile, String name, TUnit type, long value) {
        Counter counter = profile.addCounter(name, type, null);
        counter.setValue(value);
    }

    private static void addCounter(RuntimeProfile profile, String name, TUnit type, long value, String parent) {
        Counter counter = profile.addCounter(name, type, null, parent);
        counter.setValue(value);
    }

    private static String getContent(RuntimeProfile profile) {
        StringBuilder builder = new StringBuilder();
        profile.prettyPrint(builder, "");
        return builder.toString();
    }

    private static Map<String, Integer> getLineCounter(String content) {
        Map<String, Integer> lineCounter = Maps.newHashMap();
        BufferedReader reader = new BufferedReader(new StringReader(content));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                lineCounter.merge(line, 1, Integer::sum);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return lineCounter;
    }

    private static void checkParser(RuntimeProfile profile) {
        final String originalContent = getContent(profile);
        RuntimeProfile rebuildProfile = RuntimeProfileParser.parseFrom(originalContent);
        Assert.assertNotNull(rebuildProfile);
        final String rebuildContent = getContent(rebuildProfile);

        Map<String, Integer> originalLineCounter = getLineCounter(originalContent);
        Map<String, Integer> rebuildLineCounter = getLineCounter(rebuildContent);
        Assert.assertEquals(originalLineCounter, rebuildLineCounter);
    }

    @Test
    public void testInfoString() {
        RuntimeProfile profile = new RuntimeProfile("level 1");

        profile.addInfoString("info1", "content1");
        profile.addInfoString("info2", "content2");
        profile.addInfoString("info3", "content3");
        profile.addInfoString("info4", "content4");
        profile.addInfoString("info5", "content5");
        profile.addInfoString("info6", "100000000");

        checkParser(profile);
    }

    @Test
    public void testTimeNs() {
        RuntimeProfile profile = new RuntimeProfile("level 1");

        addCounter(profile, "timer1", TUnit.TIME_NS, 0);
        addCounter(profile, "timer2", TUnit.TIME_NS, 50);
        addCounter(profile, "timer3", TUnit.TIME_NS, DebugUtil.THOUSAND);
        addCounter(profile, "timer4", TUnit.TIME_NS, 50L * DebugUtil.THOUSAND);
        addCounter(profile, "timer5", TUnit.TIME_NS, DebugUtil.MILLION);
        addCounter(profile, "timer6", TUnit.TIME_NS, 50L * DebugUtil.MILLION);
        addCounter(profile, "timer7", TUnit.TIME_NS, DebugUtil.BILLION);
        addCounter(profile, "timer8", TUnit.TIME_NS, 30L * DebugUtil.BILLION);
        addCounter(profile, "timer9", TUnit.TIME_NS, 90L * DebugUtil.BILLION);
        addCounter(profile, "timer10", TUnit.TIME_NS, 1800L * DebugUtil.BILLION);
        addCounter(profile, "timer11", TUnit.TIME_NS, 7200L * DebugUtil.BILLION);
        addCounter(profile, "timer12", TUnit.TIME_NS, 9000L * DebugUtil.BILLION);

        checkParser(profile);
    }

    @Test
    public void testUnit() {
        RuntimeProfile profile = new RuntimeProfile("level 1");

        addCounter(profile, "unit1", TUnit.UNIT, 0);
        addCounter(profile, "unit2", TUnit.UNIT, 50);
        addCounter(profile, "unit3", TUnit.UNIT, DebugUtil.THOUSAND);
        addCounter(profile, "unit4", TUnit.UNIT, 50L * DebugUtil.THOUSAND);
        addCounter(profile, "unit5", TUnit.UNIT, DebugUtil.MILLION);
        addCounter(profile, "unit6", TUnit.UNIT, 50L * DebugUtil.MILLION);
        addCounter(profile, "unit7", TUnit.UNIT, DebugUtil.BILLION);
        addCounter(profile, "unit8", TUnit.UNIT, 50L * DebugUtil.BILLION);
        addCounter(profile, "unit9", TUnit.UNIT, 50L * DebugUtil.BILLION + 500L * DebugUtil.MILLION);

        checkParser(profile);
    }

    @Test
    public void testUnitPerSec() {
        RuntimeProfile profile = new RuntimeProfile("level 1");

        addCounter(profile, "unitPerSec1", TUnit.UNIT_PER_SECOND, 0);
        addCounter(profile, "unitPerSec2", TUnit.UNIT_PER_SECOND, 50);
        addCounter(profile, "unitPerSec3", TUnit.UNIT_PER_SECOND, DebugUtil.THOUSAND);
        addCounter(profile, "unitPerSec4", TUnit.UNIT_PER_SECOND, 50L * DebugUtil.THOUSAND);
        addCounter(profile, "unitPerSec5", TUnit.UNIT_PER_SECOND, DebugUtil.MILLION);
        addCounter(profile, "unitPerSec6", TUnit.UNIT_PER_SECOND, 50L * DebugUtil.MILLION);
        addCounter(profile, "unitPerSec7", TUnit.UNIT_PER_SECOND, DebugUtil.BILLION);
        addCounter(profile, "unitPerSec8", TUnit.UNIT_PER_SECOND, 50L * DebugUtil.BILLION);

        checkParser(profile);
    }

    @Test
    public void testBytes() {
        RuntimeProfile profile = new RuntimeProfile("level 1");

        addCounter(profile, "byte1", TUnit.BYTES, 0);
        addCounter(profile, "byte2", TUnit.BYTES, 50);
        addCounter(profile, "byte3", TUnit.BYTES, DebugUtil.KILOBYTE);
        addCounter(profile, "byte4", TUnit.BYTES, 50L * DebugUtil.KILOBYTE);
        addCounter(profile, "byte5", TUnit.BYTES, DebugUtil.MEGABYTE);
        addCounter(profile, "byte6", TUnit.BYTES, 50L * DebugUtil.MEGABYTE);
        addCounter(profile, "byte7", TUnit.BYTES, DebugUtil.GIGABYTE);
        addCounter(profile, "byte8", TUnit.BYTES, 50L * DebugUtil.GIGABYTE);
        addCounter(profile, "byte9", TUnit.BYTES, DebugUtil.TERABYTE);
        addCounter(profile, "byte10", TUnit.BYTES, 50L * DebugUtil.TERABYTE);

        checkParser(profile);
    }

    @Test
    public void testBytesPerSec() {
        RuntimeProfile profile = new RuntimeProfile("level 1");

        addCounter(profile, "bytePerSec1", TUnit.BYTES_PER_SECOND, 0);
        addCounter(profile, "bytePerSec2", TUnit.BYTES_PER_SECOND, 50);
        addCounter(profile, "bytePerSec3", TUnit.BYTES_PER_SECOND, DebugUtil.KILOBYTE);
        addCounter(profile, "bytePerSec4", TUnit.BYTES_PER_SECOND, 50L * DebugUtil.KILOBYTE);
        addCounter(profile, "bytePerSec5", TUnit.BYTES_PER_SECOND, DebugUtil.MEGABYTE);
        addCounter(profile, "bytePerSec6", TUnit.BYTES_PER_SECOND, 50L * DebugUtil.MEGABYTE);
        addCounter(profile, "bytePerSec7", TUnit.BYTES_PER_SECOND, DebugUtil.GIGABYTE);
        addCounter(profile, "bytePerSec8", TUnit.BYTES_PER_SECOND, 50L * DebugUtil.GIGABYTE);
        addCounter(profile, "bytePerSec9", TUnit.BYTES_PER_SECOND, DebugUtil.TERABYTE);
        addCounter(profile, "bytePerSec10", TUnit.BYTES_PER_SECOND, 50L * DebugUtil.TERABYTE);

        checkParser(profile);
    }

    @Test
    public void testCounterHierarchy() {
        RuntimeProfile profile = new RuntimeProfile("level 1");

        addCounter(profile, "byte1", TUnit.BYTES, 1);
        addCounter(profile, "byte1-1", TUnit.BYTES, 2, "byte1");
        addCounter(profile, "byte1-2", TUnit.BYTES, 3, "byte1");
        addCounter(profile, "byte1-2-1", TUnit.BYTES, 4, "byte1-2");
        addCounter(profile, "byte1-2-2", TUnit.BYTES, 5, "byte1-2");
        addCounter(profile, "byte1-3", TUnit.BYTES, 6, "byte1");
        addCounter(profile, "byte1-3-1", TUnit.BYTES, 7, "byte1-3");
        addCounter(profile, "byte2", TUnit.BYTES, 8);

        checkParser(profile);
    }

    @Test
    public void testEmptyProfileHierarchy() {
        RuntimeProfile level1 = new RuntimeProfile("level 1");

        RuntimeProfile level2 = new RuntimeProfile("level 2");
        level1.addChild(level2);

        RuntimeProfile level3 = new RuntimeProfile("level 3");
        level2.addChild(level3);

        RuntimeProfile level4 = new RuntimeProfile("level 4");
        level3.addChild(level4);

        RuntimeProfile level32 = new RuntimeProfile("level 3_2");
        level2.addChild(level32);

        RuntimeProfile level42 = new RuntimeProfile("level 4_2");
        level3.addChild(level42);

        RuntimeProfile level22 = new RuntimeProfile("level 2_2");
        level1.addChild(level22);

        checkParser(level1);
    }

    @Test
    public void testProfileHierarchy() {
        RuntimeProfile level1 = new RuntimeProfile("level 1");
        addCounter(level1, "byte1", TUnit.BYTES, 1);

        RuntimeProfile level2 = new RuntimeProfile("level 2");
        addCounter(level2, "byte1", TUnit.BYTES, 2);
        level1.addChild(level2);

        RuntimeProfile level3 = new RuntimeProfile("level 3");
        addCounter(level3, "byte1", TUnit.BYTES, 3);
        level2.addChild(level3);

        RuntimeProfile level4 = new RuntimeProfile("level 4");
        addCounter(level4, "byte1", TUnit.BYTES, 4);
        level3.addChild(level4);

        RuntimeProfile level32 = new RuntimeProfile("level 3_2");
        addCounter(level32, "byte1", TUnit.BYTES, 5);
        level2.addChild(level32);

        RuntimeProfile level42 = new RuntimeProfile("level 4_2");
        addCounter(level42, "byte1", TUnit.BYTES, 6);
        level3.addChild(level42);

        RuntimeProfile level22 = new RuntimeProfile("level 2_2");
        addCounter(level22, "byte1", TUnit.BYTES, 7);
        level1.addChild(level22);

        checkParser(level1);
    }

    @Test
    public void testProfileHierarchy2() {
        RuntimeProfile query = new RuntimeProfile("Query");

        RuntimeProfile summary = new RuntimeProfile("Summary");
        summary.addInfoString("Query Id", "xxx");
        query.addChild(summary);

        RuntimeProfile execution = new RuntimeProfile("Execution");
        addCounter(execution, "QueryAllocatedMemoryUsage", TUnit.BYTES, 1);
        summary.addChild(execution);

        RuntimeProfile pipeline = new RuntimeProfile("Pipeline (id=0)");
        addCounter(pipeline, "ActiveTime", TUnit.TIME_NS, 1);
        addCounter(pipeline, "__MIN_OF_ActiveTime", TUnit.TIME_NS, 2, "ActiveTime");
        addCounter(pipeline, "__MAX_OF_ActiveTime", TUnit.TIME_NS, 3, "ActiveTime");
        addCounter(pipeline, "BlockByInputEmpty", TUnit.TIME_NS, 4);
        addCounter(pipeline, "DriverPrepareTime", TUnit.TIME_NS, 5);
        addCounter(pipeline, "__MIN_OF_DriverPrepareTime", TUnit.TIME_NS, 6, "DriverPrepareTime");
        addCounter(pipeline, "__MAX_OF_DriverPrepareTime", TUnit.TIME_NS, 7, "DriverPrepareTime");
        addCounter(pipeline, "DriverTotalTime", TUnit.TIME_NS, 8);
        addCounter(pipeline, "__MIN_OF_DriverTotalTime", TUnit.TIME_NS, 9, "DriverTotalTime");
        addCounter(pipeline, "__MAX_OF_DriverTotalTime", TUnit.TIME_NS, 10, "DriverTotalTime");
        addCounter(pipeline, "GlobalScheduleCount", TUnit.UNIT, 11);
        addCounter(pipeline, "__MIN_OF_GlobalScheduleCount", TUnit.UNIT, 11, "GlobalScheduleCount");
        addCounter(pipeline, "__MAX_OF_GlobalScheduleCount", TUnit.UNIT, 12, "GlobalScheduleCount");
        addCounter(pipeline, "GlobalScheduleTime", TUnit.TIME_NS, 13);
        addCounter(pipeline, "__MIN_OF_GlobalScheduleTime", TUnit.TIME_NS, 14, "GlobalScheduleTime");
        addCounter(pipeline, "__MAX_OF_GlobalScheduleTime", TUnit.TIME_NS, 15, "GlobalScheduleTime");
        execution.addChild(pipeline);

        checkParser(query);
    }
}