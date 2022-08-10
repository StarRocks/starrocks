// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/common/util/RuntimeProfileTest.java

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

package com.starrocks.common.util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.thrift.TCounter;
import com.starrocks.thrift.TRuntimeProfileNode;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TUnit;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class RuntimeProfileTest {

    @Test
    public void testSortChildren() {
        RuntimeProfile profile = new RuntimeProfile("profile");
        // init profile
        RuntimeProfile profile1 = new RuntimeProfile("profile1");
        RuntimeProfile profile2 = new RuntimeProfile("profile2");
        RuntimeProfile profile3 = new RuntimeProfile("profile3");
        profile1.getCounterTotalTime().setValue(1);
        profile2.getCounterTotalTime().setValue(3);
        profile3.getCounterTotalTime().setValue(2);
        profile.addChild(profile1);
        profile.addChild(profile2);
        profile.addChild(profile3);
        // compare
        profile.sortChildren();
        // check result
        long time0 = profile.getChildList().get(0).first.getCounterTotalTime().getValue();
        long time1 = profile.getChildList().get(1).first.getCounterTotalTime().getValue();
        long time2 = profile.getChildList().get(2).first.getCounterTotalTime().getValue();

        Assert.assertEquals(3, time0);
        Assert.assertEquals(2, time1);
        Assert.assertEquals(1, time2);
    }

    @Test
    public void testInfoStrings() {
        RuntimeProfile profile = new RuntimeProfile("profileName");

        // not exists key
        Assert.assertNull(profile.getInfoString("key"));
        // normal add and get
        profile.addInfoString("key", "value");
        String value = profile.getInfoString("key");
        Assert.assertNotNull(value);
        Assert.assertEquals(value, "value");
        // from thrift to profile and first update
        TRuntimeProfileTree tprofileTree = new TRuntimeProfileTree();
        TRuntimeProfileNode tnode = new TRuntimeProfileNode();
        tprofileTree.addToNodes(tnode);
        tnode.info_strings = new HashMap<String, String>();
        tnode.info_strings.put("key", "value2");
        tnode.info_strings.put("key3", "value3");
        tnode.info_strings_display_order = new ArrayList<String>();
        tnode.info_strings_display_order.add("key");
        tnode.info_strings_display_order.add("key3");

        profile.update(tprofileTree);
        Assert.assertEquals(profile.getInfoString("key"), "value2");
        Assert.assertEquals(profile.getInfoString("key3"), "value3");
        // second update
        tnode.info_strings.put("key", "value4");

        profile.update(tprofileTree);
        Assert.assertEquals(profile.getInfoString("key"), "value4");

        StringBuilder builder = new StringBuilder();
        profile.prettyPrint(builder, "");
        Assert.assertEquals(builder.toString(),
                "profileName:\n   - key: value4\n   - key3: value3\n");
    }

    @Test
    public void testCounter() {
        RuntimeProfile profile = new RuntimeProfile();
        profile.addCounter("key", TUnit.UNIT);
        Assert.assertNotNull(profile.getCounterMap().get("key"));
        Assert.assertNull(profile.getCounterMap().get("key2"));
        profile.getCounterMap().get("key").setValue(1);
        Assert.assertEquals(profile.getCounterMap().get("key").getValue(), 1);
    }

    @Test
    public void testUpdate() throws IOException {
        RuntimeProfile profile = new RuntimeProfile("REAL_ROOT");
        /*  the profile tree
         *                      ROOT(time=5s info[key=value])
         *                  A(time=2s)            B(time=1s info[BInfo1=BValu1;BInfo2=BValue2])
         *       A_SON(time=10ms counter[counterA1=1; counterA2=2; counterA1Son=3])
         */
        TRuntimeProfileTree tprofileTree = new TRuntimeProfileTree();
        TRuntimeProfileNode tnodeRoot = new TRuntimeProfileNode();
        TRuntimeProfileNode tnodeA = new TRuntimeProfileNode();
        TRuntimeProfileNode tnodeB = new TRuntimeProfileNode();
        TRuntimeProfileNode tnodeASon = new TRuntimeProfileNode();
        tnodeRoot.num_children = 2;
        tnodeA.num_children = 1;
        tnodeASon.num_children = 0;
        tnodeB.num_children = 0;
        tprofileTree.addToNodes(tnodeRoot);
        tprofileTree.addToNodes(tnodeA);
        tprofileTree.addToNodes(tnodeASon);
        tprofileTree.addToNodes(tnodeB);
        tnodeRoot.info_strings = new HashMap<String, String>();
        tnodeRoot.info_strings.put("key", "value");
        tnodeRoot.info_strings_display_order = new ArrayList<String>();
        tnodeRoot.info_strings_display_order.add("key");
        tnodeRoot.counters = Lists.newArrayList();
        tnodeA.counters = Lists.newArrayList();
        tnodeB.counters = Lists.newArrayList();
        tnodeASon.counters = Lists.newArrayList();

        tnodeRoot.counters.add(new TCounter("TotalTime", TUnit.TIME_NS, 3000000000L));
        tnodeA.counters.add(new TCounter("TotalTime", TUnit.TIME_NS, 1000000000L));
        tnodeB.counters.add(new TCounter("TotalTime", TUnit.TIME_NS, 1000000000L));
        tnodeASon.counters.add(new TCounter("TotalTime", TUnit.TIME_NS, 10000000));
        tnodeASon.counters.add(new TCounter("counterA1", TUnit.UNIT, 1));
        tnodeASon.counters.add(new TCounter("counterA2", TUnit.BYTES, 1234567L));
        tnodeASon.counters.add(new TCounter("counterA1Son", TUnit.UNIT, 3));
        tnodeASon.child_counters_map = Maps.newHashMap();

        Set<String> set1 = Sets.newHashSet();
        set1.add("counterA1");
        set1.add("counterA2");
        tnodeASon.child_counters_map.put("", set1);
        Set<String> set2 = Sets.newHashSet();
        set2.add("counterA1Son");
        tnodeASon.child_counters_map.put("counterA1", set2);
        tnodeB.info_strings = Maps.newHashMap();
        tnodeB.info_strings_display_order = Lists.newArrayList();
        tnodeB.info_strings.put("BInfo1", "BValue1");
        tnodeB.info_strings.put("BInfo2", "BValue2");
        tnodeB.info_strings_display_order.add("BInfo2");
        tnodeB.info_strings_display_order.add("BInfo1");
        tnodeRoot.indent = true;
        tnodeA.indent = true;
        tnodeB.indent = true;
        tnodeASon.indent = true;
        tnodeRoot.name = "ROOT";
        tnodeA.name = "A";
        tnodeB.name = "B";
        tnodeASon.name = "ASON";

        profile.update(tprofileTree);
        StringBuilder builder = new StringBuilder();
        profile.computeTimeInProfile();
        profile.prettyPrint(builder, "");
    }

    @Test
    public void testMergeIsomorphicProfiles1() {
        List<RuntimeProfile> profiles = Lists.newArrayList();

        RuntimeProfile profile1 = new RuntimeProfile("profile");
        {
            Counter time1 = profile1.addCounter("time1", TUnit.TIME_NS);
            time1.setValue(2000000000L);
            Counter time2 = profile1.addCounter("time2", TUnit.TIME_NS);
            time2.setValue(0);

            Counter count1 = profile1.addCounter("count1", TUnit.UNIT);
            count1.setValue(1);

            profiles.add(profile1);
        }

        RuntimeProfile profile2 = new RuntimeProfile("profile");
        {
            Counter time1 = profile2.addCounter("time1", TUnit.TIME_NS);
            time1.setValue(2000000000L);
            Counter time2 = profile2.addCounter("time2", TUnit.TIME_NS);
            time2.setValue(2000000000L);

            Counter count1 = profile2.addCounter("count1", TUnit.UNIT);
            count1.setValue(1);

            profiles.add(profile2);
        }

        RuntimeProfile.mergeIsomorphicProfiles(profiles);

        RuntimeProfile mergedProfile = profiles.get(0);
        Counter mergedTime1 = mergedProfile.getCounter("time1");
        Assert.assertEquals(2000000000L, mergedTime1.getValue());
        Counter mergedMinOfTime1 = mergedProfile.getCounter("__MIN_OF_time1");
        Counter mergedMaxOfTime1 = mergedProfile.getCounter("__MAX_OF_time1");
        Assert.assertNull(mergedMinOfTime1);
        Assert.assertNull(mergedMaxOfTime1);

        Counter mergedTime2 = mergedProfile.getCounter("time2");
        Assert.assertEquals(1000000000L, mergedTime2.getValue());
        Counter mergedMinOfTime2 = mergedProfile.getCounter("__MIN_OF_time2");
        Counter mergedMaxOfTime2 = mergedProfile.getCounter("__MAX_OF_time2");
        Assert.assertNotNull(mergedMinOfTime2);
        Assert.assertNotNull(mergedMaxOfTime2);
        Assert.assertEquals(0, mergedMinOfTime2.getValue());
        Assert.assertEquals(2000000000L, mergedMaxOfTime2.getValue());

        Counter mergedCount1 = mergedProfile.getCounter("count1");
        Assert.assertEquals(2, mergedCount1.getValue());
        Counter mergedMinOfCount1 = mergedProfile.getCounter("__MIN_OF_count1");
        Counter mergedMaxOfCount1 = mergedProfile.getCounter("__MAX_OF_count1");
        Assert.assertNotNull(mergedMinOfCount1);
        Assert.assertNotNull(mergedMaxOfCount1);
        Assert.assertEquals(1, mergedMinOfCount1.getValue());
        Assert.assertEquals(1, mergedMaxOfCount1.getValue());
    }

    @Test
    public void testMergeIsomorphicProfiles2() {
        List<RuntimeProfile> profiles = Lists.newArrayList();

        RuntimeProfile profile1 = new RuntimeProfile("profile");
        {
            Counter time1 = profile1.addCounter("time1", TUnit.TIME_NS);
            time1.setValue(2000000000L);
            Counter minOfTime1 = profile1.addCounter("__MIN_OF_time1", TUnit.TIME_NS, "time1");
            minOfTime1.setValue(1500000000L);
            Counter maxOfTime1 = profile1.addCounter("__MAX_OF_time1", TUnit.TIME_NS, "time1");
            maxOfTime1.setValue(5000000000L);

            Counter count1 = profile1.addCounter("count1", TUnit.UNIT);
            count1.setValue(6);
            Counter minOfCount1 = profile1.addCounter("__MIN_OF_count1", TUnit.UNIT, "count1");
            minOfCount1.setValue(1);
            Counter maxOfCount1 = profile1.addCounter("__MAX_OF_count1", TUnit.UNIT, "count1");
            maxOfCount1.setValue(3);

            profiles.add(profile1);
        }

        RuntimeProfile profile2 = new RuntimeProfile("profile");
        {
            Counter time1 = profile2.addCounter("time1", TUnit.TIME_NS);
            time1.setValue(3000000000L);
            Counter minOfTime1 = profile2.addCounter("__MIN_OF_time1", TUnit.TIME_NS, "time1");
            minOfTime1.setValue(100000000L);
            Counter maxOfTime1 = profile2.addCounter("__MAX_OF_time1", TUnit.TIME_NS, "time1");
            maxOfTime1.setValue(4000000000L);

            Counter count1 = profile2.addCounter("count1", TUnit.UNIT);
            count1.setValue(15);
            Counter minOfCount1 = profile2.addCounter("__MIN_OF_count1", TUnit.UNIT, "count1");
            minOfCount1.setValue(4);
            Counter maxOfCount1 = profile2.addCounter("__MAX_OF_count1", TUnit.UNIT, "count1");
            maxOfCount1.setValue(6);

            profiles.add(profile2);
        }

        RuntimeProfile.mergeIsomorphicProfiles(profiles);

        RuntimeProfile mergedProfile = profiles.get(0);
        Counter mergedTime1 = mergedProfile.getCounter("time1");
        Assert.assertEquals(2500000000L, mergedTime1.getValue());
        Counter mergedMinOfTime1 = mergedProfile.getCounter("__MIN_OF_time1");
        Counter mergedMaxOfTime1 = mergedProfile.getCounter("__MAX_OF_time1");
        Assert.assertNotNull(mergedMinOfTime1);
        Assert.assertNotNull(mergedMaxOfTime1);
        Assert.assertEquals(100000000L, mergedMinOfTime1.getValue());
        Assert.assertEquals(5000000000L, mergedMaxOfTime1.getValue());

        Counter mergedCount1 = mergedProfile.getCounter("count1");
        Assert.assertEquals(21, mergedCount1.getValue());
        Counter mergedMinOfCount1 = mergedProfile.getCounter("__MIN_OF_count1");
        Counter mergedMaxOfCount1 = mergedProfile.getCounter("__MAX_OF_count1");
        Assert.assertNotNull(mergedMinOfCount1);
        Assert.assertNotNull(mergedMaxOfCount1);
        Assert.assertEquals(1, mergedMinOfCount1.getValue());
        Assert.assertEquals(6, mergedMaxOfCount1.getValue());
    }

    @Test
    public void testRemoveRedundantMinMaxMetrics() {
        RuntimeProfile profile0 = new RuntimeProfile("profile0");
        RuntimeProfile profile1 = new RuntimeProfile("profile1");
        profile0.addChild(profile1);

        Counter time1 = profile1.addCounter("time1", TUnit.TIME_NS);
        time1.setValue(1500000000L);
        Counter minOfTime1 = profile1.addCounter("__MIN_OF_time1", TUnit.TIME_NS, "time1");
        minOfTime1.setValue(1500000000L);
        Counter maxOfTime1 = profile1.addCounter("__MAX_OF_time1", TUnit.TIME_NS, "time1");
        maxOfTime1.setValue(1500000000L);

        Counter time2 = profile1.addCounter("time2", TUnit.TIME_NS);
        time2.setValue(2000000000L);
        Counter minOfTime2 = profile1.addCounter("__MIN_OF_time2", TUnit.TIME_NS, "time2");
        minOfTime2.setValue(1500000000L);
        Counter maxOfTime2 = profile1.addCounter("__MAX_OF_time2", TUnit.TIME_NS, "time2");
        maxOfTime2.setValue(3000000000L);

        Counter count1 = profile1.addCounter("count1", TUnit.UNIT);
        count1.setValue(6);
        Counter minOfCount1 = profile1.addCounter("__MIN_OF_count1", TUnit.UNIT, "count1");
        minOfCount1.setValue(1);
        Counter maxOfCount1 = profile1.addCounter("__MAX_OF_count1", TUnit.UNIT, "count1");
        maxOfCount1.setValue(1);

        Counter count2 = profile1.addCounter("count2", TUnit.UNIT);
        count2.setValue(6);
        Counter minOfCount2 = profile1.addCounter("__MIN_OF_count2", TUnit.UNIT, "count2");
        minOfCount2.setValue(1);
        Counter maxOfCount2 = profile1.addCounter("__MAX_OF_count2", TUnit.UNIT, "count2");
        maxOfCount2.setValue(3);

        RuntimeProfile.removeRedundantMinMaxMetrics(profile0);

        Assert.assertNull(profile1.getCounter("__MIN_OF_time1"));
        Assert.assertNull(profile1.getCounter("__MAX_OF_time1"));
        Assert.assertNotNull(profile1.getCounter("__MIN_OF_time2"));
        Assert.assertNotNull(profile1.getCounter("__MAX_OF_time2"));
        Assert.assertNull(profile1.getCounter("__MIN_OF_count1"));
        Assert.assertNull(profile1.getCounter("__MAX_OF_count1"));
        Assert.assertNotNull(profile1.getCounter("__MIN_OF_count2"));
        Assert.assertNotNull(profile1.getCounter("__MAX_OF_count2"));
    }
}
