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
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.starrocks.thrift.TCounter;
import com.starrocks.thrift.TCounterAggregateType;
import com.starrocks.thrift.TCounterMergeType;
import com.starrocks.thrift.TCounterStrategy;
import com.starrocks.thrift.TRuntimeProfileNode;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TUnit;
import org.junit.Assert;
import org.junit.Test;

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
        profile.addCounter("key", TUnit.UNIT, null);
        Assert.assertNotNull(profile.getCounterMap().get("key"));
        Assert.assertNull(profile.getCounterMap().get("key2"));
        profile.getCounterMap().get("key").setValue(1);
        Assert.assertEquals(profile.getCounterMap().get("key").getValue(), 1);
    }

    @Test
    public void testUpdate() {
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
            Counter time1 = profile1.addCounter("time1", TUnit.TIME_NS, null);
            time1.setValue(2000000000L);
            Counter time2 = profile1.addCounter("time2", TUnit.TIME_NS, null);
            time2.setValue(0);

            Counter count1 = profile1.addCounter("count1", TUnit.UNIT, null);
            count1.setValue(1);

            profiles.add(profile1);
        }

        RuntimeProfile profile2 = new RuntimeProfile("profile");
        {
            Counter time1 = profile2.addCounter("time1", TUnit.TIME_NS, null);
            time1.setValue(2000000000L);
            Counter time2 = profile2.addCounter("time2", TUnit.TIME_NS, null);
            time2.setValue(2000000000L);

            Counter count1 = profile2.addCounter("count1", TUnit.UNIT, null);
            count1.setValue(1);

            profiles.add(profile2);
        }

        RuntimeProfile mergedProfile = RuntimeProfile.mergeIsomorphicProfiles(profiles, null);
        Assert.assertNotNull(mergedProfile);

        Counter mergedTime1 = mergedProfile.getCounter("time1");
        Assert.assertEquals(2000000000L, mergedTime1.getValue());
        Counter mergedMinOfTime1 = mergedProfile.getCounter("__MIN_OF_time1");
        Counter mergedMaxOfTime1 = mergedProfile.getCounter("__MAX_OF_time1");
        Assert.assertNotNull(mergedMinOfTime1);
        Assert.assertNotNull(mergedMaxOfTime1);
        Assert.assertEquals(2000000000L, mergedMinOfTime1.getValue());
        Assert.assertEquals(2000000000L, mergedMaxOfTime1.getValue());

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
            Counter time1 = profile1.addCounter("time1", TUnit.TIME_NS, null);
            time1.setValue(2000000000L);
            Counter minOfTime1 = profile1.addCounter("__MIN_OF_time1", TUnit.TIME_NS, null, "time1");
            minOfTime1.setValue(1500000000L);
            Counter maxOfTime1 = profile1.addCounter("__MAX_OF_time1", TUnit.TIME_NS, null, "time1");
            maxOfTime1.setValue(5000000000L);

            Counter count1 = profile1.addCounter("count1", TUnit.UNIT, null);
            count1.setValue(6);
            Counter minOfCount1 = profile1.addCounter("__MIN_OF_count1", TUnit.UNIT, null, "count1");
            minOfCount1.setValue(1);
            Counter maxOfCount1 = profile1.addCounter("__MAX_OF_count1", TUnit.UNIT, null, "count1");
            maxOfCount1.setValue(3);

            profiles.add(profile1);
        }

        RuntimeProfile profile2 = new RuntimeProfile("profile");
        {
            Counter time1 = profile2.addCounter("time1", TUnit.TIME_NS, null);
            time1.setValue(3000000000L);
            Counter minOfTime1 = profile2.addCounter("__MIN_OF_time1", TUnit.TIME_NS, null, "time1");
            minOfTime1.setValue(100000000L);
            Counter maxOfTime1 = profile2.addCounter("__MAX_OF_time1", TUnit.TIME_NS, null, "time1");
            maxOfTime1.setValue(4000000000L);

            Counter count1 = profile2.addCounter("count1", TUnit.UNIT, null);
            count1.setValue(15);
            Counter minOfCount1 = profile2.addCounter("__MIN_OF_count1", TUnit.UNIT, null, "count1");
            minOfCount1.setValue(4);
            Counter maxOfCount1 = profile2.addCounter("__MAX_OF_count1", TUnit.UNIT, null, "count1");
            maxOfCount1.setValue(6);

            profiles.add(profile2);
        }

        RuntimeProfile mergedProfile = RuntimeProfile.mergeIsomorphicProfiles(profiles, null);
        Assert.assertNotNull(mergedProfile);

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
    public void testProfileMergeStrategy() {
        List<RuntimeProfile> profiles = Lists.newArrayList();

        TCounterStrategy strategy1 = new TCounterStrategy();
        strategy1.aggregate_type = TCounterAggregateType.SUM;
        strategy1.merge_type = TCounterMergeType.MERGE_ALL;

        TCounterStrategy strategy2 = new TCounterStrategy();
        strategy2.aggregate_type = TCounterAggregateType.SUM;
        strategy2.merge_type = TCounterMergeType.SKIP_ALL;

        TCounterStrategy strategy3 = new TCounterStrategy();
        strategy3.aggregate_type = TCounterAggregateType.AVG;
        strategy3.merge_type = TCounterMergeType.SKIP_FIRST_MERGE;

        TCounterStrategy strategy4 = new TCounterStrategy();
        strategy4.aggregate_type = TCounterAggregateType.AVG;
        strategy4.merge_type = TCounterMergeType.SKIP_SECOND_MERGE;

        RuntimeProfile profile1 = new RuntimeProfile("profile");
        {
            Counter time1 = profile1.addCounter("time1", TUnit.TIME_NS, strategy1);
            time1.setValue(1000000000L);

            Counter time2 = profile1.addCounter("time2", TUnit.TIME_NS, strategy2);
            time2.setValue(2000000000L);

            Counter count1 = profile1.addCounter("count1", TUnit.UNIT, strategy3);
            count1.setValue(6);

            Counter count2 = profile1.addCounter("count2", TUnit.UNIT, strategy4);
            count2.setValue(8);

            profiles.add(profile1);
        }

        RuntimeProfile profile2 = new RuntimeProfile("profile");
        {
            Counter time1 = profile2.addCounter("time1", TUnit.TIME_NS, strategy1);
            time1.setValue(1000000000L);

            Counter time2 = profile2.addCounter("time2", TUnit.TIME_NS, strategy2);
            time2.setValue(3000000000L);

            Counter count1 = profile2.addCounter("count1", TUnit.UNIT, strategy3);
            count1.setValue(6);

            Counter count2 = profile2.addCounter("count2", TUnit.UNIT, strategy4);
            count2.setValue(8);
            profiles.add(profile2);
        }

        RuntimeProfile mergedProfile = RuntimeProfile.mergeIsomorphicProfiles(profiles, null);
        Assert.assertNotNull(mergedProfile);

        Counter mergedTime1 = mergedProfile.getCounter("time1");
        Assert.assertEquals(2000000000L, mergedTime1.getValue());
        Counter mergedMinOfTime1 = mergedProfile.getCounter("__MIN_OF_time1");
        Counter mergedMaxOfTime1 = mergedProfile.getCounter("__MAX_OF_time1");
        Assert.assertNotNull(mergedMinOfTime1);
        Assert.assertNotNull(mergedMaxOfTime1);
        Assert.assertEquals(1000000000L, mergedMinOfTime1.getValue());
        Assert.assertEquals(1000000000L, mergedMaxOfTime1.getValue());

        Counter mergedTime2 = mergedProfile.getCounter("time2");
        Assert.assertEquals(2000000000L, mergedTime2.getValue());
        Counter mergedMinOfTime2 = mergedProfile.getCounter("__MIN_OF_time2");
        Counter mergedMaxOfTime2 = mergedProfile.getCounter("__MAX_OF_time2");
        Assert.assertNull(mergedMinOfTime2);
        Assert.assertNull(mergedMaxOfTime2);

        Counter mergedCount1 = mergedProfile.getCounter("count1");
        Assert.assertEquals(6, mergedCount1.getValue());
        Counter mergedMinOfCount1 = mergedProfile.getCounter("__MIN_OF_count1");
        Counter mergedMaxOfCount1 = mergedProfile.getCounter("__MAX_OF_count1");
        Assert.assertNotNull(mergedMinOfCount1);
        Assert.assertNotNull(mergedMaxOfCount1);
        Assert.assertEquals(6, mergedMinOfCount1.getValue());
        Assert.assertEquals(6, mergedMaxOfCount1.getValue());

        Counter mergedCount2 = mergedProfile.getCounter("count2");
        Assert.assertEquals(8, mergedCount2.getValue());
        Counter mergedMinOfCount2 = mergedProfile.getCounter("__MIN_OF_count2");
        Counter mergedMaxOfCount2 = mergedProfile.getCounter("__MAX_OF_count2");
        Assert.assertNull(mergedMinOfCount2);
        Assert.assertNull(mergedMaxOfCount2);
    }

    @Test
    public void testRemoveRedundantMinMaxMetrics() {
        RuntimeProfile profile0 = new RuntimeProfile("profile0");
        RuntimeProfile profile1 = new RuntimeProfile("profile1");
        profile0.addChild(profile1);

        Counter time1 = profile1.addCounter("time1", TUnit.TIME_NS, null);
        time1.setValue(1500000000L);
        Counter minOfTime1 = profile1.addCounter("__MIN_OF_time1", TUnit.TIME_NS, null, "time1");
        minOfTime1.setValue(1500000000L);
        Counter maxOfTime1 = profile1.addCounter("__MAX_OF_time1", TUnit.TIME_NS, null, "time1");
        maxOfTime1.setValue(1500000000L);

        Counter time2 = profile1.addCounter("time2", TUnit.TIME_NS, null);
        time2.setValue(2000000000L);
        Counter minOfTime2 = profile1.addCounter("__MIN_OF_time2", TUnit.TIME_NS, null, "time2");
        minOfTime2.setValue(1500000000L);
        Counter maxOfTime2 = profile1.addCounter("__MAX_OF_time2", TUnit.TIME_NS, null, "time2");
        maxOfTime2.setValue(3000000000L);

        Counter count1 = profile1.addCounter("count1", TUnit.UNIT, null);
        count1.setValue(6);
        Counter minOfCount1 = profile1.addCounter("__MIN_OF_count1", TUnit.UNIT, null, "count1");
        minOfCount1.setValue(1);
        Counter maxOfCount1 = profile1.addCounter("__MAX_OF_count1", TUnit.UNIT, null, "count1");
        maxOfCount1.setValue(1);

        Counter count2 = profile1.addCounter("count2", TUnit.UNIT, null);
        count2.setValue(6);
        Counter minOfCount2 = profile1.addCounter("__MIN_OF_count2", TUnit.UNIT, null, "count2");
        minOfCount2.setValue(1);
        Counter maxOfCount2 = profile1.addCounter("__MAX_OF_count2", TUnit.UNIT, null, "count2");
        maxOfCount2.setValue(3);

        RuntimeProfile.removeRedundantMinMaxMetrics(profile0);

        Assert.assertNull(profile1.getCounter("__MIN_OF_time1"));
        Assert.assertNull(profile1.getCounter("__MAX_OF_time1"));
        Assert.assertNotNull(profile1.getCounter("__MIN_OF_time2"));
        Assert.assertNotNull(profile1.getCounter("__MAX_OF_time2"));
        Assert.assertNotNull(profile1.getCounter("__MIN_OF_count1"));
        Assert.assertNotNull(profile1.getCounter("__MAX_OF_count1"));
        Assert.assertNotNull(profile1.getCounter("__MIN_OF_count2"));
        Assert.assertNotNull(profile1.getCounter("__MAX_OF_count2"));
    }

    @Test
    public void testMissingCounter() {
        List<RuntimeProfile> profiles = Lists.newArrayList();

        RuntimeProfile profile1 = new RuntimeProfile("profile");
        {
            Counter count1 = profile1.addCounter("count1", TUnit.UNIT, null);
            count1.setValue(1);

            Counter count2 = profile1.addCounter("count2", TUnit.UNIT, null);
            count2.setValue(5);
            Counter count2Sub = profile1.addCounter("count2_sub", TUnit.UNIT, null, "count2");
            count2Sub.setValue(6);
            profiles.add(profile1);
        }

        RuntimeProfile profile2 = new RuntimeProfile("profile");
        {
            Counter count1 = profile2.addCounter("count1", TUnit.UNIT, null);
            count1.setValue(1);
            Counter count1Sub = profile2.addCounter("count1_sub", TUnit.UNIT, null, "count1");
            count1Sub.setValue(2);

            profiles.add(profile2);
        }

        RuntimeProfile mergedProfile = RuntimeProfile.mergeIsomorphicProfiles(profiles, null);
        Assert.assertNotNull(mergedProfile);

        Assert.assertEquals(13, mergedProfile.getCounterMap().size());
        RuntimeProfile.removeRedundantMinMaxMetrics(mergedProfile);
        Assert.assertEquals(7, mergedProfile.getCounterMap().size());
        Assert.assertTrue(mergedProfile.getCounterMap().containsKey("count1"));
        Assert.assertEquals(2, mergedProfile.getCounterMap().get("count1").getValue());
        Assert.assertTrue(mergedProfile.getCounterMap().containsKey("__MIN_OF_count1"));
        Assert.assertEquals(1, mergedProfile.getCounterMap().get("__MIN_OF_count1").getValue());
        Assert.assertTrue(mergedProfile.getCounterMap().containsKey("__MAX_OF_count1"));
        Assert.assertEquals(1, mergedProfile.getCounterMap().get("__MAX_OF_count1").getValue());
        Assert.assertTrue(mergedProfile.getCounterMap().containsKey("count1_sub"));
        Assert.assertEquals(2, mergedProfile.getCounterMap().get("count1_sub").getValue());
        Assert.assertTrue(mergedProfile.getCounterMap().containsKey("count2"));
        Assert.assertEquals(5, mergedProfile.getCounterMap().get("count2").getValue());
        Assert.assertTrue(mergedProfile.getCounterMap().containsKey("count2_sub"));
        Assert.assertEquals(6, mergedProfile.getCounterMap().get("count2_sub").getValue());
    }

    @Test
    public void testConflictInfoString() {
        List<RuntimeProfile> profiles = Lists.newArrayList();

        RuntimeProfile profile1 = new RuntimeProfile("profile");
        {
            profile1.addInfoString("key1", "value1");
            profiles.add(profile1);
        }

        RuntimeProfile profile2 = new RuntimeProfile("profile");
        {
            profile2.addInfoString("key1", "value2");
            profiles.add(profile2);
        }

        RuntimeProfile profile3 = new RuntimeProfile("profile");
        {
            profile3.addInfoString("key1", "value1");
            profiles.add(profile3);
        }

        RuntimeProfile profile4 = new RuntimeProfile("profile");
        {
            profile4.addInfoString("key1__DUP(1)", "value3");
            profile4.addInfoString("key1", "value4");
            profiles.add(profile4);
        }

        RuntimeProfile profile5 = new RuntimeProfile("profile");
        {
            profile5.addInfoString("key1", "value5");
            profile5.addInfoString("key1__DUP(1)", "value6");
            profiles.add(profile5);
        }

        RuntimeProfile mergedProfile = RuntimeProfile.mergeIsomorphicProfiles(profiles, null);
        Assert.assertNotNull(mergedProfile);

        Set<String> expectedValues = Sets.newHashSet("value1", "value2", "value3", "value4", "value5", "value6");
        Set<String> actualValues = Sets.newHashSet();

        actualValues.add(mergedProfile.getInfoString("key1"));
        actualValues.add(mergedProfile.getInfoString("key1__DUP(0)"));
        actualValues.add(mergedProfile.getInfoString("key1__DUP(1)"));
        actualValues.add(mergedProfile.getInfoString("key1__DUP(2)"));
        actualValues.add(mergedProfile.getInfoString("key1__DUP(3)"));
        actualValues.add(mergedProfile.getInfoString("key1__DUP(4)"));

        Assert.assertEquals(expectedValues, actualValues);
    }

    @Test
    public void testJsonProfileFormater() {
        //profile
        RuntimeProfile profile = new RuntimeProfile("profile");
        profile.addInfoString("key", "value");
        profile.addInfoString("key1", "value1");
        Counter count1 = profile.addCounter("count1", TUnit.UNIT, null);
        count1.setValue(15);

        //child1
        RuntimeProfile child1 = new RuntimeProfile("child1");
        child1.addInfoString("child1_key", "child1_value");
        profile.addChild(child1);

        //child11
        RuntimeProfile child11 = new RuntimeProfile("child11");
        child11.addInfoString("child11_key", "child11_value");
        Counter count2 = child11.addCounter("data_size", TUnit.BYTES_PER_SECOND, null);
        count2.setValue(10240);
        Counter count3 = child11.addCounter("count2", TUnit.UNIT_PER_SECOND, null);
        count3.setValue(1000);
        child1.addChild(child11);

        //child12
        RuntimeProfile child12 = new RuntimeProfile("child12");
        child1.addChild(child12);
        Counter count4 = child12.addCounter("count3", TUnit.UNIT, null);
        count4.setValue(15);
        Counter count5 = child12.addCounter("data_size", TUnit.BYTES, null);
        count5.setValue(10240);
        Counter count6 = child12.addCounter("time_ns", TUnit.TIME_NS, null);
        count6.setValue(1000000);

        RuntimeProfile.JsonProfileFormater formater = new RuntimeProfile.JsonProfileFormater();
        String jsonStr = formater.format(profile);
        JsonObject jsonObj = new Gson().fromJson(jsonStr, JsonElement.class).getAsJsonObject();

        JsonObject jsonObjProfile = jsonObj.getAsJsonObject("profile");
        Assert.assertEquals(jsonObjProfile.getAsJsonPrimitive("key").getAsString(), "value");
        Assert.assertEquals(jsonObjProfile.getAsJsonPrimitive("key1").getAsString(), "value1");
        Assert.assertEquals(jsonObjProfile.getAsJsonPrimitive("count1").getAsString(), "15");

        JsonObject jsonObjchild1 = jsonObjProfile.getAsJsonObject("child1");
        Assert.assertEquals(jsonObjchild1.getAsJsonPrimitive("child1_key").getAsString(), "child1_value");

        JsonObject jsonObjchild11 = jsonObjchild1.getAsJsonObject("child11");
        Assert.assertEquals(jsonObjchild11.getAsJsonPrimitive("child11_key").getAsString(), "child11_value");
        Assert.assertEquals(jsonObjchild11.getAsJsonPrimitive("count2").getAsString(), "1.0K /sec");
        Assert.assertEquals(jsonObjchild11.getAsJsonPrimitive("data_size").getAsString(), "10.0 KB/sec");

        JsonObject jsonObjchild12 = jsonObjchild1.getAsJsonObject("child12");
        Assert.assertEquals(jsonObjchild12.getAsJsonPrimitive("count3").getAsString(), "15");
        Assert.assertEquals(jsonObjchild12.getAsJsonPrimitive("data_size").getAsString(), "10.00 KB");
        Assert.assertEquals(jsonObjchild12.getAsJsonPrimitive("time_ns").getAsString(), "1.0ms");

    }
}
