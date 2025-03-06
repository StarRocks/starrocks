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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/util/RuntimeProfile.java

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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.starrocks.common.Pair;
import com.starrocks.common.Reference;
import com.starrocks.thrift.TCounter;
import com.starrocks.thrift.TCounterStrategy;
import com.starrocks.thrift.TRuntimeProfileNode;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * It is accessed by two kinds of thread, one is to create this RuntimeProfile
 * , named 'query thread', the other is to call
 * {@link com.starrocks.common.proc.CurrentQueryInfoProvider}.
 */
public class RuntimeProfile {

    private static final Logger LOG = LogManager.getLogger(RuntimeProfile.class);
    public static final String ROOT_COUNTER = "";
    public static final String TOTAL_TIME_COUNTER = "TotalTime";
    public static final String MERGED_INFO_PREFIX_MIN = "__MIN_OF_";
    public static final String MERGED_INFO_PREFIX_MAX = "__MAX_OF_";

    private final Counter counterTotalTime;

    private final Map<String, String> infoStrings = Collections.synchronizedMap(Maps.newLinkedHashMap());

    // These will be hold by other thread.
    private final Map<String, Pair<Counter, String>> counterMap = Maps.newConcurrentMap();
    private final Map<String, RuntimeProfile> childMap = Maps.newConcurrentMap();

    private final Map<String, Set<String>> childCounterMap = Maps.newConcurrentMap();
    private final List<Pair<RuntimeProfile, Boolean>> childList = Lists.newCopyOnWriteArrayList();

    private String name;
    private double localTimePercent;
    // The version of this profile. It is used to prevent updating this profile
    // from an old one.
    private volatile long version = 0;

    public RuntimeProfile(String name) {
        this();
        this.name = name;
    }

    public RuntimeProfile() {
        this.counterTotalTime = new Counter(TUnit.TIME_NS, null, 0);
        this.localTimePercent = 0;
        this.counterMap.put(TOTAL_TIME_COUNTER, Pair.create(counterTotalTime, ROOT_COUNTER));
    }

    public Counter getCounterTotalTime() {
        return counterTotalTime;
    }

    public Map<String, Counter> getCounterMap() {
        return counterMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().first));
    }

    public List<Pair<RuntimeProfile, Boolean>> getChildList() {
        return childList;
    }

    public Map<String, RuntimeProfile> getChildMap() {
        return childMap;
    }

    public RuntimeProfile getChild(String childName) {
        return childMap.get(childName);
    }

    public void removeAllChildren() {
        childList.clear();
        childMap.clear();
    }

    public Counter addCounter(String name, TUnit type, TCounterStrategy strategy) {
        return addCounter(name, type, strategy, ROOT_COUNTER);
    }

    public Counter addCounter(String name, TUnit type, TCounterStrategy strategy, String parentName) {
        if (strategy == null) {
            strategy = Counter.createStrategy(type);
        }
        Pair<Counter, String> pair = this.counterMap.get(name);
        if (pair != null) {
            return pair.first;
        } else {
            Preconditions.checkState(parentName.equals(ROOT_COUNTER) || this.counterMap.containsKey(parentName),
                    String.format("dangling counter %s->%s", parentName, name));
            Counter newCounter = new Counter(type, strategy, 0);
            this.counterMap.put(name, Pair.create(newCounter, parentName));

            if (!childCounterMap.containsKey(parentName)) {
                childCounterMap.putIfAbsent(parentName, Sets.newConcurrentHashSet());
            }
            Set<String> childNames = childCounterMap.get(parentName);
            childNames.add(name);
            return newCounter;
        }
    }

    public void removeCounter(String name) {
        if (!counterMap.containsKey(name)) {
            return;
        }

        // Remove from its parent sub sets
        Pair<Counter, String> pair = counterMap.get(name);
        String parentName = pair.second;
        if (childCounterMap.containsKey(parentName)) {
            Set<String> childNames = childCounterMap.get(parentName);
            childNames.remove(name);
        }

        // Remove child counter recursively
        Queue<String> nameQueue = Lists.newLinkedList();
        nameQueue.offer(name);
        while (!nameQueue.isEmpty()) {
            String topName = nameQueue.poll();
            Set<String> childNames = childCounterMap.get(topName);
            if (childNames != null) {
                for (String childName : childNames) {
                    nameQueue.offer(childName);
                }
            }
            counterMap.remove(topName);
            childCounterMap.remove(topName);
        }
    }

    public Counter getCounter(String name) {
        Pair<Counter, String> pair = counterMap.get(name);
        if (pair != null) {
            return pair.first;
        }
        return null;
    }

    public Pair<Counter, String> getCounterPair(String name) {
        return counterMap.get(name);
    }

    public Counter getMaxCounter(String name) {
        Counter counter;
        if ((counter = getCounter(MERGED_INFO_PREFIX_MAX + name)) != null) {
            return counter;
        }

        return getCounter(name);
    }

    // Copy all the counters from src profile
    public void copyAllCountersFrom(RuntimeProfile srcProfile) {
        if (srcProfile == null || this == srcProfile) {
            return;
        }

        Queue<Pair<String, String>> nameQueue = Lists.newLinkedList();
        nameQueue.offer(Pair.create(ROOT_COUNTER, ROOT_COUNTER));
        while (!nameQueue.isEmpty()) {
            Pair<String, String> topPair = nameQueue.poll();

            String name = topPair.first;
            String parentName = topPair.second;

            if (!Objects.equals(ROOT_COUNTER, name)) {
                Counter srcCounter = srcProfile.counterMap.get(name).first;
                Counter newCounter = addCounter(name, srcCounter.getType(), srcCounter.getStrategy(), parentName);
                newCounter.setValue(srcCounter.getValue());
                if (srcCounter.getMinValue().isPresent()) {
                    newCounter.setMinValue(srcCounter.getMinValue().get());
                }
                if (srcCounter.getMaxValue().isPresent()) {
                    newCounter.setMaxValue(srcCounter.getMaxValue().get());
                }
            }

            Set<String> childNames = srcProfile.childCounterMap.get(name);
            if (childNames != null) {
                for (String childName : childNames) {
                    nameQueue.offer(Pair.create(childName, name));
                }
            }
        }
    }

    public long getVersion() {
        return version;
    }

    public void update(final TRuntimeProfileTree thriftProfile) {
        Reference<Integer> idx = new Reference<>(0);
        update(thriftProfile.nodes, idx, false);
        Preconditions.checkState(idx.getRef().equals(thriftProfile.nodes.size()));
    }

    // Update a subtree of profiles from nodes, rooted at idx. It will do a preorder
    // traversal, and modify idx in the traversal process. idx will point to the node
    // immediately following this subtree after the traversal. If the version of the
    // parent node, or the version of root node for this subtree is older, skip to update
    // the profile of subtree, but still traverse the nodes to get the node immediately
    // following this subtree.
    private void update(List<TRuntimeProfileNode> nodes, Reference<Integer> idx, boolean isParentNodeOld) {
        TRuntimeProfileNode node = nodes.get(idx.getRef());

        boolean isNodeOld;
        if (isParentNodeOld || (node.isSetVersion() && node.version < version)) {
            isNodeOld = true;
        } else {
            isNodeOld = false;
            if (node.isSetVersion()) {
                version = node.version;
            }
        }

        // update this level's counters
        if (!isNodeOld && node.counters != null) {
            // mapping from counterName to parentCounterName
            Map<String, String> child2ParentMap = Maps.newHashMap();
            if (node.child_counters_map != null) {
                // update childCounters
                for (Map.Entry<String, Set<String>> entry : node.child_counters_map.entrySet()) {
                    String parentName = entry.getKey();
                    for (String childName : entry.getValue()) {
                        child2ParentMap.put(childName, parentName);
                    }
                }
            }
            // First processing counters by hierarchy
            Map<String, TCounter> tCounterMap = node.counters.stream()
                    .collect(Collectors.toMap(TCounter::getName, t -> t));
            Queue<String> nameQueue = Lists.newLinkedList();
            nameQueue.offer(ROOT_COUNTER);
            while (!nameQueue.isEmpty()) {
                String topName = nameQueue.poll();

                if (!Objects.equals(ROOT_COUNTER, topName)) {
                    Pair<Counter, String> pair = counterMap.get(topName);
                    TCounter tcounter = tCounterMap.get(topName);
                    String parentName = child2ParentMap.get(topName);
                    if (pair == null && tcounter != null && parentName != null) {
                        Counter counter =
                                addCounter(topName, tcounter.type, tcounter.strategy, parentName);
                        counter.setValue(tcounter.value);
                        counter.setStrategy(tcounter.strategy);
                        if (tcounter.isSetMin_value()) {
                            counter.setMinValue(tcounter.getMin_value());
                        }
                        if (tcounter.isSetMax_value()) {
                            counter.setMaxValue(tcounter.getMax_value());
                        }
                        tCounterMap.remove(topName);
                    } else if (pair != null && tcounter != null) {
                        if (pair.first.getType() != tcounter.type) {
                            LOG.error("Cannot update counters with the same name but different types"
                                    + " type=" + tcounter.type);
                        } else {
                            pair.first.setValue(tcounter.value);
                        }
                        tCounterMap.remove(topName);
                    }
                }

                if (node.child_counters_map != null) {
                    Set<String> childNames = node.child_counters_map.get(topName);
                    if (childNames != null) {
                        for (String childName : childNames) {
                            nameQueue.offer(childName);
                        }
                    }
                }
            }
            // Second, processing the remaining counters, set ROOT_COUNTER as it's parent
            for (TCounter tcounter : tCounterMap.values()) {
                Pair<Counter, String> pair = counterMap.get(tcounter.name);
                if (pair == null) {
                    Counter counter = addCounter(tcounter.name, tcounter.type, tcounter.strategy);
                    counter.setValue(tcounter.value);
                    counter.setStrategy(tcounter.strategy);
                    if (tcounter.isSetMin_value()) {
                        counter.setMinValue(tcounter.getMin_value());
                    }
                    if (tcounter.isSetMax_value()) {
                        counter.setMaxValue(tcounter.getMax_value());
                    }
                } else {
                    if (pair.first.getType() != tcounter.type) {
                        LOG.error("Cannot update counters with the same name but different types"
                                + " type=" + tcounter.type);
                    } else {
                        pair.first.setValue(tcounter.value);
                    }
                }
            }
        }

        if (!isNodeOld && node.info_strings_display_order != null) {
            Map<String, String> nodeInfoStrings = node.info_strings;
            for (String key : node.info_strings_display_order) {
                String value = nodeInfoStrings.get(key);
                Preconditions.checkState(value != null);
                addInfoString(key, value);
            }
        }

        idx.setRef(idx.getRef() + 1);

        for (int i = 0; i < node.num_children; i++) {
            TRuntimeProfileNode tchild = nodes.get(idx.getRef());
            String childName = tchild.name;
            RuntimeProfile childProfile = this.childMap.get(childName);
            if (childProfile == null) {
                childProfile = new RuntimeProfile(childName);
                addChild(childProfile);
            }
            childProfile.update(nodes, idx, isNodeOld);
        }
    }

    // Print the profile:
    //  1. Profile Name
    //  2. Info Strings
    //  3. Counters
    //  4. Children
    public void prettyPrint(StringBuilder builder, String prefix) {
        ProfileFormatter formatter = new DefaultProfileFormatter(builder);
        formatter.format(this, prefix);
    }

    public String toString() {
        ProfileFormatter formater = new DefaultProfileFormatter();
        return formater.format(this);
    }

    public static String printCounter(Counter counter) {
        if (counter == null) {
            return null;
        }
        return printCounter(counter.getValue(), counter.getType());
    }

    private static String printCounter(long value, TUnit type) {
        StringBuilder builder = new StringBuilder();
        try (Formatter fmt = new Formatter()) {
            if (type == TUnit.UNIT || type == TUnit.UNIT_PER_SECOND) {
                Pair<Double, String> pair = DebugUtil.getUint(value);
                if (pair.second.isEmpty()) {
                    builder.append(value);
                } else {
                    builder.append(fmt.format("%.3f", pair.first)).append(pair.second)
                            .append(" (").append(value).append(")");
                }
                if (type == TUnit.UNIT_PER_SECOND) {
                    builder.append(" /sec");
                }
            } else if (type == TUnit.BYTES || type == TUnit.BYTES_PER_SECOND) {
                Pair<Double, String> pair = DebugUtil.getByteUint(value);
                builder.append(fmt.format("%.3f", pair.first)).append(" ").append(pair.second);
                if (type == TUnit.BYTES_PER_SECOND) {
                    builder.append("/sec");
                }
            } else if (type == TUnit.TIME_NS) {
                if (value >= DebugUtil.BILLION) {
                    // If the time is over a second, print it up to ms.
                    value /= DebugUtil.MILLION;
                    DebugUtil.printTimeMs(value, builder);
                } else if (value >= DebugUtil.MILLION) {
                    // if the time is over a ms, print it up to microsecond in the unit of ms.
                    value /= DebugUtil.THOUSAND;
                    long remain = value % DebugUtil.THOUSAND;
                    if (remain == 0) {
                        builder.append(value / DebugUtil.THOUSAND).append("ms");
                    } else {
                        builder.append(value / DebugUtil.THOUSAND).append(".")
                                .append(String.format("%03d", remain)).append("ms");
                    }
                } else if (value >= DebugUtil.THOUSAND) {
                    // if the time is over a microsecond, print it using unit microsecond
                    long remain = value % DebugUtil.THOUSAND;
                    if (remain == 0) {
                        builder.append(value / DebugUtil.THOUSAND).append("us");
                    } else {
                        builder.append(value / DebugUtil.THOUSAND).append(".")
                                .append(String.format("%03d", remain)).append("us");
                    }
                } else {
                    builder.append(value).append("ns");
                }
            } else if (type == TUnit.DOUBLE_VALUE) {
                builder.append(fmt.format("%.3f", (double) value));
            } else if (type == TUnit.NONE) {
                // Do nothing
            } else {
                Preconditions.checkState(false, "type=" + type);
            }
        }
        return builder.toString();
    }

    // concurrency safe
    public void addChild(RuntimeProfile child) {
        if (child == null) {
            return;
        }

        childMap.put(child.name, child);
        Pair<RuntimeProfile, Boolean> pair = Pair.create(child, true);
        childList.add(pair);
    }

    // concurrency safe
    public void addChildren(List<RuntimeProfile> children) {
        if (children.isEmpty()) {
            return;
        }
        final RuntimeProfile child = children.get(0);
        childMap.put(child.name, child);
        List<Pair<RuntimeProfile, Boolean>> childList =
                children.stream().map(c -> new Pair<>(c, true)).collect(Collectors.toList());
        this.childList.addAll(childList);
    }

    public void removeChild(String childName) {
        RuntimeProfile childProfile = childMap.remove(childName);
        if (childProfile == null) {
            return;
        }
        childList.removeIf(childPair -> childPair.first == childProfile);
    }

    // Because the profile of summary and child fragment is not a real parent-child relationship
    // Each child profile needs to calculate the time proportion consumed by itself
    public void computeTimeInChildProfile() {
        childMap.values().forEach(RuntimeProfile::computeTimeInProfile);
    }

    public void computeTimeInProfile() {
        computeTimeInProfile(this.counterTotalTime.getValue());
    }

    public void computeTimeInProfile(long total) {
        if (total == 0) {
            return;
        }

        // Add all the total times in all the children
        long totalChildTime = 0;

        for (Pair<RuntimeProfile, Boolean> pair : this.childList) {
            totalChildTime += pair.first.getCounterTotalTime().getValue();
        }
        long localTime = this.getCounterTotalTime().getValue() - totalChildTime;
        // Counters have some margin, set to 0 if it was negative.
        localTime = Math.max(0, localTime);
        this.localTimePercent = (double) localTime / (double) total;
        this.localTimePercent = Math.min(1.0, this.localTimePercent) * 100;

        // Recurse on children
        for (Pair<RuntimeProfile, Boolean> pair : this.childList) {
            pair.first.computeTimeInProfile(total);
        }
    }

    // from bigger to smaller
    public void sortChildren() {
        this.childList.sort((profile1, profile2) ->
                Long.compare(profile2.first.getCounterTotalTime().getValue(),
                        profile1.first.getCounterTotalTime().getValue()));
    }

    public void addInfoString(String key, String value) {
        this.infoStrings.put(key, value);
    }

    public void removeInfoString(String key) {
        infoStrings.remove(key);
    }

    // Copy all info strings from src profile
    public void copyAllInfoStringsFrom(RuntimeProfile srcProfile, Set<String> excludedInfoStrings) {
        if (srcProfile == null || this == srcProfile) {
            return;
        }

        srcProfile.infoStrings.forEach((key, value) -> {
            if (CollectionUtils.isNotEmpty(excludedInfoStrings) && excludedInfoStrings.contains(key)) {
                return;
            }
            if (!this.infoStrings.containsKey(key)) {
                this.infoStrings.put(key, value);
            } else if (!Objects.equals(value, this.infoStrings.get(key))) {
                String originalKey = key;
                int pos;
                if ((pos = key.indexOf("__DUP(")) != -1) {
                    originalKey = key.substring(0, pos);
                }
                int offset = -1;
                int previousOffset;
                int step = 1;
                while (true) {
                    previousOffset = offset;
                    offset += step;
                    String indexedKey = String.format("%s__DUP(%d)", originalKey, offset);
                    if (!this.infoStrings.containsKey(indexedKey)) {
                        if (step == 1) {
                            this.infoStrings.put(indexedKey, value);
                            break;
                        }
                        // Forward too much, try to forward half of the former size
                        offset = previousOffset;
                        step >>= 1;
                        continue;
                    }
                    step <<= 1;
                }
            }
        });
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getInfoStrings() {
        return infoStrings;
    }

    public Map<String, Set<String>> getChildCounterMap() {
        return childCounterMap;
    }

    public double getLocalTimePercent() {
        return localTimePercent;
    }

    public boolean containsInfoString(String key) {
        return infoStrings.containsKey(key);
    }

    // Returns the value to which the specified key is mapped;
    // or null if this map contains no mapping for the key.
    public String getInfoString(String key) {
        return infoStrings.get(key);
    }

    // Serializes profile to thrift. Not threadsafe.
    public TRuntimeProfileTree toThrift() {
        TRuntimeProfileTree profileTree = new TRuntimeProfileTree();
        profileTree.setNodes(new ArrayList<>());
        toThrift(profileTree.nodes);
        return profileTree;
    }

    // Flatten the tree of runtime profiles by in-order traversal. Not threadsafe.
    private void toThrift(List<TRuntimeProfileNode> nodes) {
        TRuntimeProfileNode node = new TRuntimeProfileNode();
        nodes.add(node);

        node.setName(name);
        node.setNum_children(childMap.size());
        node.setIndent(true);
        node.setVersion(version);

        for (Map.Entry<String, Pair<Counter, String>> entry : counterMap.entrySet()) {
            Counter counter = entry.getValue().first;
            TCounter tCounter = new TCounter();
            tCounter.setName(entry.getKey());
            tCounter.setValue(counter.getValue());
            tCounter.setType(counter.getType());
            tCounter.setStrategy(counter.getStrategy());
            counter.getMinValue().ifPresent(tCounter::setMin_value);
            counter.getMaxValue().ifPresent(tCounter::setMax_value);
            node.addToCounters(tCounter);
        }

        for (Map.Entry<String, Set<String>> entry : childCounterMap.entrySet()) {
            node.putToChild_counters_map(entry.getKey(), new HashSet<>(entry.getValue()));
        }

        for (Map.Entry<String, String> entry : infoStrings.entrySet()) {
            node.putToInfo_strings(entry.getKey(), entry.getValue());
            node.addToInfo_strings_display_order(entry.getKey());
        }

        for (RuntimeProfile child : childMap.values()) {
            child.toThrift(nodes);
        }
    }

    // Merge all the isomorphic sub profiles and the caller must know for sure
    // that all the children are isomorphic, otherwise, the behavior is undefined
    public static RuntimeProfile mergeIsomorphicProfiles(List<RuntimeProfile> profiles,
                                                         Set<String> excludedInfoStrings) {
        if (CollectionUtils.isEmpty(profiles)) {
            return null;
        }

        RuntimeProfile mergedProfile = new RuntimeProfile(profiles.get(0).getName());

        for (RuntimeProfile runtimeProfile : profiles) {
            mergedProfile.copyAllInfoStringsFrom(runtimeProfile, excludedInfoStrings);
        }

        // Find all counters, although these profiles are expected to be isomorphic,
        // some counters are only attached to one of them
        List<Map<String, Pair<TUnit, String>>> allLevelCounters = Lists.newArrayList();
        for (RuntimeProfile profile : profiles) {
            // Level order traverse starts with root
            Queue<String> nameQueue = Lists.newLinkedList();
            nameQueue.offer(ROOT_COUNTER);
            int levelIdx = -1;
            while (!nameQueue.isEmpty()) {
                levelIdx++;
                List<String> currentNames = Lists.newArrayList(nameQueue);
                nameQueue.clear();
                for (String name : currentNames) {
                    Set<String> childNames = profile.childCounterMap.get(name);
                    if (childNames != null) {
                        for (String childName : childNames) {
                            nameQueue.offer(childName);
                        }
                    }

                    if (Objects.equals(ROOT_COUNTER, name)) {
                        continue;
                    }
                    Pair<Counter, String> pair = profile.counterMap.get(name);
                    if (pair == null) {
                        LOG.warn("missing counter, profileName={}, counterName={}", profile.name, name);
                        continue;
                    }
                    Counter counter = pair.first;
                    String parentName = pair.second;

                    while (allLevelCounters.size() <= levelIdx) {
                        allLevelCounters.add(Maps.newHashMap());
                    }

                    Map<String, Pair<TUnit, String>> levelCounters = allLevelCounters.get(levelIdx);
                    if (!levelCounters.containsKey(name)) {
                        levelCounters.put(name, Pair.create(counter.getType(), parentName));
                        continue;
                    }
                    TUnit existType = levelCounters.get(name).first;
                    if (!existType.equals(counter.getType())) {
                        LOG.warn(
                                "find non-isomorphic counter, profileName={}, counterName={}, existType={}, " +
                                        "anotherType={}",
                                mergedProfile.name, name, existType.name(), counter.getType().name());
                        continue;
                    }
                }
            }
        }

        List<Triple<TUnit, String, String>> levelOrderedCounters = Lists.newArrayList();
        for (Map<String, Pair<TUnit, String>> levelCounters : allLevelCounters) {
            for (Map.Entry<String, Pair<TUnit, String>> entry : levelCounters.entrySet()) {
                String name = entry.getKey();
                TUnit type = entry.getValue().first;
                String parentName = entry.getValue().second;
                levelOrderedCounters.add(Triple.of(type, name, parentName));
            }
        }

        for (Triple<TUnit, String, String> triple : levelOrderedCounters) {
            TUnit type = triple.getLeft();
            String name = triple.getMiddle();
            String parentName = triple.getRight();

            // We don't need to calculate sum or average of counter's extra info (min value and max value) created by be
            if (name.startsWith(MERGED_INFO_PREFIX_MIN) || name.startsWith(MERGED_INFO_PREFIX_MAX)) {
                continue;
            }

            List<Counter> counters = Lists.newArrayList();
            long minValue = Long.MAX_VALUE;
            long maxValue = Long.MIN_VALUE;
            boolean alreadyMerged = false;
            boolean skipMerge = false;
            Counter skipMergeCounter = null;
            TCounterStrategy strategy = null;
            for (RuntimeProfile profile : profiles) {
                Counter counter = profile.getCounter(name);

                // Allow some counters which only attach to one of the isomorphic profiles
                // E.g. A bunch of ExchangeSinkOperators may share one SinkBuffer, so the metrics
                // of SinkBuffer only attach to the first ExchangeSinkOperator's profile
                if (counter == null) {
                    continue;
                }
                if (!type.equals(counter.getType())) {
                    LOG.warn(
                            "find non-isomorphic counter, profileName={}, counterName={}, existType={}, anotherType={}",
                            mergedProfile.name, name, type.name(), counter.getType().name());
                    continue;
                }
                strategy = counter.getStrategy();
                if (counter.isSkipMerge()) {
                    skipMerge = true;
                    skipMergeCounter = counter;
                    break;
                }

                if (!counter.isSkipMinMax()) {
                    if (counter.getMinValue().isPresent()) {
                        alreadyMerged = true;
                        minValue = Math.min(counter.getMinValue().get(), minValue);
                    } else {
                        // TODO: keep compatible with older version backend, can be removed in next version
                        Counter minCounter = profile.getCounter(MERGED_INFO_PREFIX_MIN + name);
                        if (minCounter != null) {
                            alreadyMerged = true;
                            if (minCounter.getValue() < minValue) {
                                minValue = minCounter.getValue();
                            }
                        }
                    }
                    if (counter.getMaxValue().isPresent()) {
                        alreadyMerged = true;
                        maxValue = Math.max(counter.getMaxValue().get(), maxValue);
                    } else {
                        // TODO: keep compatible with older version backend, can be removed in next version
                        Counter maxCounter = profile.getCounter(MERGED_INFO_PREFIX_MAX + name);
                        if (maxCounter != null) {
                            alreadyMerged = true;
                            if (maxCounter.getValue() > maxValue) {
                                maxValue = maxCounter.getValue();
                            }
                        }
                    }

                }

                counters.add(counter);
            }
            Counter mergedCounter;
            if (!Objects.equals(ROOT_COUNTER, parentName) && mergedProfile.getCounter(parentName) != null) {
                mergedCounter = mergedProfile.addCounter(name, type, strategy, parentName);
            } else {
                if (!Objects.equals(ROOT_COUNTER, parentName)) {
                    LOG.warn("missing parent counter, profileName={}, counterName={}, parentCounterName={}",
                            mergedProfile.name, name, parentName);
                }
                mergedCounter = mergedProfile.addCounter(name, type, strategy);
            }
            if (skipMerge) {
                mergedCounter.setValue(skipMergeCounter.getValue());
            } else {
                Counter.MergedInfo mergedInfo = Counter.mergeIsomorphicCounters(counters);
                final long mergedValue = mergedInfo.mergedValue;
                if (!alreadyMerged) {
                    minValue = mergedInfo.minValue;
                    maxValue = mergedInfo.maxValue;
                }
                mergedCounter.setValue(mergedValue);

                if (!mergedCounter.isSkipMinMax()) {
                    Counter minCounter =
                            mergedProfile.addCounter(MERGED_INFO_PREFIX_MIN + name, type, mergedCounter.getStrategy(),
                                    name);
                    Counter maxCounter =
                            mergedProfile.addCounter(MERGED_INFO_PREFIX_MAX + name, type, mergedCounter.getStrategy(),
                                    name);
                    if (minValue != Integer.MAX_VALUE) {
                        mergedCounter.setMinValue(minValue);
                        minCounter.setValue(minValue);
                    }
                    if (maxValue != Integer.MIN_VALUE) {
                        mergedCounter.setMaxValue(maxValue);
                        maxCounter.setValue(maxValue);
                    }
                }
            }

        }

        // merge children
        int maxChildSize = 0;
        RuntimeProfile profileWithFullChild = null;
        for (RuntimeProfile profile : profiles) {
            if (profile.getChildList().size() > maxChildSize) {
                maxChildSize = profile.getChildList().size();
                profileWithFullChild = profile;
            }
        }
        if (profileWithFullChild != null) {
            boolean identical = true;
            for (int i = 0; i < maxChildSize; i++) {
                Pair<RuntimeProfile, Boolean> prototypeKv = profileWithFullChild.getChildList().get(i);
                String childName = prototypeKv.first.getName();
                List<RuntimeProfile> subProfiles = Lists.newArrayList();
                for (RuntimeProfile profile : profiles) {
                    RuntimeProfile child = profile.getChild(childName);
                    if (child == null) {
                        identical = false;
                        LOG.debug("find non-isomorphic children, profileName={}, requiredChildName={}",
                                profile.name, childName);
                        continue;
                    }
                    subProfiles.add(child);
                }
                RuntimeProfile mergedChild = mergeIsomorphicProfiles(subProfiles, excludedInfoStrings);
                mergedProfile.addChild(mergedChild);
            }
            if (!identical) {
                mergedProfile.addInfoString("NotIdentical", "");
            }
        }

        return mergedProfile;
    }

    public static void removeRedundantMinMaxMetrics(RuntimeProfile profile) {
        for (String name : profile.counterMap.keySet()) {
            Counter counter = profile.getCounter(name);
            Counter minCounter = profile.getCounter(MERGED_INFO_PREFIX_MIN + name);
            Counter maxCounter = profile.getCounter(MERGED_INFO_PREFIX_MAX + name);
            if (counter == null || minCounter == null || maxCounter == null) {
                continue;
            }

            if (Objects.equals(minCounter.getValue(), maxCounter.getValue()) &&
                    Objects.equals(minCounter.getValue(), counter.getValue())) {
                profile.removeCounter(MERGED_INFO_PREFIX_MIN + name);
                profile.removeCounter(MERGED_INFO_PREFIX_MAX + name);
            }
        }

        profile.getChildList().forEach(pair -> removeRedundantMinMaxMetrics(pair.first));
    }

    interface ProfileFormatter {

        // Print the profile:
        //  1. Profile Name
        //  2. Info Strings
        //  3. Counters
        //  4. Children
        String format(RuntimeProfile profile, String prefix);

        String format(RuntimeProfile profile);
    }

    static class DefaultProfileFormatter implements ProfileFormatter {

        private final StringBuilder builder;

        DefaultProfileFormatter(StringBuilder builder) {
            this.builder = builder;
        }

        DefaultProfileFormatter() {
            this.builder = new StringBuilder();
        }

        @Override
        public String format(RuntimeProfile profile, String prefix) {
            this.doFormat(profile, prefix);
            return builder.toString();
        }

        @Override
        public String format(RuntimeProfile profile) {
            this.doFormat(profile, "");
            return builder.toString();
        }

        private void doFormat(RuntimeProfile profile, String prefix) {
            Counter totalTimeCounter = profile.getCounterMap().get(TOTAL_TIME_COUNTER);
            Preconditions.checkState(totalTimeCounter != null);
            // 1. profile name
            builder.append(prefix).append(profile.getName()).append(":");
            // total time
            if (totalTimeCounter.getValue() != 0) {
                try (Formatter fmt = new Formatter()) {
                    builder.append("(Active: ")
                            .append(printCounter(totalTimeCounter.getValue(), totalTimeCounter.getType()));
                    if (DebugUtil.THOUSAND < totalTimeCounter.getValue()) {
                        // TotalTime in nanosecond concated if it's larger than 1000ns
                        builder.append("[").append(totalTimeCounter.getValue()).append("ns]");
                    }
                    builder.append(", % non-child: ").append(fmt.format("%.2f", profile.getLocalTimePercent()))
                            .append("%)");
                }
            }
            builder.append("\n");

            // 2. info String
            for (Map.Entry<String, String> infoPair : profile.getInfoStrings().entrySet()) {
                String key = infoPair.getKey();
                String value = infoPair.getValue();
                builder.append(prefix).append("   - ").append(key);
                if (StringUtils.isNotBlank(value)) {
                    builder.append(": ").append(profile.getInfoString(key));
                }
                builder.append("\n");
            }

            // 3. counters
            printChildCounters(profile, prefix, RuntimeProfile.ROOT_COUNTER);

            // 4. children
            for (Pair<RuntimeProfile, Boolean> childPair : profile.getChildList()) {
                boolean indent = childPair.second;
                RuntimeProfile childProfile = childPair.first;
                doFormat(childProfile, prefix + (indent ? "  " : ""));
            }
        }

        private void printChildCounters(RuntimeProfile profile, String prefix, String counterName) {
            Map<String, Set<String>> childCounterMap = profile.getChildCounterMap();
            if (childCounterMap.get(counterName) == null) {
                return;
            }
            List<String> childNames = Lists.newArrayList(childCounterMap.get(counterName));
            childNames.sort(String::compareTo);

            // Keep MIN/MAX metrics head of other child counters
            List<String> minMaxChildNames = Lists.newArrayListWithCapacity(2);
            List<String> otherChildNames = Lists.newArrayListWithCapacity(childNames.size());
            for (String childName : childNames) {
                if (childName.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MIN)
                        || childName.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MAX)) {
                    minMaxChildNames.add(childName);
                } else {
                    otherChildNames.add(childName);
                }
            }
            List<String> reorderedChildNames = Lists.newArrayListWithCapacity(childNames.size());
            reorderedChildNames.addAll(minMaxChildNames);
            reorderedChildNames.addAll(otherChildNames);

            Map<String, Counter> counterMap1 = profile.getCounterMap();
            for (String childName : reorderedChildNames) {
                Counter childCounter = counterMap1.get(childName);
                Preconditions.checkState(childCounter != null);
                builder.append(prefix).append("   - ").append(childName).append(": ")
                        .append(printCounter(childCounter.getValue(), childCounter.getType())).append("\n");
                this.printChildCounters(profile, prefix + "  ", childName);
            }
        }
    }

    static class JsonProfileFormatter implements ProfileFormatter {
        private final JsonObject builder;

        JsonProfileFormatter() {
            this.builder = new JsonObject();
        }

        @Override
        public String format(RuntimeProfile profile, String prefix) {
            //may parse prefix to json
            return this.format(profile);
        }

        @Override
        public String format(RuntimeProfile profile) {
            addRuntimeProfile(profile, builder);
            Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
            return gson.toJson(builder);
        }

        public void addRuntimeProfile(RuntimeProfile profile, JsonObject jsonObject) {
            Counter totalTimeCounter = profile.getCounterMap().get(TOTAL_TIME_COUNTER);
            Preconditions.checkState(totalTimeCounter != null);
            // 1. profile name
            JsonObject innerJsonObject = new JsonObject();
            jsonObject.add(profile.getName(), innerJsonObject);
            // total time
            if (totalTimeCounter.getValue() != 0) {
                try (Formatter fmt = new Formatter()) {
                    innerJsonObject.addProperty("Active",
                            printCounter(totalTimeCounter.getValue(), totalTimeCounter.getType()));
                    if (DebugUtil.THOUSAND < totalTimeCounter.getValue()) {
                        innerJsonObject.addProperty("Active_ns", totalTimeCounter.getValue());
                    }
                    innerJsonObject.addProperty("non-child", fmt.format("%.2f", profile.getLocalTimePercent()) + "%");
                }
            }
            if (profile.getInfoStrings().size() > 0) {
                // 2. info String
                for (Map.Entry<String, String> infoPair : profile.getInfoStrings().entrySet()) {
                    String key = infoPair.getKey();
                    innerJsonObject.addProperty(key, profile.getInfoString(key));
                }
            }

            // 3. counters
            addChildCounters(profile, RuntimeProfile.ROOT_COUNTER, innerJsonObject);

            // 4. children
            if (profile.getChildList().size() > 0) {
                for (Pair<RuntimeProfile, Boolean> childPair : profile.getChildList()) {
                    RuntimeProfile childProfile = childPair.first;
                    addRuntimeProfile(childProfile, innerJsonObject);
                }
            }
        }

        private void addChildCounters(RuntimeProfile profile, String counterName, JsonObject childJsonObject) {
            Map<String, Set<String>> childCounterMap = profile.getChildCounterMap();

            if (childCounterMap.get(counterName) == null) {
                return;
            }
            List<String> childNames = Lists.newArrayList(childCounterMap.get(counterName));
            childNames.sort(String::compareTo);

            // Keep MIN/MAX metrics head of other child counters
            List<String> minMaxChildNames = Lists.newArrayListWithCapacity(2);
            List<String> otherChildNames = Lists.newArrayListWithCapacity(childNames.size());
            for (String childName : childNames) {
                if (childName.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MIN)
                        || childName.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MAX)) {
                    minMaxChildNames.add(childName);
                } else {
                    otherChildNames.add(childName);
                }
            }
            List<String> reorderedChildNames = Lists.newArrayListWithCapacity(childNames.size());
            reorderedChildNames.addAll(minMaxChildNames);
            reorderedChildNames.addAll(otherChildNames);

            for (String childName : reorderedChildNames) {
                Counter childCounter = profile.getCounterMap().get(childName);
                Preconditions.checkState(childCounter != null);
                childJsonObject.addProperty(childName,
                        printCounter(childCounter.getValue(), childCounter.getType()));
                this.addChildCounters(profile, childName, childJsonObject);
            }
        }
    }
}
