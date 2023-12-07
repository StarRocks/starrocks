// This file is made available under Elastic License 2.0.
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
import com.starrocks.common.Pair;
import com.starrocks.common.Reference;
import com.starrocks.thrift.TCounter;
import com.starrocks.thrift.TRuntimeProfileNode;
import com.starrocks.thrift.TRuntimeProfileTree;
import com.starrocks.thrift.TUnit;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Formatter;
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
    private static final String ROOT_COUNTER = "";
    private static final String MERGED_INFO_PREFIX_MIN = "__MIN_OF_";
    private static final String MERGED_INFO_PREFIX_MAX = "__MAX_OF_";

    private final Counter counterTotalTime;

    private final Map<String, String> infoStrings = Collections.synchronizedMap(Maps.newLinkedHashMap());

    // These will be hold by other thread.
    private final Map<String, Pair<Counter, String>> counterMap = Maps.newConcurrentMap();
    private final Map<String, RuntimeProfile> childMap = Maps.newConcurrentMap();

    private final Map<String, Set<String>> childCounterMap = Maps.newConcurrentMap();
    private final List<Pair<RuntimeProfile, Boolean>> childList = Lists.newCopyOnWriteArrayList();

    private String name;
    private double localTimePercent;

    public RuntimeProfile(String name) {
        this();
        this.name = name;
    }

    public RuntimeProfile() {
        this.counterTotalTime = new Counter(TUnit.TIME_NS, 0);
        this.localTimePercent = 0;
        this.counterMap.put("TotalTime", Pair.create(counterTotalTime, ROOT_COUNTER));
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

    public Counter addCounter(String name, TUnit type) {
        return addCounter(name, type, ROOT_COUNTER);
    }

    public Counter addCounter(String name, TUnit type, String parentName) {
        Pair<Counter, String> pair = this.counterMap.get(name);
        if (pair != null) {
            return pair.first;
        } else {
            Preconditions.checkState(parentName.equals(ROOT_COUNTER)
                    || this.counterMap.containsKey(parentName));
            Counter newCounter = new Counter(type, 0);
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
                Counter newCounter = addCounter(name, srcCounter.getType(), parentName);
                newCounter.setValue(srcCounter.getValue());
            }

            Set<String> childNames = srcProfile.childCounterMap.get(name);
            if (childNames != null) {
                for (String childName : childNames) {
                    nameQueue.offer(Pair.create(childName, name));
                }
            }
        }
    }

    public void update(final TRuntimeProfileTree thriftProfile) {
        Reference<Integer> idx = new Reference<>(0);
        update(thriftProfile.nodes, idx);
        Preconditions.checkState(idx.getRef().equals(thriftProfile.nodes.size()));
    }

    // preorder traversal, idx should be modified in the traversal process
    private void update(List<TRuntimeProfileNode> nodes, Reference<Integer> idx) {
        TRuntimeProfileNode node = nodes.get(idx.getRef());

        // update this level's counters
        if (node.counters != null) {
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
                                addCounter(topName, tcounter.type, parentName);
                        counter.setValue(tcounter.value);
                        counter.setSkipMerge(tcounter.skip_merge);
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
                    Counter counter = addCounter(tcounter.name, tcounter.type);
                    counter.setValue(tcounter.value);
                    counter.setSkipMerge(tcounter.skip_merge);
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

        if (node.info_strings_display_order != null) {
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
            childProfile.update(nodes, idx);
        }
    }

    // Print the profile:
    //  1. Profile Name
    //  2. Info Strings
    //  3. Counters
    //  4. Children
    public void prettyPrint(StringBuilder builder, String prefix) {
        Pair<Counter, String> pair = this.counterMap.get("TotalTime");
        Preconditions.checkState(pair != null);
        // 1. profile name
        builder.append(prefix).append(name).append(":");
        // total time
        if (pair.first.getValue() != 0) {
            try (Formatter fmt = new Formatter()) {
                builder.append("(Active: ")
                        .append(this.printCounter(pair.first.getValue(), pair.first.getType()));
                if (DebugUtil.THOUSAND < pair.first.getValue()) {
                    // TotalTime in nanosecond concated if it's larger than 1000ns
                    builder.append("[").append(pair.first.getValue()).append("ns]");
                }
                builder.append(", % non-child: ").append(fmt.format("%.2f", localTimePercent))
                        .append("%)");
            }
        }
        builder.append("\n");

        // 2. info String
        for (Map.Entry<String, String> infoPair : this.infoStrings.entrySet()) {
            String key = infoPair.getKey();
            builder.append(prefix).append("   - ").append(key).append(": ")
                    .append(this.infoStrings.get(key)).append("\n");
        }

        // 3. counters
        printChildCounters(prefix, ROOT_COUNTER, builder);

        // 4. children
        for (Pair<RuntimeProfile, Boolean> childPair : childList) {
            boolean indent = childPair.second;
            RuntimeProfile profile = childPair.first;
            profile.prettyPrint(builder, prefix + (indent ? "  " : ""));
        }
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        prettyPrint(builder, "");
        return builder.toString();
    }

    private void printChildCounters(String prefix, String counterName, StringBuilder builder) {
        if (childCounterMap.get(counterName) == null) {
            return;
        }
        List<String> childNames = Lists.newArrayList(childCounterMap.get(counterName));
        childNames.sort(String::compareTo);

        // Keep MIN/MAX metrics head of other child counters
        List<String> minMaxChildNames = Lists.newArrayListWithCapacity(2);
        List<String> otherChildNames = Lists.newArrayListWithCapacity(childNames.size());
        for (String childName : childNames) {
            if (childName.startsWith(MERGED_INFO_PREFIX_MIN) || childName.startsWith(MERGED_INFO_PREFIX_MAX)) {
                minMaxChildNames.add(childName);
            } else {
                otherChildNames.add(childName);
            }
        }
        List<String> reorderedChildNames = Lists.newArrayListWithCapacity(childNames.size());
        reorderedChildNames.addAll(minMaxChildNames);
        reorderedChildNames.addAll(otherChildNames);

        for (String childName : reorderedChildNames) {
            Pair<Counter, String> pair = this.counterMap.get(childName);
            Preconditions.checkState(pair != null);
            builder.append(prefix).append("   - ").append(childName).append(": ")
                    .append(printCounter(pair.first.getValue(), pair.first.getType())).append("\n");
            this.printChildCounters(prefix + "  ", childName, builder);
        }
    }

    private String printCounter(long value, TUnit type) {
        StringBuilder builder = new StringBuilder();
        long tmpValue = value;
        switch (type) {
            case UNIT: {
                Pair<Double, String> pair = DebugUtil.getUint(tmpValue);
                if (pair.second.isEmpty()) {
                    builder.append(tmpValue);
                } else {
                    builder.append(pair.first).append(pair.second)
                            .append(" (").append(tmpValue).append(")");
                }
                break;
            }
            case TIME_NS: {
                if (tmpValue >= DebugUtil.BILLION) {
                    // If the time is over a second, print it up to ms.
                    tmpValue /= DebugUtil.MILLION;
                    DebugUtil.printTimeMs(tmpValue, builder);
                } else if (tmpValue >= DebugUtil.MILLION) {
                    // if the time is over a ms, print it up to microsecond in the unit of ms.
                    tmpValue /= 1000;
                    builder.append(tmpValue / 1000).append(".").append(tmpValue % 1000).append("ms");
                } else if (tmpValue > 1000) {
                    // if the time is over a microsecond, print it using unit microsecond
                    builder.append(tmpValue / 1000).append(".").append(tmpValue % 1000).append("us");
                } else {
                    builder.append(tmpValue).append("ns");
                }
                break;
            }
            case BYTES: {
                Pair<Double, String> pair = DebugUtil.getByteUint(tmpValue);
                Formatter fmt = new Formatter();
                builder.append(fmt.format("%.2f", pair.first)).append(" ").append(pair.second);
                fmt.close();
                break;
            }
            case BYTES_PER_SECOND: {
                Pair<Double, String> pair = DebugUtil.getByteUint(tmpValue);
                builder.append(pair.first).append(" ").append(pair.second).append("/sec");
                break;
            }
            case DOUBLE_VALUE: {
                Formatter fmt = new Formatter();
                builder.append(fmt.format("%.2f", (double) tmpValue));
                fmt.close();
                break;
            }
            case UNIT_PER_SECOND: {
                Pair<Double, String> pair = DebugUtil.getUint(tmpValue);
                if (pair.second.isEmpty()) {
                    builder.append(tmpValue);
                } else {
                    builder.append(pair.first).append(pair.second)
                            .append(" ").append("/sec");
                }
                break;
            }
            default: {
                Preconditions.checkState(false, "type=" + type);
                break;
            }
        }
        return builder.toString();
    }

    public void addChild(RuntimeProfile child) {
        if (child == null) {
            return;
        }

        childMap.put(child.name, child);
        Pair<RuntimeProfile, Boolean> pair = Pair.create(child, true);
        childList.add(pair);
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
    public void copyAllInfoStringsFrom(RuntimeProfile srcProfile) {
        if (srcProfile == null || this == srcProfile) {
            return;
        }

        srcProfile.infoStrings.forEach((key, value) -> {
            if (!this.infoStrings.containsKey(key)) {
                this.infoStrings.put(key, value);
            } else if (!Objects.equals(value, this.infoStrings.get(key))) {
                String originalKey = key;
                int pos;
                if ((pos = key.indexOf("__DUP(")) != -1) {
                    originalKey = key.substring(0, pos);
                }
                int i = 0;
                while (true) {
                    String indexedKey = String.format("%s__DUP(%d)", originalKey, i++);
                    if (!this.infoStrings.containsKey(indexedKey)) {
                        this.infoStrings.put(indexedKey, value);
                        break;
                    }
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

    // Returns the value to which the specified key is mapped;
    // or null if this map contains no mapping for the key.
    public String getInfoString(String key) {
        return infoStrings.get(key);
    }

    // Merge all the isomorphic sub profiles and the caller must know for sure
    // that all the children are isomorphic, otherwise, the behavior is undefined
    // The merged result will be stored in the first profile
    public static void mergeIsomorphicProfiles(List<RuntimeProfile> profiles) {
        if (CollectionUtils.isEmpty(profiles)) {
            return;
        }

        RuntimeProfile profile0 = profiles.get(0);

        for (int i = 1; i < profiles.size(); i++) {
            profile0.copyAllInfoStringsFrom(profiles.get(i));
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
                    Preconditions.checkNotNull(pair);
                    Counter counter = pair.first;
                    if (counter.isSkipMerge()) {
                        continue;
                    }
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
                                "find non-isomorphic counter, profileName={}, counterName={}, existType={}, anotherType={}",
                                profile0.name, name, existType.name(), counter.getType().name());
                        return;
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
                            profile0.name, name, type.name(), counter.getType().name());
                    return;
                }
                if (counter.isSkipMerge()) {
                    skipMerge = true;
                }

                Counter minCounter = profile.getCounter(MERGED_INFO_PREFIX_MIN + name);
                if (minCounter != null) {
                    alreadyMerged = true;
                    if (minCounter.getValue() < minValue) {
                        minValue = minCounter.getValue();
                    }
                }
                Counter maxCounter = profile.getCounter(MERGED_INFO_PREFIX_MAX + name);
                if (maxCounter != null) {
                    alreadyMerged = true;
                    if (maxCounter.getValue() > maxValue) {
                        maxValue = maxCounter.getValue();
                    }
                }
                counters.add(counter);
            }
            Counter.MergedInfo mergedInfo = Counter.mergeIsomorphicCounters(type, counters);
            final long mergedValue = mergedInfo.mergedValue;
            if (!alreadyMerged) {
                minValue = mergedInfo.minValue;
                maxValue = mergedInfo.maxValue;
            }

            Counter counter0 = profile0.getCounter(name);
            // As stated before, some counters may only attach to one of the isomorphic profiles
            // and the first profile may not have this counter, so we create a counter here
            if (counter0 == null) {
                if (!Objects.equals(ROOT_COUNTER, parentName) && profile0.getCounter(parentName) != null) {
                    counter0 = profile0.addCounter(name, type, parentName);
                } else {
                    if (!Objects.equals(ROOT_COUNTER, parentName)) {
                        LOG.warn("missing parent counter, profileName={}, counterName={}, parentCounterName={}",
                                profile0.name, name, parentName);
                    }
                    counter0 = profile0.addCounter(name, type);
                }
                if (skipMerge) {
                    counter0.setSkipMerge(true);
                }
            }
            counter0.setValue(mergedValue);

            Counter minCounter = profile0.addCounter(MERGED_INFO_PREFIX_MIN + name, type, name);
            Counter maxCounter = profile0.addCounter(MERGED_INFO_PREFIX_MAX + name, type, name);
            minCounter.setValue(minValue);
            maxCounter.setValue(maxValue);
        }

        // merge children
        for (int i = 0; i < profile0.childList.size(); i++) {
            List<RuntimeProfile> subProfiles = Lists.newArrayList();
            RuntimeProfile child0 = profile0.childList.get(i).first;
            subProfiles.add(child0);
            for (int j = 1; j < profiles.size(); j++) {
                RuntimeProfile profile = profiles.get(j);
                if (i >= profile.childList.size()) {
                    LOG.warn("find non-isomorphic children, profileName={}, childProfileNames={}" +
                                    ", another profileName={}, another childProfileNames={}",
                            profile0.name,
                            profile0.childList.stream().map(p -> p.first.getName()).collect(Collectors.toList()),
                            profile.name,
                            profile.childList.stream().map(p -> p.first.getName()).collect(Collectors.toList()));
                    return;
                }
                RuntimeProfile child = profile.childList.get(i).first;
                subProfiles.add(child);
            }
            mergeIsomorphicProfiles(subProfiles);
        }
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
}
