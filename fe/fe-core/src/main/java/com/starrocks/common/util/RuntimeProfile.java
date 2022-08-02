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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * It is accessed by two kinds of thread, one is to create this RuntimeProfile
 * , named 'query thread', the other is to call
 * {@link com.starrocks.common.proc.CurrentQueryInfoProvider}.
 */
public class RuntimeProfile {
    private static final Logger LOG = LogManager.getLogger(RuntimeProfile.class);
    private static final String ROOT_COUNTER = "";
    private static final Set<String> NON_MERGE_COUNTER_NAMES =
            Sets.newHashSet("DegreeOfParallelism", "RuntimeBloomFilterNum", "RuntimeInFilterNum", "PushdownPredicates");

    private final Counter counterTotalTime;

    private final Map<String, String> infoStrings = Maps.newHashMap();
    private final List<String> infoStringsDisplayOrder = Lists.newArrayList();

    // These will be hold by other thread.
    private final Map<String, Counter> counterMap = Maps.newConcurrentMap();
    private final Map<String, RuntimeProfile> childMap = Maps.newConcurrentMap();

    private final Map<String, TreeSet<String>> childCounterMap = Maps.newConcurrentMap();
    private final List<Pair<RuntimeProfile, Boolean>> childList = Lists.newArrayList();

    private String name;
    private double localTimePercent;

    public RuntimeProfile(String name) {
        this();
        this.name = name;
    }

    public RuntimeProfile() {
        this.counterTotalTime = new Counter(TUnit.TIME_NS, 0);
        this.localTimePercent = 0;
        this.counterMap.put("TotalTime", counterTotalTime);
    }

    public Counter getCounterTotalTime() {
        return counterTotalTime;
    }

    public Map<String, Counter> getCounterMap() {
        return counterMap;
    }

    public List<Pair<RuntimeProfile, Boolean>> getChildList() {
        return childList;
    }

    public Map<String, RuntimeProfile> getChildMap() {
        return childMap;
    }

    public void removeAllChildren() {
        childList.clear();
        childMap.clear();
    }

    public Counter addCounter(String name, TUnit type) {
        return addCounter(name, type, ROOT_COUNTER);
    }

    public Counter addCounter(String name, TUnit type, String parentCounterName) {
        Counter counter = this.counterMap.get(name);
        if (counter != null) {
            return counter;
        } else {
            Preconditions.checkState(parentCounterName.equals(ROOT_COUNTER)
                    || this.counterMap.containsKey(parentCounterName));
            Counter newCounter = new Counter(type, 0);
            this.counterMap.put(name, newCounter);

            TreeSet<String> childCounters = childCounterMap.getOrDefault(parentCounterName, new TreeSet<>());
            childCounters.add(name);
            childCounterMap.put(parentCounterName, childCounters);
            return newCounter;
        }
    }

    public Counter getCounter(String name) {
        return counterMap.get(name);
    }

    // Copy all the counters from src profile
    public void copyAllCountersFrom(RuntimeProfile srcProfile) {
        if (srcProfile == null || this == srcProfile) {
            return;
        }

        srcProfile.counterMap.forEach((name, counter) -> {
            addCounter(name, counter.getType());
            getCounter(name).setValue(counter.getValue());
        });
    }

    public void update(final TRuntimeProfileTree thriftProfile) {
        Reference<Integer> idx = new Reference<Integer>(0);
        update(thriftProfile.nodes, idx);
        Preconditions.checkState(idx.getRef().equals(thriftProfile.nodes.size()));
    }

    // preorder traversal, idx should be modified in the traversal process
    private void update(List<TRuntimeProfileNode> nodes, Reference<Integer> idx) {
        TRuntimeProfileNode node = nodes.get(idx.getRef());

        // update this level's counters
        if (node.counters != null) {
            for (TCounter tcounter : node.counters) {
                Counter counter = counterMap.get(tcounter.name);
                if (counter == null) {
                    counterMap.put(tcounter.name, new Counter(tcounter.type, tcounter.value));
                } else {
                    if (counter.getType() != tcounter.type) {
                        LOG.error("Cannot update counters with the same name but different types"
                                + " type=" + tcounter.type);
                    } else {
                        counter.setValue(tcounter.value);
                    }
                }
            }

            if (node.child_counters_map != null) {
                // update childCounters
                for (Map.Entry<String, Set<String>> entry : node.child_counters_map.entrySet()) {
                    String parentCounterName = entry.getKey();
                    TreeSet<String> childCounters = childCounterMap.getOrDefault(parentCounterName, new TreeSet<>());
                    childCounters.addAll(entry.getValue());
                    childCounterMap.put(parentCounterName, childCounters);
                }
            }
        }

        if (node.info_strings_display_order != null) {
            Map<String, String> nodeInfoStrings = node.info_strings;
            for (String key : node.info_strings_display_order) {
                String value = nodeInfoStrings.get(key);
                Preconditions.checkState(value != null);
                if (this.infoStrings.containsKey(key)) {
                    // exists then replace
                    this.infoStrings.put(key, value);
                } else {
                    this.infoStrings.put(key, value);
                    this.infoStringsDisplayOrder.add(key);
                }
            }
        }

        idx.setRef(idx.getRef() + 1);

        for (int i = 0; i < node.num_children; i++) {
            TRuntimeProfileNode tchild = nodes.get(idx.getRef());
            String childName = tchild.name;
            RuntimeProfile childProfile = this.childMap.get(childName);
            if (childProfile == null) {
                childMap.put(childName, new RuntimeProfile(childName));
                childProfile = this.childMap.get(childName);
                Pair<RuntimeProfile, Boolean> pair = Pair.create(childProfile, tchild.indent);
                this.childList.add(pair);
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
        Counter counter = this.counterMap.get("TotalTime");
        Preconditions.checkState(counter != null);
        // 1. profile name
        builder.append(prefix).append(name).append(":");
        // total time
        if (counter.getValue() != 0) {
            try (Formatter fmt = new Formatter()) {
                builder.append("(Active: ")
                        .append(this.printCounter(counter.getValue(), counter.getType()));
                if (DebugUtil.THOUSAND < counter.getValue()) {
                    // TotalTime in nanosecond concated if it's larger than 1000ns
                    builder.append("[").append(counter.getValue()).append("ns]");
                }
                builder.append(", % non-child: ").append(fmt.format("%.2f", localTimePercent))
                        .append("%)");
            }
        }
        builder.append("\n");

        // 2. info String
        for (String key : this.infoStringsDisplayOrder) {
            builder.append(prefix).append("   - ").append(key).append(": ")
                    .append(this.infoStrings.get(key)).append("\n");
        }

        // 3. counters
        printChildCounters(prefix, ROOT_COUNTER, builder);

        // 4. children
        for (int i = 0; i < childList.size(); i++) {
            Pair<RuntimeProfile, Boolean> pair = childList.get(i);
            boolean indent = pair.second;
            RuntimeProfile profile = pair.first;
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
        Set<String> childCounterSet = new TreeSet<>(childCounterMap.get(counterName));

        for (String childCounterName : childCounterSet) {
            Counter counter = this.counterMap.get(childCounterName);
            Preconditions.checkState(counter != null);
            builder.append(prefix).append("   - ").append(childCounterName).append(": ")
                    .append(printCounter(counter.getValue(), counter.getType())).append("\n");
            this.printChildCounters(prefix + "  ", childCounterName, builder);
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

        this.childMap.put(child.name, child);
        Pair<RuntimeProfile, Boolean> pair = Pair.create(child, true);
        this.childList.add(pair);
    }

    // Because the profile of summary and child fragment is not a real parent-child relationship
    // Each child profile needs to calculate the time proportion consumed by itself
    public void computeTimeInChildProfile() {
        childMap.values().stream().
                forEach(child -> child.computeTimeInProfile());
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

        for (int i = 0; i < this.childList.size(); ++i) {
            totalChildTime += childList.get(i).first.getCounterTotalTime().getValue();
        }
        long localTime = this.getCounterTotalTime().getValue() - totalChildTime;
        // Counters have some margin, set to 0 if it was negative.
        localTime = Math.max(0, localTime);
        this.localTimePercent = Double.valueOf(localTime) / Double.valueOf(total);
        this.localTimePercent = Math.min(1.0, this.localTimePercent) * 100;

        // Recurse on children
        for (int i = 0; i < this.childList.size(); i++) {
            childList.get(i).first.computeTimeInProfile(total);
        }
    }

    // from bigger to smaller
    public void sortChildren() {
        Collections.sort(this.childList, new Comparator<Pair<RuntimeProfile, Boolean>>() {
            @Override
            public int compare(Pair<RuntimeProfile, Boolean> profile1, Pair<RuntimeProfile, Boolean> profile2) {
                return Long.compare(profile2.first.getCounterTotalTime().getValue(),
                        profile1.first.getCounterTotalTime().getValue());
            }
        });
    }

    public void addInfoString(String key, String value) {
        String target = this.infoStrings.get(key);
        if (target == null) {
            this.infoStrings.put(key, value);
            this.infoStringsDisplayOrder.add(key);
        } else {
            this.infoStrings.put(key, value);
        }
    }

    // Copy all info strings from src profile
    public void copyAllInfoStringsFrom(RuntimeProfile srcProfile) {
        if (srcProfile == null || this == srcProfile) {
            return;
        }

        this.infoStrings.putAll(srcProfile.infoStrings);
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
        final String mergedInfoPrefixMin = "__MIN_OF_";
        final String mergedInfoPrefixMax = "__MAX_OF_";

        // merge counters
        Map<String, TUnit> counterTypes = Maps.newTreeMap();
        for (RuntimeProfile profile : profiles) {
            for (Map.Entry<String, Counter> entry : profile.counterMap.entrySet()) {
                String name = entry.getKey();
                Counter counter = entry.getValue();

                if (NON_MERGE_COUNTER_NAMES.contains(name)) {
                    continue;
                }

                if (!counterTypes.containsKey(name)) {
                    counterTypes.put(name, counter.getType());
                    continue;
                }
                TUnit existType = counterTypes.get(name);
                if (!existType.equals(counter.getType())) {
                    LOG.warn(
                            "find non-isomorphic counter, profileName={}, counterName={}, existType={}, anotherType={}",
                            profile0.name, name, existType.name(), counter.getType().name());
                    return;
                }
            }
        }

        for (Map.Entry<String, TUnit> entry : counterTypes.entrySet()) {
            String name = entry.getKey();
            TUnit type = entry.getValue();

            // We don't need to calculate sum or average of counter's extra info (min value and max value) created by be
            if (name.startsWith(mergedInfoPrefixMin) || name.startsWith(mergedInfoPrefixMax)) {
                continue;
            }

            List<Counter> counters = Lists.newArrayList();
            long minValue = Long.MAX_VALUE;
            long maxValue = Long.MIN_VALUE;
            boolean alreadyMerged = false;
            for (RuntimeProfile profile : profiles) {
                Counter counter = profile.getCounter(name);

                // Allow some of the counters only attach to one of the isomorphic profiles
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

                Counter minCounter = profile.getCounter(mergedInfoPrefixMin + name);
                if (minCounter != null) {
                    alreadyMerged = true;
                    if (minCounter.getValue() < minValue) {
                        minValue = minCounter.getValue();
                    }
                }
                Counter maxCounter = profile.getCounter(mergedInfoPrefixMax + name);
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
            // As memtioned before, some counters may only attach to one of the isomorphic profiles
            // and the first profile may not have this counter, so we create a counter here
            if (counter0 == null) {
                counter0 = profile0.addCounter(name, type);
            }
            counter0.setValue(mergedValue);

            boolean updateMinMax = false;
            if (alreadyMerged) {
                updateMinMax = true;
            } else {
                // If the values vary greatly, we need to save extra info (min value and max value) of this counter
                double diff = maxValue - minValue;
                if (Counter.isAverageType(counter0.getType())) {
                    if (diff > 5000000L && diff > mergedValue / 5.0) {
                        updateMinMax = true;
                    }
                } else {
                    // All sum type counters will have extra info (min value and max value)
                    updateMinMax = true;
                }
            }
            if (updateMinMax) {
                Counter minCounter = profile0.addCounter(mergedInfoPrefixMin + name, type, name);
                Counter maxCounter = profile0.addCounter(mergedInfoPrefixMax + name, type, name);
                minCounter.setValue(minValue);
                maxCounter.setValue(maxValue);
            }
        }

        // merge children
        for (int i = 0; i < profile0.childList.size(); i++) {
            List<RuntimeProfile> subProfiles = Lists.newArrayList();
            RuntimeProfile child0 = profile0.childList.get(i).first;
            subProfiles.add(child0);
            for (int j = 1; j < profiles.size(); j++) {
                RuntimeProfile profile = profiles.get(j);
                if (i >= profile.childList.size()) {
                    LOG.warn("find non-isomorphic children, profileName={}, childNames={}" +
                                    ", another profileName={}, another childNames={}",
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
}
