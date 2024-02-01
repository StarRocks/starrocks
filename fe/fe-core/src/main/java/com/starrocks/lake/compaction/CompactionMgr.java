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

package com.starrocks.lake.compaction;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.io.Text;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class CompactionMgr {
    private static final Logger LOG = LogManager.getLogger(CompactionMgr.class);

    @SerializedName(value = "partitionStatisticsHashMap")
    private ConcurrentHashMap<PartitionIdentifier, PartitionStatistics> partitionStatisticsHashMap =
            new ConcurrentHashMap<>();

    private Selector selector;
    private Sorter sorter;
    private CompactionScheduler compactionScheduler;

    public CompactionMgr() {
        try {
            init();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void init() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException {
        String packageName = CompactionMgr.class.getPackage().getName();
        Class<?> selectorClazz = Class.forName(packageName + "." + Config.lake_compaction_selector);
        selector = (Selector) selectorClazz.getConstructor().newInstance();

        Class<?> sorterClazz = Class.forName(packageName + "." + Config.lake_compaction_sorter);
        sorter = (Sorter) sorterClazz.getConstructor().newInstance();
    }

    public void start() {
        if (compactionScheduler == null) {
            compactionScheduler = new CompactionScheduler(this, GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo(),
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr(), GlobalStateMgr.getCurrentState());
            compactionScheduler.start();
        }
    }

    public void handleLoadingFinished(PartitionIdentifier partition, long version, long versionTime,
                                      Quantiles compactionScore) {
        PartitionVersion currentVersion = new PartitionVersion(version, versionTime);
        PartitionStatistics statistics = partitionStatisticsHashMap.compute(partition, (k, v) -> {
            if (v == null) {
                v = new PartitionStatistics(partition);
            }
            v.setCurrentVersion(currentVersion);
            v.setCompactionScore(compactionScore);
            if (v.getCompactionVersion() == null) {
                v.setCompactionVersion(new PartitionVersion(0, versionTime));
            }
            return v;
        });
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished loading: {}", statistics);
        }
    }

    public void handleCompactionFinished(PartitionIdentifier partition, long version, long versionTime,
                                         Quantiles compactionScore) {
        PartitionVersion compactionVersion = new PartitionVersion(version, versionTime);
        PartitionStatistics statistics = partitionStatisticsHashMap.compute(partition, (k, v) -> {
            if (v == null) {
                v = new PartitionStatistics(partition);
            }
            v.setCurrentVersion(compactionVersion);
            v.setCompactionVersion(compactionVersion);
            v.setCompactionScore(compactionScore);
            return v;
        });
        if (LOG.isDebugEnabled()) {
            LOG.debug("Finished compaction: {}", statistics);
        }
    }

    @NotNull
    List<PartitionIdentifier> choosePartitionsToCompact(@NotNull Set<PartitionIdentifier> excludes) {
        return choosePartitionsToCompact().stream().filter(p -> !excludes.contains(p)).collect(Collectors.toList());
    }

    @NotNull
    List<PartitionIdentifier> choosePartitionsToCompact() {
        List<PartitionStatistics> selection = sorter.sort(selector.select(partitionStatisticsHashMap.values()));
        return selection.stream().map(PartitionStatistics::getPartition).collect(Collectors.toList());
    }

    @NotNull
    Set<PartitionIdentifier> getAllPartitions() {
        return new HashSet<>(partitionStatisticsHashMap.keySet());
    }

    @NotNull
    public Collection<PartitionStatistics> getAllStatistics() {
        return partitionStatisticsHashMap.values();
    }

    @Nullable
    public PartitionStatistics getStatistics(PartitionIdentifier identifier) {
        return partitionStatisticsHashMap.get(identifier);
    }

    public double getMaxCompactionScore() {
        return partitionStatisticsHashMap.values().stream().mapToDouble(stat -> stat.getCompactionScore().getMax())
                .max().orElse(0);
    }

    void enableCompactionAfter(PartitionIdentifier partition, long delayMs) {
        PartitionStatistics statistics = partitionStatisticsHashMap.computeIfPresent(partition, (k, v) -> {
            // FE's follower nodes may have a different timestamp with the leader node.
            v.setNextCompactionTime(System.currentTimeMillis() + delayMs);
            return v;
        });
        if (statistics != null && LOG.isDebugEnabled()) {
            LOG.debug("Enable compaction after {}ms: {}", delayMs, statistics);
        }
    }

    void removePartition(PartitionIdentifier partition) {
        partitionStatisticsHashMap.remove(partition);
    }

    public long saveCompactionManager(DataOutput out, long checksum) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
        checksum ^= getChecksum();
        return checksum;
    }

    public long getChecksum() {
        return partitionStatisticsHashMap.size();
    }

    @NotNull
    public List<CompactionRecord> getHistory() {
        if (compactionScheduler != null) {
            return compactionScheduler.getHistory();
        } else {
            return Collections.emptyList();
        }
    }

    public void cancelCompaction(long txnId) {
        compactionScheduler.cancelCompaction(txnId);
    }

    public static CompactionMgr loadCompactionManager(DataInput in) throws IOException {
        String json = Text.readString(in);
        CompactionMgr compactionManager = GsonUtils.GSON.fromJson(json, CompactionMgr.class);
        try {
            compactionManager.init();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return compactionManager;
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.COMPACTION_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        CompactionMgr compactionManager = reader.readJson(CompactionMgr.class);
        partitionStatisticsHashMap = compactionManager.partitionStatisticsHashMap;
    }

    public long getPartitionStatsCount() {
        return partitionStatisticsHashMap.size();
    }
}
