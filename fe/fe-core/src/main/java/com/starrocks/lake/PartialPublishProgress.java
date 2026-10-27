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

package com.starrocks.lake;

import com.starrocks.catalog.Tablet;
import com.starrocks.proto.TabletStatPB;
import com.starrocks.proto.VectorIndexBuildInfoPB;
import com.starrocks.system.ComputeNode;
import com.starrocks.transaction.PartitionCommitInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * What earlier attempts at publishing one version range of one partition already got done.
 * <p>
 * A shared-data publish of a partition fans out to every compute node that owns one of its tablets.
 * Each node persists the metadata of the tablets it did publish; a tablet that is still being applied
 * from an earlier request, or that did not fit in the deadline, comes back in {@code failed_tablets}.
 * Without this record the next attempt resent the whole partition and every node re-answered for
 * tablets it had long finished. With it the next attempt sends only {@link #pendingTablets} and seeds
 * its result maps from {@link #restoreInto}, so the final success still carries every tablet's
 * compaction score, tablet stats and txn-log owner.
 * <p>
 * Keyed by version range: a batch whose composition changed (different start or end version) must
 * republish everything, because the already-published metadata belongs to the old end version.
 * Lives on the {@link PartitionCommitInfo}s of the batch, next to the retry back-off stamp, and is
 * never serialized.
 * <p>
 * Only the per-node publish path uses it. The aggregate (file bundling) publish writes one metadata
 * bundle for the whole partition, so it stays all-or-nothing.
 */
public class PartialPublishProgress {
    private final long baseVersion;
    private final long newVersion;
    private final Set<Long> publishedTabletIds = new HashSet<>();
    private final Map<Long, Double> compactionScores = new HashMap<>();
    private final Map<Long, TabletStatPB> tabletStats = new HashMap<>();
    private final List<VectorIndexBuildInfoPB> vectorIndexBuildInfos = new ArrayList<>();
    private final Map<ComputeNode, Set<Long>> nodeToTablets = new HashMap<>();

    public PartialPublishProgress(long baseVersion, long newVersion) {
        this.baseVersion = baseVersion;
        this.newVersion = newVersion;
    }

    public long getBaseVersion() {
        return baseVersion;
    }

    public long getNewVersion() {
        return newVersion;
    }

    public boolean covers(long baseVersion, long newVersion) {
        return this.baseVersion == baseVersion && this.newVersion == newVersion;
    }

    public Set<Long> getPublishedTabletIds() {
        return Collections.unmodifiableSet(publishedTabletIds);
    }

    /**
     * Records one attempt: every tablet in {@code sentTablets} that is not in {@code failedTabletIds}
     * is now published. The maps and the list are what the attempt collected; entries keyed by a
     * failed tablet are dropped. {@code nodeToTablets} already names only published tablets (see
     * {@link Utils#publishVersionBatch}), it is copied as is.
     */
    public void recordAttempt(Collection<Tablet> sentTablets, Set<Long> failedTabletIds,
                              Map<Long, Double> compactionScores, Map<Long, TabletStatPB> tabletStats,
                              List<VectorIndexBuildInfoPB> vectorIndexBuildInfos,
                              Map<ComputeNode, List<Long>> nodeToTablets) {
        for (Tablet tablet : sentTablets) {
            if (!failedTabletIds.contains(tablet.getId())) {
                publishedTabletIds.add(tablet.getId());
            }
        }
        if (compactionScores != null) {
            for (Map.Entry<Long, Double> entry : compactionScores.entrySet()) {
                if (!failedTabletIds.contains(entry.getKey())) {
                    this.compactionScores.put(entry.getKey(), entry.getValue());
                }
            }
        }
        if (tabletStats != null) {
            for (Map.Entry<Long, TabletStatPB> entry : tabletStats.entrySet()) {
                if (!failedTabletIds.contains(entry.getKey())) {
                    this.tabletStats.put(entry.getKey(), entry.getValue());
                }
            }
        }
        if (vectorIndexBuildInfos != null) {
            // A node only reports build info for tablets it did publish, so nothing to filter here.
            // Each attempt's list is seeded from restoreInto, so replace rather than append.
            this.vectorIndexBuildInfos.clear();
            this.vectorIndexBuildInfos.addAll(vectorIndexBuildInfos);
        }
        if (nodeToTablets != null) {
            for (Map.Entry<ComputeNode, List<Long>> entry : nodeToTablets.entrySet()) {
                for (Long tabletId : entry.getValue()) {
                    if (!failedTabletIds.contains(tabletId)) {
                        this.nodeToTablets.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).add(tabletId);
                    }
                }
            }
        }
    }

    /** The tablets of the partition that still have to be sent. */
    public List<Tablet> pendingTablets(Collection<Tablet> allTablets) {
        List<Tablet> pending = new ArrayList<>();
        for (Tablet tablet : allTablets) {
            if (!publishedTabletIds.contains(tablet.getId())) {
                pending.add(tablet);
            }
        }
        return pending;
    }

    /** Seeds the result maps of a new attempt with what earlier attempts collected. */
    public void restoreInto(Map<Long, Double> compactionScores, Map<Long, TabletStatPB> tabletStats,
                            List<VectorIndexBuildInfoPB> vectorIndexBuildInfos,
                            Map<ComputeNode, List<Long>> nodeToTablets) {
        if (compactionScores != null) {
            compactionScores.putAll(this.compactionScores);
        }
        if (tabletStats != null) {
            tabletStats.putAll(this.tabletStats);
        }
        if (vectorIndexBuildInfos != null) {
            for (VectorIndexBuildInfoPB info : this.vectorIndexBuildInfos) {
                if (!vectorIndexBuildInfos.contains(info)) {
                    vectorIndexBuildInfos.add(info);
                }
            }
        }
        if (nodeToTablets != null) {
            for (Map.Entry<ComputeNode, Set<Long>> entry : this.nodeToTablets.entrySet()) {
                List<Long> ids = nodeToTablets.computeIfAbsent(entry.getKey(), k -> new ArrayList<>());
                for (Long tabletId : entry.getValue()) {
                    if (!ids.contains(tabletId)) {
                        ids.add(tabletId);
                    }
                }
            }
        }
    }

    /**
     * The progress any of {@code commitInfos} carries for exactly this version range, or null. The
     * batch is rebuilt from the transaction states every daemon cycle, so the record has to be looked
     * up from the commit infos rather than kept on the batch.
     */
    public static PartialPublishProgress find(List<PartitionCommitInfo> commitInfos, long baseVersion,
                                              long newVersion) {
        for (PartitionCommitInfo commitInfo : commitInfos) {
            PartialPublishProgress progress = commitInfo.getPartialPublishProgress();
            if (progress != null && progress.covers(baseVersion, newVersion)) {
                return progress;
            }
        }
        return null;
    }

    public static void attach(List<PartitionCommitInfo> commitInfos, PartialPublishProgress progress) {
        for (PartitionCommitInfo commitInfo : commitInfos) {
            commitInfo.setPartialPublishProgress(progress);
        }
    }

    public static void clear(List<PartitionCommitInfo> commitInfos) {
        for (PartitionCommitInfo commitInfo : commitInfos) {
            commitInfo.setPartialPublishProgress(null);
        }
    }
}
