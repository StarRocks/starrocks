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

package com.starrocks.sql.automv.lifecycle;

import com.google.api.client.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import com.starrocks.sql.automv.util.TieredList;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MVLifecycleManager {

    private static final MVName AUDIT_LATEST_TIMESTAMP_MARK = MVName.generateSpecial("AUDIT_LATEST_TIMESTAMP_MARK");
    private Map<MVName, MVLifecycle> nameToMVLifecycles = Maps.newConcurrentMap();
    private Map<String, Double> mvHitRatioMap = Maps.newConcurrentMap();
    private Map<String, List<MVLifecycle>> digestToMVLifecycles = Maps.newHashMap();
    private List<MVLifecycle> legacyMVLifecycles = Lists.newArrayList();
    private MVChangeLog auditLatestTimestamp = MVChangeLog.genesis(AUDIT_LATEST_TIMESTAMP_MARK);

    private Supplier<MVPhasePolicy> mvPhasePolicySupplier;

    @VisibleForTesting
    public MVLifecycleManager() {
        mvPhasePolicySupplier = () -> MVPhasePolicy.newBuilder().setMVHitRatioProvider(this::getMVHitRatio).build();
    }

    @VisibleForTesting
    public Map<MVName, MVLifecycle> getNameToMVLifecycles() {
        return nameToMVLifecycles;
    }

    public Optional<Long> getAuditLatestTimestamp() {
        if (auditLatestTimestamp.getEntries().isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(auditLatestTimestamp.getLatestEntry().getEnterTime());
        }
    }

    public boolean contains(String digest) {
        return digestToMVLifecycles.containsKey(digest);
    }

    public void replayMVChangeLog(MVChangeLog mvChangeLog) {
        if (mvChangeLog.getMVName().equals(AUDIT_LATEST_TIMESTAMP_MARK)) {
            auditLatestTimestamp = mvChangeLog;
        } else {
            nameToMVLifecycles.merge(mvChangeLog.getMVName(), MVLifecycle.ofDangling(mvChangeLog),
                    MVLifecycle::replaceMVChangeLog);
        }
    }

    public void commitCradle(MVName mvName) {
        MVLifecycle mvLifecycle = MVLifecycle.ofDangling(MVChangeLog.genesis(mvName));
        mvLifecycle.commit(MVPhase.MP_CRADLE);
        addMVLifecycle(mvLifecycle);
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        int numJson = 2 + nameToMVLifecycles.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockIDEPack.MV_LIFECYCLE_MGR, numJson);
        writer.writeInt(nameToMVLifecycles.size() + 1);
        writer.writeJson(auditLatestTimestamp);
        for (MVLifecycle mvLifecycle : nameToMVLifecycles.values()) {
            writer.writeJson(mvLifecycle.getMVChangeLog());
        }
        writer.close();
    }

    public Double getMVHitRatio(String mvName) {
        return mvHitRatioMap.get(mvName);
    }

    public void populateMVHitRatio(ConcurrentMap<String, Double> mvHitRatioMap) {
        this.mvHitRatioMap = mvHitRatioMap;
    }

    public void updateAuditLatestTimestamp(long ts) {
        TieredList<MVChangeLog.Entry> entries = TieredList.<MVChangeLog.Entry>genesis()
                .concatOne(new MVChangeLog.Entry(ts, MVPhase.MP_CRADLE, MVPhase.MP_CRADLE));
        auditLatestTimestamp = new MVChangeLog(AUDIT_LATEST_TIMESTAMP_MARK, entries);
        auditLatestTimestamp.persist();
    }

    @VisibleForTesting
    public MVChangeLog getAuditLatestTimestampChangeLog() {
        return auditLatestTimestamp;
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int numJson = reader.readInt();
        try {
            for (int i = 0; i < numJson; ++i) {
                MVChangeLog mvChangeLog = reader.readJson(MVChangeLog.Builder.class).build();
                if (mvChangeLog.getMVName().equals(AUDIT_LATEST_TIMESTAMP_MARK)) {
                    auditLatestTimestamp = mvChangeLog;
                } else {
                    nameToMVLifecycles.put(mvChangeLog.getMVName(), MVLifecycle.ofDangling(mvChangeLog));
                }
            }
        } catch (Throwable ignored) {
            nameToMVLifecycles.clear();
        }
    }

    public void associateMVWithLifecycle(List<Pair<MVName, MaterializedViewPlus>> mvList) {
        // if a legacy MV has no MVChangeLog to associate with, we set its phase to be MP_CRADLE .
        legacyMVLifecycles = mvList.stream().map(p -> {
            MVName name = p.first;
            MaterializedViewPlus mvPlus = p.second;
            return nameToMVLifecycles
                    .computeIfAbsent(name, (key) -> MVLifecycle.ofCradle(mvPlus, name))
                    .attach(mvPlus);
        }).collect(Collectors.toList());

        // if a MVLifecycle is danging(no legacy MV to associate with), we detach the deprecated
        // MV from the MVLifecycle if it is present.
        Set<MVName> legacyMVNames = mvList.stream().map(p -> p.first).collect(Collectors.toSet());
        nameToMVLifecycles.values()
                .stream()
                .filter(mvLifecycle -> !legacyMVNames.contains(mvLifecycle.getMVName()))
                .forEach(MVLifecycle::detach);
        // classify the legacy MVs(both alive MVs and dead MVs) into the identical digest groups to
        // prevent the duplicate MV from being created again.
        digestToMVLifecycles = nameToMVLifecycles.values()
                .stream()
                .collect(Collectors.groupingBy(MVLifecycle::getDigest));
    }

    public void addMVLifecycle(MVLifecycle mvLifecycle) {
        nameToMVLifecycles.put(mvLifecycle.getMVName(), mvLifecycle);
        if (mvLifecycle.isAttached()) {
            legacyMVLifecycles.add(mvLifecycle);
        }
        digestToMVLifecycles.computeIfAbsent(mvLifecycle.getDigest(), (key) -> Lists.newArrayList())
                .add(mvLifecycle);
    }

    private void sweepExtinctionMVLifecycles(MVPhasePolicy policy) {
        Map<Boolean, List<MVLifecycle>> mvLifecycleGroups = nameToMVLifecycles.values()
                .stream()
                .collect(Collectors.partitioningBy(mvLifecycle ->
                        policy.getExceedExtinctionRetentionMaxTimeDictator().test(mvLifecycle)));

        List<MVLifecycle> erasingGroups = mvLifecycleGroups.get(true);
        List<MVLifecycle> retainingGroups = mvLifecycleGroups.get(false);
        if (erasingGroups.isEmpty()) {
            // nameToMVLifecycles need not to be changed
        } else if (erasingGroups.size() == nameToMVLifecycles.size()) {
            nameToMVLifecycles = new ConcurrentHashMap<>();
            legacyMVLifecycles = Lists.newArrayList();
            digestToMVLifecycles = Maps.newHashMap();
            mvHitRatioMap = new ConcurrentHashMap<>();
        } else {
            nameToMVLifecycles = retainingGroups
                    .stream()
                    .collect(Collectors.toConcurrentMap(MVLifecycle::getMVName, Function.identity()));

            legacyMVLifecycles = retainingGroups
                    .stream()
                    .filter(MVLifecycle::isAttached)
                    .collect(Collectors.toList());

            digestToMVLifecycles = retainingGroups
                    .stream()
                    .collect(Collectors.groupingBy(mvLifecycle -> mvLifecycle.getMVName().getDigest()));

            Set<String> erasedMVs = retainingGroups.stream()
                    .map(MVLifecycle::getMVName)
                    .map(MVName::toString)
                    .collect(Collectors.toSet());
            mvHitRatioMap = mvHitRatioMap.entrySet().stream()
                    .filter(e -> erasedMVs.contains(e.getKey()))
                    .collect(Collectors.toConcurrentMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    public void scanMVLifecycles() {
        MVPhasePolicy policy = getMVPhasePolicySupplier().get();
        sweepExtinctionMVLifecycles(policy);
        for (MVLifecycle mvLifecycle : nameToMVLifecycles.values()) {
            policy.transfer(mvLifecycle);
        }
    }

    public Supplier<MVPhasePolicy> getMVPhasePolicySupplier() {
        return mvPhasePolicySupplier;
    }

    @VisibleForTesting
    public void setMVPhasePolicySupplier(Supplier<MVPhasePolicy> mvPhasePolicySupplier) {
        this.mvPhasePolicySupplier = mvPhasePolicySupplier;
    }

}
