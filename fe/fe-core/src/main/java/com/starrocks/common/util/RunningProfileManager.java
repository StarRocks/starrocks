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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.memory.MemoryTrackable;
import com.starrocks.qe.scheduler.QueryRuntimeProfile;
import com.starrocks.thrift.TFragmentProfile;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.starrocks.qe.scheduler.QueryRuntimeProfile.MARKED_COUNT_DOWN_VALUE;

public class RunningProfileManager implements MemoryTrackable {
    private static final Logger LOG = LogManager.getLogger(RunningProfileManager.class);
    private static RunningProfileManager INSTANCE = null;
    private final Map<TUniqueId, RunningProfile> profiles = Maps.newConcurrentMap();
    private static final int MEMORY_PROFILE_SAMPLES = 10;

    private RunningProfileManager() {
    }

    public static RunningProfileManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new RunningProfileManager();
        }
        return INSTANCE;
    }

    @Override
    public Map<String, Long> estimateCount() {
        return ImmutableMap.of("RunningProfile", (long) profiles.size());
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        List<Object> profileSamples = profiles.values()
                .stream()
                .limit(MEMORY_PROFILE_SAMPLES)
                .collect(Collectors.toList());

        return Lists.newArrayList(Pair.create(profileSamples, (long) profiles.size()));
    }

    public RunningProfile getRunningProfile(TUniqueId queryId) {
        return profiles.get(queryId);
    }

    public List<String> getAllRunningQuery() {
        return profiles.keySet().stream().map(key -> DebugUtil.printId(key)).collect(Collectors.toUnmodifiableList());
    }

    public void clearProfiles() {
        profiles.clear();
    }

    public void registerProfile(TUniqueId queryId, RunningProfile queryProfile) {
        LOG.info("registerProfile: queryId: {}", DebugUtil.printId(queryId));
        profiles.putIfAbsent(queryId, queryProfile);
    }

    public void removeProfile(TUniqueId queryId) {
        LOG.info("removeProfile: queryId: {}", DebugUtil.printId(queryId));
        profiles.remove(queryId);
    }

    public TStatus asyncProfileReport(TFragmentProfile request) {
        TStatus status;
        final RunningProfile queryProfile = profiles.get(request.getQuery_id());
        if (queryProfile == null) {
            status = new TStatus(TStatusCode.NOT_FOUND);
            LOG.info("asyncProfileReport query not found: queryId: {}", DebugUtil.printId(request.getQuery_id()));
            status.addToError_msgs("query id " + DebugUtil.printId(request.getQuery_id()) + " not found");
            return status;
        }

        final FragmentInstanceProfile fragmentInstanceProfile =
                queryProfile.fragmentInstanceProfiles.get(request.fragment_instance_id);

        if (fragmentInstanceProfile == null) {
            status = new TStatus(TStatusCode.NOT_FOUND);
            LOG.info("asyncProfileReport query fragment not found: queryId: {}",
                    DebugUtil.printId(request.getQuery_id()));
            status.addToError_msgs("query id " + DebugUtil.printId(request.getQuery_id()) + ", fragment instance id " +
                    DebugUtil.printId(request.fragment_instance_id) + " not found");
            return status;
        }

        boolean shouldProcessDone = false;
        if (request.isDone()) {
            shouldProcessDone = fragmentInstanceProfile.isDone.compareAndSet(false, true);
        } else {
            // if the fragment is already done, we can return directly
            if (fragmentInstanceProfile.isDone.get()) {
                status = new TStatus(TStatusCode.OK);
                return status;
            }
        }

        try {
            LOG.info(
                    "asyncProfileReport, queryid: {}, instanceIdx: {}, isDone:{}, shouldProcessDone:{}, total instance num:{}",
                    DebugUtil.printId(request.getQuery_id()), DebugUtil.printId(request.fragment_instance_id),
                    fragmentInstanceProfile.isDone.toString(), shouldProcessDone,
                    queryProfile.fragmentInstanceProfiles.size());
            queryProfile.tryToTriggerRuntimeProfileUpdate();
            fragmentInstanceProfile.updateRunningProfile(request);
            queryProfile.updateLoadChannelProfile(request);
            
            if (shouldProcessDone) {
                queryProfile.finishInstance(request.fragment_instance_id);
            }
        } catch (Throwable e) {
            LOG.warn("update profile failed {}", DebugUtil.printId(request.getQuery_id()), e);
        }

        return new TStatus(TStatusCode.OK);
    }

    public static class FragmentInstanceProfile {
        // whether this fragment is done
        private final AtomicBoolean isDone;
        private final RuntimeProfile instanceProfile;

        public FragmentInstanceProfile(RuntimeProfile instanceProfile) {
            this.isDone = new AtomicBoolean(false);
            this.instanceProfile = instanceProfile;
        }

        public synchronized void updateRunningProfile(TFragmentProfile request) {
            if (request.isSetProfile()) {
                instanceProfile.update(request.getProfile());
            }
        }
    }

    public static class RunningProfile {
        private final AtomicLong lastRuntimeProfileUpdateTime = new AtomicLong(System.currentTimeMillis());
        private final ProfilingExecPlan profilingExecPlan;
        private final RuntimeProfile executionProfile;
        private final Optional<RuntimeProfile> loadChannelProfile;
        private final int runtimeProfileReportInterval;
        private final boolean needMergeProfile;
        private final boolean isBrokerLoad;
        private MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal;
        // topProfileSupplier is used for running profile to generate top level profile
        // when query is done, topProfileSupplier is null
        private Supplier<RuntimeProfile> topProfileSupplier;
        private Map<TUniqueId, FragmentInstanceProfile> fragmentInstanceProfiles;
        private TUniqueId queryId;

        public RunningProfile(RuntimeProfile executionProfile, Optional<RuntimeProfile> loadChannelProfile,
                              Supplier<RuntimeProfile> topProfileSupplier, ProfilingExecPlan profilingExecPlan,
                              int runtimeProfileReportInterval, boolean needMergeProfile, boolean isisBrokerLoad,
                              TUniqueId queryId) {
            this.executionProfile = executionProfile;
            this.loadChannelProfile = loadChannelProfile;
            this.topProfileSupplier = topProfileSupplier;
            this.profilingExecPlan = profilingExecPlan;
            this.runtimeProfileReportInterval = runtimeProfileReportInterval;
            this.needMergeProfile = needMergeProfile;
            this.isBrokerLoad = isisBrokerLoad;
            this.queryId = queryId;
        }

        public void registerInstanceProfiles(Collection<TUniqueId> instanceIds) {
            profileDoneSignal = new MarkedCountDownLatch<>(instanceIds.size());
            instanceIds.forEach(instanceId -> profileDoneSignal.addMark(instanceId, MARKED_COUNT_DOWN_VALUE));
            fragmentInstanceProfiles = Maps.newConcurrentMap();
        }

        public void addInstanceProfile(TUniqueId instanceId, RuntimeProfile profile) {
            fragmentInstanceProfiles.put(instanceId, new FragmentInstanceProfile(profile));
        }

        public void addListener(Runnable listener) {
            profileDoneSignal.addListener(listener);
        }

        public synchronized void clearTopProfileSupplier() {
            this.topProfileSupplier = null;
        }

        public void tryToTriggerRuntimeProfileUpdate() {
            long now = System.currentTimeMillis();
            long lastTime = lastRuntimeProfileUpdateTime.get();
            synchronized (this) {
                // topProfileSupplier may be null when the query is finished.
                // And here to make sure that runtime profile won't overwrite the final profile
                if (topProfileSupplier == null) {
                    return;
                }
            }

            if (profileDoneSignal.getLeftMarks().size() > 1 &&
                    now - lastTime > runtimeProfileReportInterval * 950L &&
                    lastRuntimeProfileUpdateTime.compareAndSet(lastTime, now)) {
                RuntimeProfile profile = topProfileSupplier.get();
                profile.addChild(
                        QueryRuntimeProfile.mergeExecutionProfile(needMergeProfile || isBrokerLoad, executionProfile,
                                loadChannelProfile, profilingExecPlan, queryId));

                synchronized (this) {
                    // topProfileSupplier may be null when the query is finished.
                    // And here to make sure that runtime profile won't overwrite the final profile
                    if (topProfileSupplier == null) {
                        return;
                    }
                    ProfileManager.getInstance().pushProfile(profilingExecPlan, profile);
                }
            }
        }

        public boolean waitForProfileReported(long timeout, TimeUnit unit) {
            boolean res = false;
            try {
                res = profileDoneSignal.await(timeout, unit);
            } catch (InterruptedException e) { // NOSONAR
                LOG.warn("profile signal await error", e);
            }

            return res;
        }

        public synchronized void updateLoadChannelProfile(TFragmentProfile request) {
            if (request.isSetLoad_channel_profile() && loadChannelProfile.isPresent()) {
                loadChannelProfile.get().update(request.load_channel_profile);
                if (LOG.isDebugEnabled()) {
                    StringBuilder builder = new StringBuilder();
                    loadChannelProfile.get().prettyPrint(builder, "");
                    LOG.info("Load channel profile for query_id={} after reported by instance_id={}\n{}",
                            DebugUtil.printId(request.getQuery_id()),
                            DebugUtil.printId(request.fragment_instance_id),
                            builder);
                }
            }
        }

        public void finishInstance(TUniqueId instanceId) {
            profileDoneSignal.markedCountDown(instanceId, MARKED_COUNT_DOWN_VALUE);
        }

        public RuntimeProfile buildExecutionProfile() {
            return QueryRuntimeProfile.mergeExecutionProfile(needMergeProfile || isBrokerLoad, executionProfile,
                    loadChannelProfile, profilingExecPlan, queryId);
        }

    }

}
