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

import com.google.common.collect.Maps;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.qe.scheduler.QueryRuntimeProfile;
import com.starrocks.thrift.TFragmentProfile;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.starrocks.qe.scheduler.QueryRuntimeProfile.MARKED_COUNT_DOWN_VALUE;

public class RunningProfileManager {
    private static final Logger LOG = LogManager.getLogger(RunningProfileManager.class);
    private static RunningProfileManager INSTANCE = null;
    private final Map<TUniqueId, RunningProfile> profiles = Maps.newConcurrentMap();

    private RunningProfileManager() {
    }

    public static RunningProfileManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new RunningProfileManager();
        }
        return INSTANCE;
    }

    public RunningProfile getRunningProfile(TUniqueId queryId) {
        return profiles.get(queryId);
    }

    public void registerProfile(TUniqueId queryId, RunningProfile queryProfile) {
        // LOG.warn("registerProfile: queryId: {}", DebugUtil.printId(queryId));
        profiles.putIfAbsent(queryId, queryProfile);
    }

    public void removeProfile(TUniqueId queryId) {
        profiles.remove(queryId);
    }

    public TStatus asyncProfileReport(TFragmentProfile request) {
        TStatus status;
        final RunningProfile queryProfile = profiles.get(request.getQuery_id());
        if (queryProfile == null) {
            status = new TStatus(TStatusCode.NOT_FOUND);
            LOG.warn("asyncProfileReport query not found: queryId: {}", DebugUtil.printId(request.getQuery_id()));
            status.addToError_msgs("query id " + DebugUtil.printId(request.getQuery_id()) + " not found");
            return status;
        }

        final FragmentInstanceProfile fragmentInstanceProfile =
                queryProfile.fragmentInstanceProfiles.get(request.fragment_instance_id);

        if (fragmentInstanceProfile == null) {
            status = new TStatus(TStatusCode.NOT_FOUND);
            LOG.warn("asyncProfileReport query fragment not found: queryId: {}", DebugUtil.printId(request.getQuery_id()));
            status.addToError_msgs("query id " + DebugUtil.printId(request.getQuery_id()) + ", fragment instance id " +
                    DebugUtil.printId(request.fragment_instance_id) + " not found");
            return status;
        }

        Boolean instanceIsDone = fragmentInstanceProfile.isDone.get();
        if (instanceIsDone) {
            status = new TStatus(TStatusCode.OK);
            return status;
        }

        if (request.isDone()) {
            fragmentInstanceProfile.isDone.set(true);
        }

        try {
            //            LOG.warn("asyncProfileReport, queryid: {}, instanceIndex: {}, isDone:{}",
            //                    DebugUtil.printId(request.getQuery_id()), DebugUtil.printId(request.fragment_instance_id),
            //                    fragmentInstanceProfile.isDone.toString());
            queryProfile.tryToTriggerRuntimeProfileUpdate();
            fragmentInstanceProfile.updateRunningProfile(request);
            queryProfile.updateLoadChannelProfile(request);
            if (fragmentInstanceProfile.isDone.get()) {
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

        public void updateRunningProfile(TFragmentProfile request) {
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
        }

        public void registerInstanceProfiles(Collection<TUniqueId> instanceIds) {
            profileDoneSignal = new MarkedCountDownLatch<>(instanceIds.size());
            instanceIds.forEach(instanceId -> profileDoneSignal.addMark(instanceId, MARKED_COUNT_DOWN_VALUE));
            fragmentInstanceProfiles = Maps.newConcurrentMap();
            //            indexInJobToExecState.forEach((index, execState) -> {
            //                fragmentInstanceProfiles.putIfAbsent(index, new FragmentInstanceProfile(execState.getProfile()));
            //                profileDoneSignal.addMark(index, MARKED_COUNT_DOWN_VALUE);
            //            });

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
            Supplier<RuntimeProfile> topProfileSupplier = null;
            synchronized (this) {
                topProfileSupplier = this.topProfileSupplier;
            }
            if (topProfileSupplier != null && profileDoneSignal.getLeftMarks().size() > 1 &&
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
                }
                ProfileManager.getInstance().pushProfile(profilingExecPlan, profile);
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

        public void updateLoadChannelProfile(TFragmentProfile request) {
            if (request.isSetLoad_channel_profile() && loadChannelProfile.isPresent()) {
                loadChannelProfile.get().update(request.load_channel_profile);
                if (LOG.isDebugEnabled()) {
                    StringBuilder builder = new StringBuilder();
                    loadChannelProfile.get().prettyPrint(builder, "");
                    LOG.debug("Load channel profile for query_id={} after reported by instance_id={}\n{}",
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
