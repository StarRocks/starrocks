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

package com.starrocks.qe.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.Counter;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.ProfilingExecPlan;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.dag.FragmentInstanceExecState;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUnit;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class QueryRuntimeProfile {
    private static final Logger LOG = LogManager.getLogger(QueryRuntimeProfile.class);

    /**
     * Set the queue size to a large value. The decision to execute the profile process task asynchronously
     * occurs when a listener is added to {@link QueryRuntimeProfile#profileDoneSignal}. The function
     * {@link QueryRuntimeProfile#addListener} will then determine if the size of the queued task exceeds
     * {@link Config#profile_process_blocking_queue_size}.
     */
    private static final ThreadPoolExecutor EXECUTOR =
            ThreadPoolManager.newDaemonFixedThreadPool(Config.profile_process_threads_num,
                    Integer.MAX_VALUE, "profile-worker", false);

    /**
     * The value is meaningless, and it is just used as a value placeholder of {@link MarkedCountDownLatch}.
     */
    private static final Long MARKED_COUNT_DOWN_VALUE = -1L;

    private final JobSpec jobSpec;

    private final ConnectContext connectContext;

    /**
     * True indicates that the profile has been reported.
     * <p> When {@link SessionVariable#isEnableLoadProfile()} is enabled,
     * if the time costs of stream load is less than {@link Config#stream_load_profile_collect_second},
     * the profile will not be reported to FE to reduce the overhead of profile under high-frequency import
     */
    private boolean profileAlreadyReported = false;

    private RuntimeProfile queryProfile;
    private final List<RuntimeProfile> fragmentProfiles;

    /**
     * The number of instances of this query.
     * <p> It is equal to the number of backends executing plan fragments on behalf of this query.
     */
    private MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal = null;

    private Supplier<RuntimeProfile> topProfileSupplier;
    private ExecPlan execPlan;
    private final AtomicLong lastRuntimeProfileUpdateTime = new AtomicLong(System.currentTimeMillis());

    // ------------------------------------------------------------------------------------
    // Fields for load.
    // ------------------------------------------------------------------------------------
    private final List<String> deltaUrls = Lists.newArrayList();
    private final Map<String, String> loadCounters = Maps.newHashMap();
    private String trackingUrl = "";
    private final Set<String> rejectedRecordPaths = Sets.newHashSet();

    // ------------------------------------------------------------------------------------
    // Fields for export.
    // ------------------------------------------------------------------------------------
    private final List<String> exportFiles = Lists.newArrayList();
    private final List<TTabletCommitInfo> commitInfos = Lists.newArrayList();
    private final List<TTabletFailInfo> failInfos = Lists.newArrayList();

    // ------------------------------------------------------------------------------------
    // Fields for external table sink
    // ------------------------------------------------------------------------------------
    private final List<TSinkCommitInfo> sinkCommitInfos = Lists.newArrayList();

    public QueryRuntimeProfile(ConnectContext connectContext,
                               JobSpec jobSpec,
                               int numFragments) {
        this.connectContext = connectContext;
        this.jobSpec = jobSpec;

        this.queryProfile = new RuntimeProfile("Execution");
        this.fragmentProfiles = new ArrayList<>(numFragments);
        for (int i = 0; i < numFragments; i++) {
            RuntimeProfile profile = new RuntimeProfile("Fragment " + i);
            fragmentProfiles.add(profile);
            queryProfile.addChild(profile);
        }
    }

    public List<String> getDeltaUrls() {
        return deltaUrls;
    }

    public Map<String, String> getLoadCounters() {
        return loadCounters;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public List<String> getRejectedRecordPaths() {
        return new ArrayList<>(rejectedRecordPaths);
    }

    public List<TTabletCommitInfo> getCommitInfos() {
        return commitInfos;
    }

    public List<TTabletFailInfo> getFailInfos() {
        return failInfos;
    }

    public List<TSinkCommitInfo> getSinkCommitInfos() {
        return sinkCommitInfos;
    }

    public List<String> getExportFiles() {
        return exportFiles;
    }

    public boolean isProfileAlreadyReported() {
        return profileAlreadyReported;
    }

    public synchronized void setTopProfileSupplier(Supplier<RuntimeProfile> topProfileSupplier) {
        this.topProfileSupplier = topProfileSupplier;
    }

    public void setExecPlan(ExecPlan execPlan) {
        this.execPlan = execPlan;
    }

    public void clearExportStatus() {
        exportFiles.clear();
    }

    public void attachInstances(Collection<TUniqueId> instanceIds) {
        // to keep things simple, make async Cancel() calls wait until plan fragment
        // execution has been initiated, otherwise we might try to cancel fragment
        // execution at backends where it hasn't even started
        profileDoneSignal = new MarkedCountDownLatch<>(instanceIds.size());
        instanceIds.forEach(instanceId -> profileDoneSignal.addMark(instanceId, MARKED_COUNT_DOWN_VALUE));
    }

    public void attachExecutionProfiles(Collection<FragmentInstanceExecState> executions) {
        for (FragmentInstanceExecState execState : executions) {
            if (!execState.computeTimeInProfile(fragmentProfiles.size())) {
                return;
            }
            fragmentProfiles.get(execState.getFragmentIndex()).addChild(execState.getProfile());
        }
    }

    public void finishInstance(TUniqueId instanceId) {
        if (profileDoneSignal != null) {
            profileDoneSignal.markedCountDown(instanceId, MARKED_COUNT_DOWN_VALUE);
        }
    }

    public void finishAllInstances(Status status) {
        if (profileDoneSignal != null) {
            profileDoneSignal.countDownToZero(status);
            LOG.info("unfinished instances: {}", getUnfinishedInstanceIds());
        }
    }

    public boolean isFinished() {
        return profileDoneSignal.getCount() == 0;
    }

    public boolean addListener(Consumer<Boolean> task) {
        if (EXECUTOR.getQueue().size() > Config.profile_process_blocking_queue_size) {
            return false;
        }
        // We need to make sure this submission won't be rejected by set the queue size to Integer.MAX_VALUE
        profileDoneSignal.addListener(() -> EXECUTOR.submit(() -> {
            task.accept(true);
        }));
        return true;
    }

    public boolean waitForProfileFinished(long timeout, TimeUnit unit) {
        boolean res = false;
        try {
            res = profileDoneSignal.await(timeout, unit);
            if (!res) {
                LOG.warn("failed to get profile within {} seconds", timeout);
            }
        } catch (InterruptedException e) { // NOSONAR
            LOG.warn("profile signal await error", e);
        }

        return res;
    }

    public RuntimeProfile getQueryProfile() {
        return queryProfile;
    }

    public void updateProfile(FragmentInstanceExecState execState, TReportExecStatusParams params) {
        if (params.isSetProfile()) {
            profileAlreadyReported = true;
        }

        // Update runtime profile when query is still in process.
        //
        // We need to export profile to ProfileManager before update this profile, because:
        // Each fragment instance will report its state based on their on own timer, and basically, these
        // timers are consistent. So we can assume that all the instances will report profile in a very short
        // time range, if we choose to export the profile to profile manager after update this instance's profile,
        // the whole profile may include the information from the previous report, except for the current instance,
        // which leads to inconsistency.
        //
        // So the profile update strategy looks like this: During a short time interval, each instance will report
        // its execState information. However, when receiving the information reported by the first instance of the
        // current batch, the previous reported state will be synchronized to the profile manager.
        long now = System.currentTimeMillis();
        long lastTime = lastRuntimeProfileUpdateTime.get();
        Supplier<RuntimeProfile> topProfileSupplier = this.topProfileSupplier;
        ExecPlan plan = execPlan;
        if (topProfileSupplier != null && plan != null && connectContext != null &&
                connectContext.isProfileEnabled() &&
                // If it's the last done report, avoiding duplicate trigger
                (!execState.isFinished() || profileDoneSignal.getLeftMarks().size() > 1) &&
                // Interval * 0.95 * 1000 to allow a certain range of deviation
                now - lastTime > (connectContext.getSessionVariable().getRuntimeProfileReportInterval() * 950L) &&
                lastRuntimeProfileUpdateTime.compareAndSet(lastTime, now)) {
            RuntimeProfile profile = topProfileSupplier.get();
            profile.addChild(buildQueryProfile(connectContext.needMergeProfile()));
            ProfilingExecPlan profilingPlan = plan.getProfilingPlan();
            saveRunningProfile(profilingPlan, profile);
        }
    }

    public synchronized void saveRunningProfile(ProfilingExecPlan profilingPlan, RuntimeProfile profile) {
        // topProfileSupplier may be null when the query is finished.
        // And here to make sure that runtime profile won't overwrite the final profile
        if (topProfileSupplier == null) {
            return;
        }
        ProfileManager.getInstance().pushProfile(profilingPlan, profile);
    }

    public void finalizeProfile() {
        fragmentProfiles.forEach(RuntimeProfile::sortChildren);
    }

    public void updateLoadInformation(FragmentInstanceExecState execState, TReportExecStatusParams params) {
        if (params.isSetDelta_urls()) {
            deltaUrls.addAll(params.getDelta_urls());
        }
        if (params.isSetLoad_counters()) {
            updateLoadCounters(params.getLoad_counters());
        }
        if (params.isSetTracking_url()) {
            trackingUrl = params.tracking_url;
        }
        if (params.isSetExport_files()) {
            exportFiles.addAll(params.getExport_files());
        }
        if (params.isSetCommitInfos()) {
            commitInfos.addAll(params.getCommitInfos());
        }
        if (params.isSetFailInfos()) {
            failInfos.addAll(params.getFailInfos());
        }
        if (params.isSetRejected_record_path()) {
            rejectedRecordPaths.add(execState.getAddress().getHostname() + ":" + params.getRejected_record_path());
        }
        if (params.isSetSink_commit_infos()) {
            sinkCommitInfos.addAll(params.getSink_commit_infos());
        }
    }

    public RuntimeProfile buildQueryProfile(boolean needMerge) {
        if (!needMerge || !jobSpec.isEnablePipeline()) {
            return queryProfile;
        }

        RuntimeProfile newQueryProfile = new RuntimeProfile(queryProfile.getName());
        long start = System.nanoTime();
        newQueryProfile.copyAllInfoStringsFrom(queryProfile, null);
        newQueryProfile.copyAllCountersFrom(queryProfile);

        long maxQueryCumulativeCpuTime = 0;
        long maxQueryPeakMemoryUsage = 0;
        long maxQueryExecutionWallTime = 0;
        long maxQuerySpillBytes = 0;

        List<RuntimeProfile> newFragmentProfiles = Lists.newArrayList();
        for (RuntimeProfile fragmentProfile : fragmentProfiles) {
            RuntimeProfile newFragmentProfile = new RuntimeProfile(fragmentProfile.getName());
            newFragmentProfiles.add(newFragmentProfile);
            newFragmentProfile.copyAllInfoStringsFrom(fragmentProfile, null);
            newFragmentProfile.copyAllCountersFrom(fragmentProfile);

            if (fragmentProfile.getChildList().isEmpty()) {
                continue;
            }

            List<RuntimeProfile> instanceProfiles = fragmentProfile.getChildList().stream()
                    .map(pair -> pair.first)
                    .collect(Collectors.toList());

            Set<String> backendAddresses = Sets.newHashSet();
            Set<String> instanceIds = Sets.newHashSet();
            Set<String> missingInstanceIds = Sets.newHashSet();
            for (RuntimeProfile instanceProfile : instanceProfiles) {
                // Setup backend meta infos
                backendAddresses.add(instanceProfile.getInfoString("Address"));
                instanceIds.add(instanceProfile.getInfoString("InstanceId"));
                if (CollectionUtils.isEmpty(instanceProfile.getChildList())) {
                    missingInstanceIds.add(instanceProfile.getInfoString("InstanceId"));
                }

                // Get query level peak memory usage, cpu cost, wall time
                Counter toBeRemove = instanceProfile.getCounter("QueryCumulativeCpuTime");
                if (toBeRemove != null) {
                    maxQueryCumulativeCpuTime = Math.max(maxQueryCumulativeCpuTime, toBeRemove.getValue());
                }
                instanceProfile.removeCounter("QueryCumulativeCpuTime");

                toBeRemove = instanceProfile.getCounter("QueryPeakMemoryUsage");
                if (toBeRemove != null) {
                    maxQueryPeakMemoryUsage = Math.max(maxQueryPeakMemoryUsage, toBeRemove.getValue());
                }
                instanceProfile.removeCounter("QueryPeakMemoryUsage");

                toBeRemove = instanceProfile.getCounter("QueryExecutionWallTime");
                if (toBeRemove != null) {
                    maxQueryExecutionWallTime = Math.max(maxQueryExecutionWallTime, toBeRemove.getValue());
                }
                instanceProfile.removeCounter("QueryExecutionWallTime");

                toBeRemove = instanceProfile.getCounter("QuerySpillBytes");
                if (toBeRemove != null) {
                    maxQuerySpillBytes = Math.max(maxQuerySpillBytes, toBeRemove.getValue());
                }
                instanceProfile.removeCounter("QuerySpillBytes");
            }
            newFragmentProfile.addInfoString("BackendAddresses", String.join(",", backendAddresses));
            newFragmentProfile.addInfoString("InstanceIds", String.join(",", instanceIds));
            if (!missingInstanceIds.isEmpty()) {
                newFragmentProfile.addInfoString("MissingInstanceIds", String.join(",", missingInstanceIds));
            }
            Counter backendNum = newFragmentProfile.addCounter("BackendNum", TUnit.UNIT, null);
            backendNum.setValue(backendAddresses.size());

            // Setup number of instance
            Counter counter = newFragmentProfile.addCounter("InstanceNum", TUnit.UNIT, null);
            counter.setValue(instanceProfiles.size());

            RuntimeProfile mergedInstanceProfile =
                    RuntimeProfile.mergeIsomorphicProfiles(instanceProfiles, Sets.newHashSet("Address", "InstanceId"));
            Preconditions.checkState(mergedInstanceProfile != null);

            newFragmentProfile.copyAllInfoStringsFrom(mergedInstanceProfile, null);
            newFragmentProfile.copyAllCountersFrom(mergedInstanceProfile);

            mergedInstanceProfile.getChildList().forEach(pair -> {
                RuntimeProfile pipelineProfile = pair.first;
                setOperatorStatus(pipelineProfile);
                newFragmentProfile.addChild(pipelineProfile);
            });

            newQueryProfile.addChild(newFragmentProfile);
        }

        // Remove redundant MIN/MAX metrics if MIN and MAX are identical
        for (RuntimeProfile fragmentProfile : newFragmentProfiles) {
            RuntimeProfile.removeRedundantMinMaxMetrics(fragmentProfile);
        }

        long queryAllocatedMemoryUsage = 0;
        long queryDeallocatedMemoryUsage = 0;
        // Calculate ExecutionTotalTime, which comprising all operator's sync time and async time
        // We can get Operator's sync time from OperatorTotalTime, and for async time, only ScanOperator and
        // ExchangeOperator have async operations, we can get async time from ScanTime(for ScanOperator) and
        // NetworkTime(for ExchangeOperator)
        long queryCumulativeOperatorTime = 0;
        long queryCumulativeScanTime = 0;
        long queryCumulativeNetworkTime = 0;
        long maxScheduleTime = 0;
        for (RuntimeProfile fragmentProfile : newFragmentProfiles) {
            Counter instanceAllocatedMemoryUsage = fragmentProfile.getCounter("InstanceAllocatedMemoryUsage");
            if (instanceAllocatedMemoryUsage != null) {
                queryAllocatedMemoryUsage += instanceAllocatedMemoryUsage.getValue();
            }
            Counter instanceDeallocatedMemoryUsage = fragmentProfile.getCounter("InstanceDeallocatedMemoryUsage");
            if (instanceDeallocatedMemoryUsage != null) {
                queryDeallocatedMemoryUsage += instanceDeallocatedMemoryUsage.getValue();
            }

            for (Pair<RuntimeProfile, Boolean> pipelineProfilePair : fragmentProfile.getChildList()) {
                RuntimeProfile pipelineProfile = pipelineProfilePair.first;
                Counter scheduleTime = pipelineProfile.getMaxCounter("ScheduleTime");
                if (scheduleTime != null) {
                    maxScheduleTime = Math.max(maxScheduleTime, scheduleTime.getValue());
                }
                for (Pair<RuntimeProfile, Boolean> operatorProfilePair : pipelineProfile.getChildList()) {
                    RuntimeProfile operatorProfile = operatorProfilePair.first;
                    RuntimeProfile commonMetrics = operatorProfile.getChild("CommonMetrics");
                    RuntimeProfile uniqueMetrics = operatorProfile.getChild("UniqueMetrics");
                    if (commonMetrics == null || uniqueMetrics == null) {
                        continue;
                    }

                    if (commonMetrics.containsInfoString("IsFinalSink")) {
                        long resultDeliverTime = 0;
                        Counter outputFullTime = pipelineProfile.getMaxCounter("OutputFullTime");
                        if (outputFullTime != null) {
                            resultDeliverTime += outputFullTime.getValue();
                        }
                        Counter pendingFinishTime = pipelineProfile.getMaxCounter("PendingFinishTime");
                        if (pendingFinishTime != null) {
                            resultDeliverTime += pendingFinishTime.getValue();
                        }
                        Counter resultDeliverTimer =
                                newQueryProfile.addCounter("ResultDeliverTime", TUnit.TIME_NS, null);
                        resultDeliverTimer.setValue(resultDeliverTime);
                    }

                    Counter operatorTotalTime = commonMetrics.getMaxCounter("OperatorTotalTime");
                    Preconditions.checkNotNull(operatorTotalTime);
                    queryCumulativeOperatorTime += operatorTotalTime.getValue();

                    Counter scanTime = uniqueMetrics.getMaxCounter("ScanTime");
                    if (scanTime != null) {
                        queryCumulativeScanTime += scanTime.getValue();
                        queryCumulativeOperatorTime += scanTime.getValue();
                    }

                    Counter networkTime = uniqueMetrics.getMaxCounter("NetworkTime");
                    if (networkTime != null) {
                        queryCumulativeNetworkTime += networkTime.getValue();
                        queryCumulativeOperatorTime += networkTime.getValue();
                    }
                }
            }
        }
        Counter queryAllocatedMemoryUsageCounter =
                newQueryProfile.addCounter("QueryAllocatedMemoryUsage", TUnit.BYTES, null);
        queryAllocatedMemoryUsageCounter.setValue(queryAllocatedMemoryUsage);
        Counter queryDeallocatedMemoryUsageCounter =
                newQueryProfile.addCounter("QueryDeallocatedMemoryUsage", TUnit.BYTES, null);
        queryDeallocatedMemoryUsageCounter.setValue(queryDeallocatedMemoryUsage);
        Counter queryCumulativeOperatorTimer =
                newQueryProfile.addCounter("QueryCumulativeOperatorTime", TUnit.TIME_NS, null);
        queryCumulativeOperatorTimer.setValue(queryCumulativeOperatorTime);
        Counter queryCumulativeScanTimer =
                newQueryProfile.addCounter("QueryCumulativeScanTime", TUnit.TIME_NS, null);
        queryCumulativeScanTimer.setValue(queryCumulativeScanTime);
        Counter queryCumulativeNetworkTimer =
                newQueryProfile.addCounter("QueryCumulativeNetworkTime", TUnit.TIME_NS, null);
        queryCumulativeNetworkTimer.setValue(queryCumulativeNetworkTime);
        Counter queryPeakScheduleTime = newQueryProfile.addCounter("QueryPeakScheduleTime", TUnit.TIME_NS, null);
        queryPeakScheduleTime.setValue(maxScheduleTime);
        newQueryProfile.getCounterTotalTime().setValue(0);

        Counter queryCumulativeCpuTime = newQueryProfile.addCounter("QueryCumulativeCpuTime", TUnit.TIME_NS, null);
        queryCumulativeCpuTime.setValue(maxQueryCumulativeCpuTime);
        Counter queryPeakMemoryUsage = newQueryProfile.addCounter("QueryPeakMemoryUsage", TUnit.BYTES, null);
        queryPeakMemoryUsage.setValue(maxQueryPeakMemoryUsage);
        Counter queryExecutionWallTime = newQueryProfile.addCounter("QueryExecutionWallTime", TUnit.TIME_NS, null);
        queryExecutionWallTime.setValue(maxQueryExecutionWallTime);
        Counter querySpillBytes = newQueryProfile.addCounter("QuerySpillBytes", TUnit.BYTES, null);
        querySpillBytes.setValue(maxQuerySpillBytes);

        if (execPlan != null) {
            newQueryProfile.addInfoString("Topology", execPlan.getProfilingPlan().toTopologyJson());
        }
        Counter processTimer =
                newQueryProfile.addCounter("FrontendProfileMergeTime", TUnit.TIME_NS, null);
        processTimer.setValue(System.nanoTime() - start);

        return newQueryProfile;
    }

    private List<String> getUnfinishedInstanceIds() {
        return profileDoneSignal.getLeftMarks().stream()
                .map(Map.Entry::getKey)
                .map(DebugUtil::printId)
                .collect(Collectors.toList());
    }

    private void setOperatorStatus(RuntimeProfile pipelineProfile) {
        for (Pair<RuntimeProfile, Boolean> child : pipelineProfile.getChildList()) {
            RuntimeProfile operatorProfile = child.first;
            RuntimeProfile commonMetrics = operatorProfile.getChild("CommonMetrics");
            Preconditions.checkNotNull(commonMetrics);

            if (commonMetrics.containsInfoString("IsChild")) {
                continue;
            }

            Counter closeTime = commonMetrics.getCounter("CloseTime");
            Counter minCloseTime = commonMetrics.getCounter("__MIN_OF_CloseTime");
            if (closeTime != null && closeTime.getValue() == 0 ||
                    minCloseTime != null && minCloseTime.getValue() == 0) {
                commonMetrics.addInfoString("IsRunning", "");
            }
        }
    }

    private void updateLoadCounters(Map<String, String> newLoadCounters) {
        long numRowsNormal = getCounterLongValueOrDefault(loadCounters, LoadEtlTask.DPP_NORMAL_ALL, 0L);
        long numRowsAbnormal = getCounterLongValueOrDefault(loadCounters, LoadEtlTask.DPP_ABNORMAL_ALL, 0L);
        long numRowsUnselected = getCounterLongValueOrDefault(loadCounters, LoadJob.UNSELECTED_ROWS, 0L);
        long numLoadBytesTotal = getCounterLongValueOrDefault(loadCounters, LoadJob.LOADED_BYTES, 0L);

        // new load counters
        numRowsNormal += getCounterLongValueOrDefault(newLoadCounters, LoadEtlTask.DPP_NORMAL_ALL, 0L);
        numRowsAbnormal += getCounterLongValueOrDefault(newLoadCounters, LoadEtlTask.DPP_ABNORMAL_ALL, 0L);
        numRowsUnselected += getCounterLongValueOrDefault(newLoadCounters, LoadJob.UNSELECTED_ROWS, 0L);
        numLoadBytesTotal += getCounterLongValueOrDefault(newLoadCounters, LoadJob.LOADED_BYTES, 0L);

        this.loadCounters.put(LoadEtlTask.DPP_NORMAL_ALL, String.valueOf(numRowsNormal));
        this.loadCounters.put(LoadEtlTask.DPP_ABNORMAL_ALL, String.valueOf(numRowsAbnormal));
        this.loadCounters.put(LoadJob.UNSELECTED_ROWS, String.valueOf(numRowsUnselected));
        this.loadCounters.put(LoadJob.LOADED_BYTES, String.valueOf(numLoadBytesTotal));
    }

    private long getCounterLongValueOrDefault(Map<String, String> counters, String key, long defaultValue) {
        String value = counters.get(key);
        if (value != null) {
            return Long.parseLong(value);
        }
        return defaultValue;
    }

}
