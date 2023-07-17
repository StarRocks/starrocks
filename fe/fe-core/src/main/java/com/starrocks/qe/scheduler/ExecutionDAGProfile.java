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
import com.starrocks.common.MarkedCountDownLatch;
import com.starrocks.common.Pair;
import com.starrocks.common.Status;
import com.starrocks.common.util.Counter;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.task.LoadEtlTask;
import com.starrocks.thrift.TPipelineProfileLevel;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ExecutionDAGProfile {
    private static final Logger LOG = LogManager.getLogger(ExecutionDAGProfile.class);

    // value is meaningless
    private static final Long MARKED_COUNT_DOWN_VALUE = -1L;

    private final ExecutionDAG executionDAG;

    private final ConnectContext context;
    private final boolean needReport;

    private final RuntimeProfile queryProfile;
    private final List<RuntimeProfile> fragmentProfiles;
    private volatile MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal = null;

    private final List<String> deltaUrls = Lists.newArrayList();
    private final Map<String, String> loadCounters = Maps.newHashMap();
    private String trackingUrl = "";
    private final Set<String> rejectedRecordPaths = Sets.newHashSet();
    // for export
    private final List<String> exportFiles = Lists.newArrayList();
    private final List<TTabletCommitInfo> commitInfos = Lists.newArrayList();
    private final List<TTabletFailInfo> failInfos = Lists.newArrayList();

    // for external table sink
    private final List<TSinkCommitInfo> sinkCommitInfos = Lists.newArrayList();

    public ExecutionDAGProfile(ConnectContext context,
                               ExecutionDAG executionDAG,
                               boolean needReport,
                               TUniqueId queryId,
                               int numFragments) {

        this.executionDAG = executionDAG;
        this.context = context;
        this.needReport = needReport;

        queryProfile = new RuntimeProfile("Execution Profile " + DebugUtil.printId(queryId));

        fragmentProfiles = new ArrayList<>(numFragments);
        for (int i = 0; i < numFragments; i++) {
            RuntimeProfile profile = new RuntimeProfile("Fragment " + i);
            fragmentProfiles.add(profile);
            queryProfile.addChild(profile);
        }
    }

    public void prepareProfileDoneSignal(Collection<TUniqueId> instanceIds) {
        // to keep things simple, make async Cancel() calls wait until plan fragment
        // execution has been initiated, otherwise we might try to cancel fragment
        // execution at backends where it hasn't even started
        MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal = new MarkedCountDownLatch<>(instanceIds.size());
        instanceIds.forEach(instanceId -> profileDoneSignal.addMark(instanceId, MARKED_COUNT_DOWN_VALUE));
        this.profileDoneSignal = profileDoneSignal;
    }

    public void attachInstanceProfiles(Collection<ExecutionFragmentInstance> executions) {
        for (ExecutionFragmentInstance execution : executions) {
            if (!execution.computeTimeInProfile(fragmentProfiles.size())) {
                continue;
            }

            fragmentProfiles.get(execution.getProfileFragmentId()).addChild(execution.getProfile());
        }
    }

    public boolean join(long timeoutSecond) {
        MarkedCountDownLatch<TUniqueId, Long> profileDoneSignal = this.profileDoneSignal;
        if (profileDoneSignal == null) {
            return false;
        }
        try {
            return profileDoneSignal.await(timeoutSecond, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public boolean isDone() {
        return profileDoneSignal != null && profileDoneSignal.getCount() == 0;
    }

    public void clearExportStatus() {
        exportFiles.clear();
    }

    public RuntimeProfile getQueryProfile() {
        return queryProfile;
    }

    public void joinIfNeedReport() {
        if (needReport) {
            // Waiting for other fragment instances to finish execution
            // Ideally, it should wait indefinitely, but out of defense, set timeout
            long timeoutSecond = context.getSessionVariable().getProfileTimeout();
            if (!join(timeoutSecond)) {
                LOG.warn("failed to get profile within {} seconds", timeoutSecond);
            }
        }
    }

    public void endProfile() {
        fragmentProfiles.forEach(RuntimeProfile::sortChildren);
    }


    public RuntimeProfile buildMergedQueryProfile(PQueryStatistics statistics) {
        SessionVariable sessionVariable = context.getSessionVariable();

        if (!sessionVariable.isEnableProfile()) {
            return queryProfile;
        }

        if (!executionDAG.getJobInformation().isEnablePipeline()) {
            return queryProfile;
        }

        int profileLevel = sessionVariable.getPipelineProfileLevel();
        if (profileLevel >= TPipelineProfileLevel.DETAIL.getValue()) {
            return queryProfile;
        }

        RuntimeProfile newQueryProfile = new RuntimeProfile(queryProfile.getName());
        newQueryProfile.copyAllInfoStringsFrom(queryProfile, null);
        newQueryProfile.copyAllCountersFrom(queryProfile);

        long maxQueryCumulativeCpuTime = 0;
        long maxQueryPeakMemoryUsage = 0;

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
            for (RuntimeProfile instanceProfile : instanceProfiles) {
                // Setup backend address infos
                backendAddresses.add(instanceProfile.getInfoString("Address"));

                // Get query level peak memory usage and cpu cost
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
            }
            newFragmentProfile.addInfoString("BackendAddresses", String.join(",", backendAddresses));
            Counter backendNum = newFragmentProfile.addCounter("BackendNum", TUnit.UNIT, null);
            backendNum.setValue(backendAddresses.size());

            // Setup number of instance
            Counter counter = newFragmentProfile.addCounter("InstanceNum", TUnit.UNIT, null);
            counter.setValue(instanceProfiles.size());

            RuntimeProfile mergedInstanceProfile =
                    RuntimeProfile.mergeIsomorphicProfiles(instanceProfiles, Sets.newHashSet("Address"));
            Preconditions.checkState(mergedInstanceProfile != null);

            newFragmentProfile.copyAllInfoStringsFrom(mergedInstanceProfile, null);
            newFragmentProfile.copyAllCountersFrom(mergedInstanceProfile);

            mergedInstanceProfile.getChildList().forEach(pair -> {
                RuntimeProfile pipelineProfile = pair.first;
                foldUnnecessaryLimitOperators(pipelineProfile);
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
        boolean foundResultSink = false;
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
                for (Pair<RuntimeProfile, Boolean> operatorProfilePair : pipelineProfile.getChildList()) {
                    RuntimeProfile operatorProfile = operatorProfilePair.first;
                    if (!foundResultSink && (operatorProfile.getName().contains("RESULT_SINK") ||
                            operatorProfile.getName().contains("OLAP_TABLE_SINK"))) {
                        long executionWallTime = pipelineProfile.getCounter("DriverTotalTime").getValue();
                        Counter executionTotalTime =
                                newQueryProfile.addCounter("QueryExecutionWallTime", TUnit.TIME_NS, null);
                        newQueryProfile.getCounterTotalTime().setValue(0);
                        executionTotalTime.setValue(executionWallTime);
                        foundResultSink = true;
                    }

                    RuntimeProfile commonMetrics = operatorProfile.getChild("CommonMetrics");
                    RuntimeProfile uniqueMetrics = operatorProfile.getChild("UniqueMetrics");
                    if (commonMetrics == null || uniqueMetrics == null) {
                        continue;
                    }
                    Counter operatorTotalTime = commonMetrics.getMaxCounter("OperatorTotalTime");
                    Preconditions.checkNotNull(operatorTotalTime);
                    queryCumulativeOperatorTime += operatorTotalTime.getValue();

                    Counter scanTime = uniqueMetrics.getMaxCounter("ScanTime");
                    if (scanTime != null) {
                        queryCumulativeOperatorTime += scanTime.getValue();
                    }

                    Counter networkTime = uniqueMetrics.getMaxCounter("NetworkTime");
                    if (networkTime != null) {
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
        newQueryProfile.getCounterTotalTime().setValue(0);

        Counter queryCumulativeCpuTime = newQueryProfile.addCounter("QueryCumulativeCpuTime", TUnit.TIME_NS, null);
        queryCumulativeCpuTime.setValue(statistics == null || statistics.cpuCostNs == null ?
                maxQueryCumulativeCpuTime : statistics.cpuCostNs);
        Counter queryPeakMemoryUsage = newQueryProfile.addCounter("QueryPeakMemoryUsage", TUnit.BYTES, null);
        queryPeakMemoryUsage.setValue(statistics == null || statistics.memCostBytes == null ?
                maxQueryPeakMemoryUsage : statistics.memCostBytes);

        return newQueryProfile;
    }

    /**
     * Remove unnecessary LimitOperator, which has same input rows and output rows
     * to keep the profile concise
     */
    private void foldUnnecessaryLimitOperators(RuntimeProfile pipelineProfile) {
        SessionVariable sessionVariable = context.getSessionVariable();
        if (!sessionVariable.isProfileLimitFold()) {
            return;
        }

        List<String> foldNames = Lists.newArrayList();
        for (Pair<RuntimeProfile, Boolean> child : pipelineProfile.getChildList()) {
            RuntimeProfile operatorProfile = child.first;
            if (operatorProfile.getName().contains("LIMIT")) {
                RuntimeProfile commonMetrics = operatorProfile.getChild("CommonMetrics");
                Preconditions.checkNotNull(commonMetrics);
                Counter pullRowNum = commonMetrics.getCounter("PullRowNum");
                Counter pushRowNum = commonMetrics.getCounter("PushRowNum");
                if (pullRowNum == null || pushRowNum == null) {
                    continue;
                }
                if (Objects.equals(pullRowNum.getValue(), pushRowNum.getValue())) {
                    foldNames.add(operatorProfile.getName());
                }
            }
        }

        foldNames.forEach(pipelineProfile::removeChild);
    }

    private void setOperatorStatus(RuntimeProfile pipelineProfile) {
        for (Pair<RuntimeProfile, Boolean> child : pipelineProfile.getChildList()) {
            RuntimeProfile operatorProfile = child.first;
            RuntimeProfile commonMetrics = operatorProfile.getChild("CommonMetrics");
            Preconditions.checkNotNull(commonMetrics);

            Counter closeTime = commonMetrics.getCounter("CloseTime");
            Counter minCloseTime = commonMetrics.getCounter("__MIN_OF_CloseTime");
            if (closeTime != null && closeTime.getValue() == 0 ||
                    minCloseTime != null && minCloseTime.getValue() == 0) {
                commonMetrics.addInfoString("Status", "Running");
            }
        }
    }

    public void markedCountDown(TUniqueId instanceId) {
        if (profileDoneSignal != null) {
            profileDoneSignal.markedCountDown(instanceId, MARKED_COUNT_DOWN_VALUE);
        }
    }

    public void countDownToZero() {
        if (profileDoneSignal != null) {
            profileDoneSignal.countDownToZero(new Status());

            LOG.info("unfinished instances: {}", getUnfinishedInstanceIds());
        }
    }

    @Override
    public String toString() {
        return "ExecutionDAGProfile " +
                "{unfinished instances=" + String.join(",", getUnfinishedInstanceIds()) + "}";
    }

    private List<String> getUnfinishedInstanceIds() {
        return profileDoneSignal.getLeftMarks().stream()
                .map(Map.Entry::getKey)
                .map(DebugUtil::printId)
                .collect(Collectors.toList());
    }

    public void updateLoadInformation(ExecutionFragmentInstance execution, TReportExecStatusParams params) {
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
            rejectedRecordPaths.add(execution.getAddress().hostname + ":" + params.getRejected_record_path());
        }
        if (params.isSetSink_commit_infos()) {
            sinkCommitInfos.addAll(params.getSink_commit_infos());
        }
    }

    private long getCounterLongValueOrDefault(Map<String, String> counter, String key, long defaultValue) {
        String value = counter.get(key);
        if (value != null) {
            return Long.parseLong(value);
        }
        return defaultValue;
    }

    public void updateLoadCounters(Map<String, String> newLoadCounters) {
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

    public List<String> getDeltaUrls() {
        return deltaUrls;
    }

    public Map<String, String> getLoadCounters() {
        return loadCounters;
    }

    public List<String> getExportFiles() {
        return exportFiles;
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

    public List<String> getRejectedRecordPaths() {
        return new ArrayList<>(rejectedRecordPaths);
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

}
