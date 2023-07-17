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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.FsBroker;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Status;
import com.starrocks.common.ThriftServer;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ProfileManager;
import com.starrocks.common.util.RuntimeProfile;
import com.starrocks.connector.exception.RemoteFileNotFoundException;
import com.starrocks.planner.ResultSink;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PPlanFragmentCancelReason;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryStatisticsItem;
import com.starrocks.qe.ResultReceiver;
import com.starrocks.qe.RowBatch;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.scheduler.assignment.FragmentAssignmentStrategyFactory;
import com.starrocks.qe.scheduler.dag.ExecutionDAG;
import com.starrocks.qe.scheduler.dag.ExecutionFragment;
import com.starrocks.qe.scheduler.dag.FragmentInstance;
import com.starrocks.qe.scheduler.dag.JobInformation;
import com.starrocks.qe.scheduler.state.CancelledState;
import com.starrocks.qe.scheduler.state.CancellingState;
import com.starrocks.qe.scheduler.state.CreatedState;
import com.starrocks.qe.scheduler.state.ExecutingState;
import com.starrocks.qe.scheduler.state.FailedState;
import com.starrocks.qe.scheduler.state.FailingState;
import com.starrocks.qe.scheduler.state.FinishedState;
import com.starrocks.qe.scheduler.state.FinishingState;
import com.starrocks.qe.scheduler.state.JobState;
import com.starrocks.qe.scheduler.state.JobStateType;
import com.starrocks.qe.scheduler.state.WaitingForResourceState;
import com.starrocks.qe.scheduler.state.event.CancelJobEvent;
import com.starrocks.qe.scheduler.state.event.DefaultJobEvent;
import com.starrocks.qe.scheduler.state.event.FailureJobEvent;
import com.starrocks.qe.scheduler.state.event.FinishingJobEvent;
import com.starrocks.qe.scheduler.state.event.JobEvent;
import com.starrocks.qe.scheduler.state.event.JobEventType;
import com.starrocks.qe.scheduler.state.statemachine.InvalidJobStateTransitionException;
import com.starrocks.qe.scheduler.state.statemachine.JobStateMachine;
import com.starrocks.qe.scheduler.state.statemachine.StateContext;
import com.starrocks.qe.scheduler.state.statemachine.StateMachineFactory;
import com.starrocks.rpc.RpcException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TLoadJobType;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReportExecStatusParams;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTabletFailInfo;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class DefaultScheduler implements ICoordinator, StateContext {

    private static final Logger LOG = LogManager.getLogger(DefaultScheduler.class);
    private static final String LOCAL_IP = FrontendOptions.getLocalHostAddress();
    private static final StateMachineFactory STATE_MACHINE_FACTORY;

    static {
        STATE_MACHINE_FACTORY = new StateMachineFactory(new CreatedState())
                // CREATED
                .addTransition(CreatedState.class, JobEventType.START_SCHEDULING, WaitingForResourceState.class,
                        new WaitingForResourceState.GotoWaitingForResource())
                .addTransition(CreatedState.class, JobEventType.CANCEL, CancelledState.class,
                        new CancelledState.GotoCancelled())
                .addTransition(CreatedState.class, JobEventType.FAIL, FailedState.class, new FailedState.GotoFailed())
                .addTransition(CreatedState.class, JobEventType.FINISHED, FailedState.class,
                        new FailedState.GotoFailed())
                // WAITING_FOR_RESOURCE
                .addTransition(WaitingForResourceState.class, JobEventType.RESOURCE_READY, ExecutingState.class,
                        new ExecutingState.GotoExecuting())
                .addTransition(WaitingForResourceState.class, JobEventType.CANCEL, CancellingState.class,
                        new CancellingState.GotoCancelling())
                .addTransition(WaitingForResourceState.class, JobEventType.FAIL, FailingState.class,
                        new FailingState.GotoFailing())
                // EXECUTING
                .addTransition(ExecutingState.class, JobEventType.FINISHING, FinishingState.class,
                        new FinishingState.GotoFinishing())
                .addTransition(ExecutingState.class, JobEventType.CANCEL, CancellingState.class,
                        new CancellingState.GotoCancelling())
                .addTransition(ExecutingState.class, JobEventType.FAIL, FailingState.class,
                        new FailingState.GotoFailing())
                .addTransition(ExecutingState.class, JobEventType.FINISHED, FinishedState.class,
                        new FinishedState.GotoFinished())
                // CANCELLING
                .addTransition(CancellingState.class, JobEventType.FINISHED, CancelledState.class,
                        new CancelledState.GotoCancelled())
                // FAILING
                .addTransition(FailingState.class, JobEventType.FINISHED, FailedState.class,
                        new FailedState.GotoFailed())
                // FINISHING
                .addTransition(FinishingState.class, JobEventType.FINISHED, FinishedState.class,
                        new FinishedState.GotoFinished())

                // RESET Event
                .addTransition(
                        ImmutableSet.of(WaitingForResourceState.class, ExecutingState.class, CancellingState.class,
                                FailingState.class, FinishingState.class, FailedState.class, FinishedState.class),
                        JobEventType.RESET, CreatedState.class,
                        new CreatedState.GotoCreated());

    }

    private final JobStateMachine stateMachine;

    private final TNetworkAddress schedulerAddress;

    private final ConnectContext connectContext;

    private final JobInformation jobInformation;
    private final ExecutionDAG executionDAG;

    private final Status status = Status.createOK();
    private Throwable failure = null;

    private final WorkerProvider.Factory workerProviderFactory = new DefaultWorkerProvider.Factory();
    private WorkerProvider workerProvider = null;
    private final FragmentAssignmentStrategyFactory fragmentAssignmentStrategyFactory;

    private ResultReceiver receiver = null;

    private final ExecutionDAGProfile executionDAGProfile;

    private int numReceivedRows = 0;
    private boolean thriftServerHighLoad = false;

    private boolean needDeploy = true;

    private Supplier<RuntimeProfile> topProfileSupplier = null;
    private final AtomicLong lastRuntimeProfileUpdateTime = new AtomicLong(0L);
    // True indicates that the profile has been reported
    // When `enable_load_profile` is enabled,
    // if the time costs of stream load is less than `stream_load_profile_collect_second`,
    // the profile will not be reported to FE to reduce the overhead of profile under high-frequency import
    private boolean profileAlreadyReported = false;

    // Fake.
    public DefaultScheduler(ConnectContext context, JobInformation jobInformation, TNetworkAddress address) {
        this.stateMachine = STATE_MACHINE_FACTORY.create(this, new CreatedState());

        this.schedulerAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);

        this.connectContext = Preconditions.checkNotNull(context, "connectContext is null");

        this.jobInformation = jobInformation;
        this.executionDAG = ExecutionDAG.build(jobInformation);

        TUniqueId queryId = jobInformation.getQueryId();
        this.executionDAGProfile = new ExecutionDAGProfile(connectContext, executionDAG, true, queryId, 1);
        ExecutionFragmentInstance fakeExecution = ExecutionFragmentInstance.createFakeExecution(queryId, address);
        executionDAGProfile.prepareProfileDoneSignal(Collections.singletonList(fakeExecution.getFragmentInstanceId()));
        executionDAGProfile.attachInstanceProfiles(Collections.singletonList(fakeExecution));

        this.fragmentAssignmentStrategyFactory = null;
        this.workerProvider = null;
    }

    public DefaultScheduler(ConnectContext context, JobInformation jobInformation, boolean needReport) {
        this.stateMachine = STATE_MACHINE_FACTORY.create(this, new CreatedState());

        this.schedulerAddress = new TNetworkAddress(LOCAL_IP, Config.rpc_port);

        this.connectContext = Preconditions.checkNotNull(context, "connectContext is null");

        this.jobInformation = jobInformation;
        this.executionDAG = ExecutionDAG.build(jobInformation);

        this.fragmentAssignmentStrategyFactory =
                new FragmentAssignmentStrategyFactory(connectContext, executionDAG, jobInformation.isEnablePipeline());

        // connectContext can be null for broker export task coordinator
        this.executionDAGProfile = new ExecutionDAGProfile(connectContext, executionDAG,
                needReport || context.getSessionVariable().isEnableProfile(), jobInformation.getQueryId(),
                executionDAG.getNumFragments());
    }

    // ------------------------------------------------------------------------------------
    // Common methods for scheduling.
    // ------------------------------------------------------------------------------------

    @Override
    public void startScheduling(boolean needDeploy) throws RpcException, UserException {
        this.needDeploy = needDeploy;

        SchedulerTraceUtil.log(jobInformation, "startScheduling [jobInformation={}]", jobInformation);
        handle(new DefaultJobEvent(JobEventType.START_SCHEDULING));

        handleStatus(getStatus());
    }

    @Override
    public void cancel(PPlanFragmentCancelReason reason, String message) {
        LOG.warn("cancel execution of query, this is outside invoke [queryId={}] [message={}]",
                DebugUtil.printId(jobInformation.getQueryId()), message);
        Status cancelStatus = new Status(TStatusCode.CANCELLED, message);
        handle(new CancelJobEvent(cancelStatus, reason));
    }

    @Override
    public void updateFragmentExecStatus(TReportExecStatusParams params) {
        ExecutionFragmentInstance execution = executionDAG.getExecution(params.backend_num);
        if (execution == null) {
            LOG.warn("unknown backend number: {}, valid backend numbers: {}", params.backend_num,
                    executionDAG.getExecutionIndexesInJob());
            return;
        }

        Status status = new Status(params.status);

        SchedulerTraceUtil.log(jobInformation,
                "updateFragmentExecStatus: receive [execution={}] [request={status={}, done={}}]",
                execution.getInstance(), status, params.isDone());

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
        // its execution information. However, when receiving the information reported by the first instance of the
        // current batch, the previous reported state will be synchronized to the profile manager.
        if (!execution.isFinished()) {
            long now = System.currentTimeMillis();
            long lastTime = lastRuntimeProfileUpdateTime.get();
            if (topProfileSupplier != null &&
                    connectContext != null &&
                    connectContext.getSessionVariable().isEnableProfile() &&
                    now - lastTime > connectContext.getSessionVariable().getRuntimeProfileReportInterval() * 1000L &&
                    lastRuntimeProfileUpdateTime.compareAndSet(lastTime, now)) {
                RuntimeProfile profile = topProfileSupplier.get();
                profile.addChild(buildMergedQueryProfile(null));
                ProfileManager.getInstance().pushProfile(profile);
            }
        }

        synchronized (this) {
            if (!execution.updateExecStatus(params)) {
                return;
            }
            if (execution.isFinished()) {
                executionDAGProfile.updateLoadInformation(execution, params);
            }
        }

        updateJobProgress(params);

        // print fragment instance profile
        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            execution.printProfile(builder);
            LOG.debug("profile for query_id={} instance_id={}\n{}", DebugUtil.printId(jobInformation.getQueryId()),
                    DebugUtil.printId(params.getFragment_instance_id()), builder.toString());
        }

        if (status.ok()) {
            onExecutionSuccess(execution);
        } else if (getState() == JobStateType.FINISHING && status.isCancelled()) {
            // The query is done and we are just waiting for remote fragments to clean up.
            // Ignore their cancelled updates.
            onExecutionSuccess(execution);
        } else {
            onExecutionFailed(execution, status);
        }
    }

    private void updateJobProgress(TReportExecStatusParams params) {
        if (params.isSetLoad_type()) {
            TLoadJobType loadJobType = params.getLoad_type();
            if (loadJobType == TLoadJobType.BROKER || loadJobType == TLoadJobType.INSERT_QUERY ||
                    loadJobType == TLoadJobType.INSERT_VALUES) {
                if (params.isSetSink_load_bytes() && params.isSetSource_load_rows() &&
                        params.isSetSource_load_bytes()) {
                    GlobalStateMgr.getCurrentState().getLoadMgr()
                            .updateJobPrgress(jobInformation.getLoadJobId(), params);
                }
            }
        } else {
            if (params.isSetSink_load_bytes() && params.isSetSource_load_rows() && params.isSetSource_load_bytes()) {
                GlobalStateMgr.getCurrentState().getLoadMgr().updateJobPrgress(jobInformation.getLoadJobId(), params);
            }
        }
    }

    private void onExecutionSuccess(ExecutionFragmentInstance execution) {
        SchedulerTraceUtil.log(jobInformation, "updateFragmentExecStatus: onExecutionFinished [execution={}]",
                execution.getInstance());

        if (execution.isFinished()) {
            executionDAGProfile.markedCountDown(execution.getFragmentInstanceId());
            if (executionDAGProfile.isDone()) {
                onFinished();
            }
        }
    }

    private void onExecutionFailed(ExecutionFragmentInstance execution, Status executionStatus) {
        SchedulerTraceUtil.log(jobInformation,
                "updateFragmentExecStatus: onExecutionFailed [execution={}] [executionStatus={}]",
                execution.getInstance(), executionStatus);
        LOG.warn("one instance report fail status, need cancel. job id: {}, query id: {}, instance id: {}",
                jobInformation.getLoadJobId(), DebugUtil.printId(jobInformation.getQueryId()),
                DebugUtil.printId(execution.getFragmentInstanceId()));

        onFailure(executionStatus);

        if (execution.isFinished()) {
            executionDAGProfile.markedCountDown(execution.getFragmentInstanceId());
            if (executionDAGProfile.isDone()) {
                onFinished();
            }
        }
    }

    public void onResourceReady() {
        handle(new DefaultJobEvent(JobEventType.RESOURCE_READY));
    }

    public void onFailure(Status status) {
        onFailure(status, null, null);
    }

    @Override
    public void onFailure(Status status, Throwable failure) {
        onFailure(status, null, failure);
    }

    public void onFailure(Status status, ExecutionFragmentInstance execution, Throwable failure) {
        handle(new FailureJobEvent(status, execution, failure));
    }

    public void onFinishing(boolean reachLimit) {
        handle(new FinishingJobEvent(reachLimit));
    }

    @Override
    public void onFinished() {
        handle(new DefaultJobEvent(JobEventType.FINISHED));
    }

    // ------------------------------------------------------------------------------------
    // Methods about state transition and status.
    // ------------------------------------------------------------------------------------

    public synchronized JobStateType getState() {
        return stateMachine.getState().getType();
    }

    private void handle(JobEvent event) {
        JobState oldState;
        JobState newState = null;
        synchronized (this) {
            oldState = stateMachine.getState();
            SchedulerTraceUtil.log(jobInformation, "state transition: receive event [{}] on state [{}]", event,
                    oldState.getType());

            try {
                newState = stateMachine.transition(event);
            } catch (InvalidJobStateTransitionException e) {
                // Do nothing.
                SchedulerTraceUtil.log(jobInformation, "state transition: event [{}] on state [{}] does nothing", event,
                        oldState.getType());
            }
        }

        if (oldState != newState && newState != null) {
            SchedulerTraceUtil.log(jobInformation, "state transition: change state from [{}] to [{}] on event [{}]",
                    oldState.getType(), newState.getType(), event);

            oldState.onLeave();
            newState.onEnter();
        }
    }

    public synchronized Status getStatus() {
        return new Status(status);
    }

    public synchronized void setStatus(Status status) {
        this.status.setStatus(status);
    }

    private void handleStatus(Status status) throws RpcException, UserException {
        switch (status.getErrorCode()) {
            case OK:
                break;
            case THRIFT_RPC_ERROR:
                LOG.warn("query rpc failed [query_id={}] [status={}]", DebugUtil.printId(jobInformation.getQueryId()),
                        status,
                        failure);
                throw new RpcException("rpc failed: " + status.getErrorMsg());
            case REMOTE_FILE_NOT_FOUND:
                throw new RemoteFileNotFoundException(status.getErrorMsg());
            case TIMEOUT:
            default:
                LOG.warn("query failed [query_id={}] [status={}]", DebugUtil.printId(jobInformation.getQueryId()),
                        status,
                        failure);
                throw new UserException(status.getErrorMsg());
        }
    }

    // ------------------------------------------------------------------------------------
    // Methods for query.
    // ------------------------------------------------------------------------------------

    @Override
    public RowBatch getNext() throws Exception {
        if (receiver == null) {
            SchedulerTraceUtil.log(jobInformation, "getNext: receiver is null");
            throw new UserException("There is no receiver.");
        }

        RowBatch resultBatch;
        Status status = new Status();
        resultBatch = receiver.getNext(status);

        if (!status.ok()) {
            SchedulerTraceUtil.log(jobInformation, "getNext: fail [status={}]", status);
            LOG.warn("get next fail, need cancel. status {}, query id: {}", status.toString(),
                    DebugUtil.printId(jobInformation.getQueryId()));
            onFailure(status);
        } else if (resultBatch.isEos()) {
            // if this query is a block query do not cancel.
            long numLimitRows = executionDAG.getRootFragment().getPlanFragment().getPlanRoot().getLimit();
            boolean hasLimit = numLimitRows > 0;
            boolean reachLimit =
                    !jobInformation.isBlockQuery() && executionDAG.getInstanceIds().size() > 1 && hasLimit &&
                            numReceivedRows >= numLimitRows;
            SchedulerTraceUtil.log(jobInformation, "getNext: eos [reachLimit={}]", reachLimit);
            onFinishing(reachLimit);
        } else {
            int curNumRows = resultBatch.getBatch().getRowsSize();
            numReceivedRows += curNumRows;
            SchedulerTraceUtil.log(jobInformation, "getNext: success [curRows={}] [totalRows={}]", curNumRows,
                    numReceivedRows);
        }

        handleStatus(getStatus());

        return resultBatch;
    }

    // ------------------------------------------------------------------------------------
    // Methods used by JobState.
    // ------------------------------------------------------------------------------------

    @Override
    public void resetJobDAG() {
        jobInformation.reset();
    }

    @Override
    public void resetAvailableComputeNodes() {
        SessionVariable sessionVariable = connectContext.getSessionVariable();
        workerProvider = workerProviderFactory.captureAvailableWorkers(GlobalStateMgr.getCurrentSystemInfo(),
                sessionVariable.isPreferComputeNode(), sessionVariable.getUseComputeNodes());
    }

    @Override
    public void requireSlot() {
        // TODO(lzh): implement requiring slot.

        onResourceReady();
    }

    @Override
    public void releaseSharingSlot() {
        // TODO(lzh): release sharing slot when implementing requiring slot.
    }

    @Override
    public void cancelSlotRequirement() {
        // TODO(lzh): cancel the in-flight slot requirement slot when implementing requiring slot.
    }

    @Override
    public void initializeExecutionDAG() throws UserException {
        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPostorder()) {
            fragmentAssignmentStrategyFactory.create(execFragment, workerProvider).assignWorkerToFragment(execFragment);
        }

        if (LOG.isDebugEnabled()) {
            executionDAG.getFragmentsInPreorder().forEach(
                    execFragment -> LOG.debug("fragment {} has instances {}",
                            execFragment.getPlanFragment().getFragmentId(),
                            execFragment.getInstances().size()));
        }

        validateExecutionDAG();

        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
            executionDAG.initFragment(execFragment);
        }

        executionDAG.finalizeDAG();
    }

    private void validateExecutionDAG() throws StarRocksPlannerException {
        for (ExecutionFragment execFragment : executionDAG.getFragmentsInPreorder()) {
            if (execFragment.getPlanFragment().getSink() instanceof ResultSink &&
                    execFragment.getInstances().size() > 1) {
                throw new StarRocksPlannerException("This sql plan has multi result sinks", ErrorType.INTERNAL_ERROR);
            }
        }
    }

    @Override
    public void initializeResultSink() throws AnalysisException {
        ExecutionFragment rootFragment = executionDAG.getRootFragment();
        if (!(rootFragment.getPlanFragment().getSink() instanceof ResultSink)) {
            return;
        }

        ResultSink resultSink = (ResultSink) rootFragment.getPlanFragment().getSink();
        FragmentInstance rootInstance = rootFragment.getInstances().get(0);
        ComputeNode rootWorker = rootInstance.getWorker();

        receiver = new ResultReceiver(rootInstance.getInstanceId(), rootWorker.getId(), rootWorker.getBrpcAddress(),
                jobInformation.getQueryOptions().query_timeout * 1000);

        // Select root fragment as global runtime filter merge address
        GlobalRuntimeFilterInitializer grfInitializer = new GlobalRuntimeFilterInitializer(executionDAG, connectContext,
                jobInformation.isEnablePipeline());
        grfInitializer.setGlobalRuntimeFilterParams(rootFragment, rootWorker.getBrpcAddress());

        if (LOG.isDebugEnabled()) {
            LOG.debug("dispatch query job: {} to {}", DebugUtil.printId(jobInformation.getQueryId()),
                    rootWorker.getAddress());
        }

        // set the broker address for OUTFILE sink
        if (resultSink.isOutputFileSink() && resultSink.needBroker()) {
            FsBroker broker =
                    GlobalStateMgr.getCurrentState().getBrokerMgr()
                            .getBroker(resultSink.getBrokerName(), rootWorker.getHost());
            resultSink.setBrokerAddr(broker.ip, broker.port);
            LOG.info("OUTFILE through broker: {}:{}", broker.ip, broker.port);
        }
    }

    @Override
    public void initializeProfile() {
        if (jobInformation.isLoadType()) {
            jobInformation.getQueryOptions().setEnable_profile(true);
            GlobalStateMgr.getCurrentState().getLoadMgr()
                    .initJobProgress(jobInformation.getLoadJobId(), jobInformation.getQueryId(),
                            executionDAG.getInstanceIds(),
                            workerProvider.getSelectedWorkerIds());
            LOG.info("dispatch load job: {} to {}", DebugUtil.printId(jobInformation.getQueryId()),
                    workerProvider.getSelectedWorkerIds());
        }

        executionDAGProfile.prepareProfileDoneSignal(executionDAG.getInstanceIds());
    }

    @Override
    public synchronized void deploy() {
        SchedulerTraceUtil.log(jobInformation, "deploy: [executionDAG={}]", executionDAG);

        Deployer deployer =
                new Deployer(connectContext, jobInformation, workerProvider, executionDAG, schedulerAddress,
                        this::onFailure);
        for (List<ExecutionFragment> concurrentFragments : executionDAG.getFragmentsInTopologicalOrderFromRoot()) {
            if (!deployer.deployFragments(concurrentFragments, needDeploy)) {
                return;
            }
        }

        executionDAGProfile.attachInstanceProfiles(executionDAG.getExecutions());
    }

    @Override
    public synchronized void cancelInternal(PPlanFragmentCancelReason reason, Status status, Throwable failure) {
        if (!this.status.ok()) {
            return;
        }
        if (!status.ok()) {
            if (Strings.isNullOrEmpty(status.getErrorMsg())) {
                status.rewriteErrorMsg();
            }
            this.status.setStatus(status);
            this.failure = failure;

            connectContext.setErrorCodeOnce(status.getErrorCodeString());
        }
        if (StringUtils.isEmpty(connectContext.getState().getErrorMessage())) {
            connectContext.getState().setError(reason.toString());
        }

        if (null != receiver) {
            receiver.cancel();
        }
        cancelRemoteFragmentsAsync(reason);
        if (reason != PPlanFragmentCancelReason.LIMIT_REACH) {
            // count down to zero to notify all objects waiting for this
            if (!connectContext.getSessionVariable().isEnableProfile()) {
                executionDAGProfile.countDownToZero();
            } else if (FeConstants.BACKEND_NODE_NOT_FOUND_ERROR.equals(status.getErrorMsg())) {
                // when enable_profile is true, it disable count down profileDoneSignal for collect all backend's profile
                // but if backend has crashed, we need count down profileDoneSignal since it will not report by itself
                executionDAGProfile.countDownToZero();
                LOG.info("count down profileDoneSignal since backend has crashed, query id: {}",
                        DebugUtil.printId(jobInformation.getQueryId()));
            }
        }
    }

    // ------------------------------------------------------------------------------------
    // Methods for profile.
    // ------------------------------------------------------------------------------------

    @Override
    public void endProfile() {
        SchedulerTraceUtil.log(jobInformation, "endProfile [profile={}]", executionDAGProfile);
        if (needDeploy) {
            executionDAGProfile.joinIfNeedReport();
            synchronized (this) {
                executionDAGProfile.endProfile();
            }
        }
    }

    @Override
    public RuntimeProfile buildMergedQueryProfile(PQueryStatistics statistics) {
        return executionDAGProfile.buildMergedQueryProfile(statistics);
    }

    @Override
    public void setTopProfileSupplier(Supplier<RuntimeProfile> topProfileSupplier) {
        this.topProfileSupplier = topProfileSupplier;
    }

    @Override
    public RuntimeProfile getQueryProfile() {
        return executionDAGProfile.getQueryProfile();
    }

    @Override
    public List<String> getDeltaUrls() {
        return executionDAGProfile.getDeltaUrls();
    }

    @Override
    public Map<String, String> getLoadCounters() {
        return executionDAGProfile.getLoadCounters();
    }

    @Override
    public List<TTabletFailInfo> getFailInfos() {
        return executionDAGProfile.getFailInfos();
    }

    @Override
    public List<TTabletCommitInfo> getCommitInfos() {
        return executionDAGProfile.getCommitInfos();
    }

    @Override
    public List<TSinkCommitInfo> getSinkCommitInfos() {
        return executionDAGProfile.getSinkCommitInfos();
    }

    @Override
    public List<String> getExportFiles() {
        return executionDAGProfile.getExportFiles();
    }

    @Override
    public String getTrackingUrl() {
        return executionDAGProfile.getTrackingUrl();
    }

    @Override
    public List<String> getRejectedRecordPaths() {
        return executionDAGProfile.getRejectedRecordPaths();
    }

    @Override
    public List<QueryStatisticsItem.FragmentInstanceInfo> getFragmentInstanceInfos() {
        return executionDAG.getFragmentInstanceInfos();
    }

    // ------------------------------------------------------------------------------------
    // Methods for load.
    // ------------------------------------------------------------------------------------

    @Override
    public boolean join(int timeoutSecond) {
        final long fixedMaxWaitSecond = 5;

        long leftTimeoutSecond = timeoutSecond;
        while (leftTimeoutSecond > 0) {
            long waitSecond = Math.min(leftTimeoutSecond, fixedMaxWaitSecond);
            SchedulerTraceUtil.log(jobInformation, "join: [waitSecond={}]", waitSecond);
            if (executionDAGProfile.join(waitSecond)) {
                SchedulerTraceUtil.log(jobInformation, "join: finish");
                return true;
            }

            if (!checkBackendState()) {
                SchedulerTraceUtil.log(jobInformation, "join: some backend is unhealthy");
                return true;
            }

            if (ThriftServer.getExecutor() != null &&
                    ThriftServer.getExecutor().getPoolSize() >= Config.thrift_server_max_worker_threads) {
                thriftServerHighLoad = true;
            }

            leftTimeoutSecond -= waitSecond;
        }
        return false;
    }

    /**
     * Check the state of backends in needCheckBackendExecStates.
     * return true if all of them are OK. Otherwise, return false.
     */
    @Override
    public boolean checkBackendState() {
        for (ExecutionFragmentInstance execution : executionDAG.getNeedCheckExecutions()) {
            if (!execution.isBackendStateHealthy()) {
                setStatus(new Status(TStatusCode.INTERNAL_ERROR,
                        "backend " + execution.getBackend().getId() + " is down"));
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isThriftServerHighLoad() {
        return thriftServerHighLoad;
    }

    @Override
    public void setLoadJobType(TLoadJobType type) {
        jobInformation.getQueryOptions().setLoad_job_type(type);
    }

    @Override
    public long getLoadJobId() {
        return jobInformation.getLoadJobId();
    }

    @Override
    public void setLoadJobId(Long jobId) {
        jobInformation.setLoadJobId(jobId);
    }

    @Override
    public Map<Integer, TNetworkAddress> getChannelIdToBEHTTPMap() {
        return executionDAG.getChannelIdToBEHTTP();
    }

    @Override
    public Map<Integer, TNetworkAddress> getChannelIdToBEPortMap() {
        return executionDAG.getChannelIdToBEPort();
    }

    @Override
    public boolean isEnableLoadProfile() {
        return connectContext != null && connectContext.getSessionVariable().isEnableLoadProfile();
    }

    @Override
    public synchronized void clearExportStatus() {
        executionDAG.clearExportStatus();
        executionDAGProfile.clearExportStatus();
        status.setStatus(Status.createOK());
        handle(new DefaultJobEvent(JobEventType.RESET));
    }

    // ------------------------------------------------------------------------------------
    // Common methods.
    // ------------------------------------------------------------------------------------

    @Override
    public Status getExecStatus() {
        return status;
    }

    @Override
    public boolean isUsingBackend(Long backendID) {
        return workerProvider.isWorkerSelected(backendID);
    }

    @Override
    public boolean isDone() {
        return executionDAGProfile.isDone();
    }

    @Override
    public TUniqueId getQueryId() {
        return jobInformation.getQueryId();
    }

    @Override
    public void setQueryId(TUniqueId queryId) {
        jobInformation.setQueryId(queryId);
    }

    @Override
    public List<ScanNode> getScanNodes() {
        return jobInformation.getScanNodes();
    }

    @Override
    public long getStartTimeMs() {
        return jobInformation.getQueryGlobals().getTimestamp_ms();
    }

    @Override
    public void setTimeout(int timeoutSecond) {
        jobInformation.getQueryOptions().setQuery_timeout(timeoutSecond);
    }

    @Override
    public boolean isProfileAlreadyReported() {
        return this.profileAlreadyReported;
    }

    private void cancelRemoteFragmentsAsync(PPlanFragmentCancelReason cancelReason) {
        for (ExecutionFragmentInstance execution : executionDAG.getExecutions()) {
            if (!execution.cancelFragmentInstance(cancelReason) &&
                    (!execution.hasBeenDeployed() || execution.isFinished())) {
                // If the execution fails to be cancelled, and it has been finished or not been deployed,
                // count down the profileDoneSignal of this execution immediately,
                // because the profile report will not arrive anymore for the finished or non-deployed execution.
                executionDAGProfile.markedCountDown(execution.getFragmentInstanceId());
            }
        }

        executionDAG.getInstances().stream()
                .filter(instance -> executionDAG.getExecution(instance.getIndexInJob()) == null)
                .forEach(instance -> executionDAGProfile.markedCountDown(instance.getInstanceId()));
    }

    @VisibleForTesting
    ExecutionDAG getExecutionDAG() {
        return executionDAG;
    }

}
