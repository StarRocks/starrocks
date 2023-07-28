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

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Table;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PMVMaintenanceTaskResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.QeProcessorImpl;
import com.starrocks.qe.scheduler.dag.JobSpec;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TMVMaintenanceStartTask;
import com.starrocks.thrift.TMVMaintenanceStopTask;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Long-running job responsible for MV incremental maintenance.
 * <p>
 * Each job is event driven and single-thread execution:
 * 1. Event driven: job state machine and execution is separated, and the state machine is driven by events
 * 2. Execution: the job is executed in JobExecutor, at most one thread could execute the job
 */
public class MVMaintenanceJob implements Writable, GsonPreProcessable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(MVMaintenanceJob.class);
    private static final int MV_QUERY_TIMEOUT = 120;

    // Persisted state
    @SerializedName("jobId")
    private final long jobId;
    @SerializedName("dbId")
    private final long dbId;
    @SerializedName("viewId")
    private final long viewId;
    @SerializedName("state")
    private JobState serializedState;
    @SerializedName("epoch")
    private MVEpoch epoch;

    // TODO(murphy) serialize the plan, current we need to rebuild the plan for job
    private final transient ExecPlan plan;

    // Runtime ephemeral state
    // At most one thread could execute this job, this flag indicates is someone scheduling this job
    private transient AtomicReference<JobState> state = new AtomicReference<>();
    private transient AtomicBoolean inSchedule = new AtomicBoolean(false);
    private transient MaterializedView view;
    private transient ConnectContext connectContext;
    // TODO(murphy) implement a real query coordinator
    private transient CoordinatorPreprocessor queryCoordinator;
    private transient TxnBasedEpochCoordinator epochCoordinator;
    private transient Map<Long, MVMaintenanceTask> taskMap;
    private transient BiMap<Long, TNetworkAddress> taskId2Addr;

    public MVMaintenanceJob(MaterializedView view) {
        this.jobId = view.getId();
        this.dbId = view.getDbId();
        this.viewId = view.getId();
        this.view = view;
        this.epoch = new MVEpoch(view.getMvId());
        this.serializedState = JobState.INIT;
        this.state.set(JobState.INIT);
        this.plan = Preconditions.checkNotNull(view.getMaintenancePlan());
    }

    public static MVMaintenanceJob read(DataInput input) throws IOException {
        MVMaintenanceJob job = GsonUtils.GSON.fromJson(Text.readString(input), MVMaintenanceJob.class);
        job.state = new AtomicReference<>();
        job.inSchedule = new AtomicBoolean();
        job.state.set(job.getSerializedState());
        return job;
    }

    // TODO recover the entire job state, include execution plan
    public void restore() {
        Table table = GlobalStateMgr.getCurrentState().getDb(dbId).getTable(viewId);
        Preconditions.checkState(table != null && table.isMaterializedView());
        this.view = (MaterializedView) table;
        this.serializedState = JobState.INIT;
        this.state.set(serializedState);
        this.inSchedule.set(false);
    }

    public void setEpoch(MVEpoch epoch) {
        this.epoch = epoch;
    }

    public void startJob() {
        Preconditions.checkState(state.compareAndSet(JobState.INIT, JobState.STARTED));
    }

    // TODO(murphy) make it a thread-safe with JobExecutor, what if it's under scheduling ?
    public void stopJob() {
        if (!inSchedule.compareAndSet(false, true)) {
            throw UnsupportedException.unsupportedException("TODO: not support stop job running job");
        }
        try {
            stopTasks();
        } catch (Exception e) {
            LOG.warn("stop job failed", e);
        } finally {
            inSchedule.compareAndSet(true, false);
        }
        state.set(JobState.STOPPED);
    }

    public void pauseJob() {
        throw UnsupportedException.unsupportedException("TODO: implement pause action");
    }

    public void resumeJob() {
        throw UnsupportedException.unsupportedException("TODO: implement resume action");
    }

    public void onSchedule() throws Exception {
        if (!inSchedule.compareAndSet(false, true)) {
            return;
        }
        try {
            switch (state.get()) {
                case INIT:
                    Preconditions.checkState(false, "has not started");
                case STARTED:
                    prepare();
                    break;
                case PREPARING:
                case PAUSED:
                case FAILED:
                    Preconditions.checkState(false, "should not be scheduled");
                    break;
                case RUN_EPOCH:
                    epoch.onSchedule();
                    epochCoordinator.runEpoch(epoch);
                    break;
                default:
            }
        } finally {
            inSchedule.compareAndSet(true, false);
        }

    }

    /**
     * Trigger the incremental maintenance by transaction publish
     */
    public void onTransactionPublish() {
        LOG.info("onTransactionPublish: {}", this);
        if (this.state.get().equals(JobState.RUN_EPOCH)) {
            this.epoch.onReady();
        } else {
            throw UnsupportedException.unsupportedException("TODO: implement ");
        }
    }

    /**
     * Prepare the job
     * 0. Generate the physical job structure, including fragment distribution, parallelism
     * 1. Deploy tasks to executors on BE
     * 2. Trigger the epoch
     */
    void prepare() throws Exception {
        this.state.set(JobState.PREPARING);
        try {
            buildContext();

            buildPhysicalTopology();

            deployTasks();

            this.state.set(JobState.RUN_EPOCH);
            LOG.info("MV maintenance job prepared: {}", this.view.getName());
        } catch (Exception e) {
            this.state.set(JobState.FAILED);
            throw e;
        }
    }

    void buildContext() {
        // TODO(murphy) fill current user
        // Build connection context
        this.connectContext = StatisticUtils.buildConnectContext();
        Database db = GlobalStateMgr.getCurrentState().getDb(view.getDbId());
        this.connectContext.getSessionVariable().setQueryTimeoutS(MV_QUERY_TIMEOUT);
        if (db != null) {
            this.connectContext.setDatabase(db.getFullName());
        }

        // Build  query coordinator
        ExecPlan execPlan = this.view.getMaintenancePlan();
        List<PlanFragment> fragments = execPlan.getFragments();
        List<ScanNode> scanNodes = execPlan.getScanNodes();
        TDescriptorTable descTable = execPlan.getDescTbl().toThrift();
        JobSpec jobSpec = JobSpec.Factory.fromMVMaintenanceJobSpec(connectContext, fragments, scanNodes, descTable);
        this.queryCoordinator = new CoordinatorPreprocessor(connectContext, jobSpec);
        this.epochCoordinator = new TxnBasedEpochCoordinator(this);
    }

    public OlapTableSink getMVOlapTableSink() {
        Preconditions.checkState(CollectionUtils.isNotEmpty(plan.getFragments()));
        Preconditions.checkState(plan.getTopFragment().getSink() instanceof OlapTableSink);
        return (OlapTableSink) (plan.getTopFragment().getSink());
    }

    /**
     * Build physical fragments for the maintenance plan
     */
    void buildPhysicalTopology() throws Exception {
        if (CollectionUtils.isNotEmpty(plan.getFragments()) &&
                plan.getTopFragment().getSink() instanceof OlapTableSink) {
            ConnectContext context = queryCoordinator.getConnectContext();
            context.getSessionVariable().setPreferComputeNode(false);
            context.getSessionVariable().setUseComputeNodes(0);
            OlapTableSink dataSink = getMVOlapTableSink();
            // NOTE use a fake transaction id, the real one would be generated when epoch started
            long fakeTransactionId = 1;
            long dbId = getView().getDbId();
            long timeout = context.getSessionVariable().getQueryTimeoutS();
            dataSink.init(context.getExecutionId(), fakeTransactionId, dbId, timeout);
            dataSink.complete();
        }
        queryCoordinator.prepareExec();

        Map<PlanFragmentId, CoordinatorPreprocessor.ExecutionFragment> fragmentExecParams =
                queryCoordinator.getFragmentExecParamsMap();
        TDescriptorTable descTable = queryCoordinator.getDescriptorTable();
        boolean enablePipeline = true;
        int tabletSinkDop = 1;

        // Group all fragment instances by BE id, and package them into a task
        BiMap<Long, TNetworkAddress> taskId2Addr = HashBiMap.create();
        BiMap<TNetworkAddress, Long> addr2TaskId = taskId2Addr.inverse();
        Map<Long, MVMaintenanceTask> tasksByBe = new HashMap<>();
        long taskIdGen = 0;
        int backendIdGen = 0;
        for (Map.Entry<PlanFragmentId, CoordinatorPreprocessor.ExecutionFragment> kv : fragmentExecParams.entrySet()) {
            CoordinatorPreprocessor.ExecutionFragment execParams = kv.getValue();
            Set<TUniqueId> inflightInstanceSet =
                    execParams.instanceExecParams.stream()
                            .map(CoordinatorPreprocessor.FragmentInstance::getInstanceId)
                            .collect(Collectors.toSet());
            List<TExecPlanFragmentParams> tParams =
                    execParams.toThrift(inflightInstanceSet, descTable, enablePipeline, tabletSinkDop,
                            tabletSinkDop, true);
            for (int i = 0; i < execParams.instanceExecParams.size(); i++) {
                CoordinatorPreprocessor.FragmentInstance instanceParam = execParams.instanceExecParams.get(i);
                // Get brpc address instead of the default address
                TNetworkAddress beRpcAddr = queryCoordinator.getBrpcAddress(instanceParam.getWorkerId());
                Long taskId = addr2TaskId.get(beRpcAddr);
                MVMaintenanceTask task;
                if (taskId == null) {
                    taskId = taskIdGen++;
                    task = MVMaintenanceTask.build(this, taskId, beRpcAddr, new ArrayList<>());
                    tasksByBe.put(taskId, task);
                    addr2TaskId.put(beRpcAddr, taskId);
                } else {
                    task = tasksByBe.get(taskId);
                }

                // TODO(murphy) is this necessary
                int backendId = backendIdGen++;
                instanceParam.setBackendNum(backendId);
                task.addFragmentInstance(tParams.get(i));
            }
        }
        this.taskId2Addr = taskId2Addr;
        this.taskMap = tasksByBe;
    }

    /**
     * Deploy job on BE executors
     */
    private void deployTasks() throws Exception {
        QeProcessorImpl.QueryInfo queryInfo = QeProcessorImpl.QueryInfo.fromMVJob(getView().getMvId(), connectContext);
        QeProcessorImpl.INSTANCE.registerQuery(connectContext.getExecutionId(), queryInfo);

        List<Future<PMVMaintenanceTaskResult>> results = new ArrayList<>();
        for (MVMaintenanceTask task : taskMap.values()) {
            long taskId = task.getTaskId();
            TNetworkAddress address = task.getBeRpcAddr();
            // Build request
            TMVMaintenanceTasks request = new TMVMaintenanceTasks();
            request.setQuery_id(queryCoordinator.getQueryId());
            request.setTask_type(MVTaskType.START_MAINTENANCE);
            setMVMaintenanceTasksInfo(request, task);

            request.setStart_maintenance(new TMVMaintenanceStartTask());
            request.start_maintenance.setFragments(task.getFragmentInstances());

            try {
                LOG.debug("[MV] try deploy task at {}: {}", address, task);
                Future<PMVMaintenanceTaskResult> resultFuture =
                        BackendServiceClient.getInstance().submitMVMaintenanceTaskAsync(address, request);
                results.add(resultFuture);
            } catch (Exception e) {
                this.state.set(JobState.FAILED);
                LOG.warn("deploy job of MV {} failed: ", view.getName());
                throw new RuntimeException(e);
            }
        }

        // Wait for all RPC
        // TODO(murphy) make it event-driven instead of blocking wait
        Exception ex = null;
        for (Future<PMVMaintenanceTaskResult> future : results) {
            try {
                future.get();
            } catch (InterruptedException e) {
                if (ex == null) {
                    ex = e;
                }
                LOG.error("deploy MV task failed", e);
            }
        }
        if (ex != null) {
            throw ex;
        }
    }

    private void setMVMaintenanceTasksInfo(TMVMaintenanceTasks request,
                                           MVMaintenanceTask task) {
        // Request information
        String dbName = GlobalStateMgr.getCurrentState().getDb(view.getDbId()).getFullName();

        request.setDb_name(dbName);
        request.setMv_name(view.getName());
        request.setDb_id(dbId);
        request.setMv_id(viewId);
        request.setJob_id(getJobId());
        request.setTask_id(task.getTaskId());
    }

    private void stopTasks() throws Exception {
        QeProcessorImpl.INSTANCE.unregisterQuery(connectContext.getExecutionId());

        List<Future<PMVMaintenanceTaskResult>> results = new ArrayList<>();
        for (MVMaintenanceTask task : taskMap.values()) {
            LOG.info("stopTasks: {}", task);
            TMVMaintenanceTasks request = new TMVMaintenanceTasks();
            request.setQuery_id(connectContext.getExecutionId());
            request.setTask_type(MVTaskType.STOP_MAINTENANCE);
            setMVMaintenanceTasksInfo(request, task);
            request.setStop_maintenance(new TMVMaintenanceStopTask());
            TNetworkAddress address = task.getBeRpcAddr();

            try {
                results.add(BackendServiceClient.getInstance().submitMVMaintenanceTaskAsync(address, request));
            } catch (Exception e) {
                this.state.set(JobState.FAILED);
                LOG.warn("stop tasks of MV {} failed: ", view.getName());
                throw new RuntimeException(e);
            }
        }

        // Wait for all RPC
        Exception ex = null;
        for (Future<PMVMaintenanceTaskResult> future : results) {
            try {
                future.get();
            } catch (InterruptedException e) {
                if (ex == null) {
                    ex = e;
                }
                LOG.error("stop MV task failed", e);
            }
        }
        if (ex != null) {
            throw ex;
        }
    }

    public boolean isRunnable() {
        JobState jobState = state.get();
        switch (jobState) {
            case PREPARING:
            case STARTED:
                return true;
            case INIT:
            case PAUSED:
            case FAILED:
            case STOPPED:
                return false;
            case RUN_EPOCH:
                MVEpoch.EpochState state = epoch.getState();
                return state != MVEpoch.EpochState.INIT && state != MVEpoch.EpochState.FAILED;
            default:
                return false;
        }
    }

    public JobState getState() {
        return this.state.get();
    }

    public MaterializedView getView() {
        return view;
    }

    public long getJobId() {
        return jobId;
    }

    public TUniqueId getQueryId() {
        return connectContext.getExecutionId();
    }

    public CoordinatorPreprocessor getQueryCoordinator() {
        return queryCoordinator;
    }

    public Map<Long, MVMaintenanceTask> getTasks() {
        return taskMap;
    }

    public MVMaintenanceTask getTask(long taskId) {
        return taskMap.get(taskId);
    }

    private JobState getSerializedState() {
        return serializedState;
    }

    public long getViewId() {
        return viewId;
    }

    public long getDbId() {
        return dbId;
    }

    public MVEpoch getEpoch() {
        return epoch;
    }

    @Override
    public String toString() {
        return String.format("MVJob id=%s,dbId=%s,viewId=%d,epoch=%s, state=%s", jobId, dbId, viewId, epoch, state);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MVMaintenanceJob that = (MVMaintenanceJob) o;
        return jobId == that.jobId && viewId == that.viewId && Objects.equals(epoch, that.epoch) &&
                Objects.equals(state.get(), that.state.get());
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, viewId, epoch, state.get());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        serializedState = state.get();
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Override
    public void gsonPostProcess() throws IOException {
        state = new AtomicReference<>();
        inSchedule = new AtomicBoolean();
        state.set(serializedState);
    }

    @Override
    public void gsonPreProcess() throws IOException {
        serializedState = state.get();
    }

    /*
     *                            BuildMeta
     *                  ┌─────────┐      ┌─────────┐
     *                  │ CREATED ├─────►│ STARTED ├─────────────┐
     *                  └─────────┘      └────┬────┘             │
     *                                        │                  │
     *                                        │    OnSchedule    │
     *             Stop          ReSchedule   ▼                  ▼
     *  ┌────────┐      ┌─────────┐      ┌─────────┐         ┌────────┐
     *  │STOPPED │◄─────┤ PAUSED  ├─────►│PREPARING├────────►│ FAILED │
     *  └────────┘      └─────────┘      └────┬────┘         └────────┘
     *                      ▲                 │                  ▲
     *                      │                 │    Deploy        │
     *                      │                 ▼                  │
     *                      │            ┌─────────┐             │
     *                      └────────────┤RUN_EPOCH├─────────────┘
     *                                   └─────────┘
     */
    public enum JobState {
        // Just initialized
        INIT,

        // Wait for scheduling
        STARTED,

        // Preparing for the job
        PREPARING,

        // Pause the job, waiting for reschedule
        PAUSED,

        // Running the epoch
        RUN_EPOCH,

        // Stopped, no tasks on executors
        STOPPED,

        // Failed the whole job, needs to be destroyed
        FAILED
    }

}
