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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.ScanNode;
import com.starrocks.proto.PMVMaintenanceTaskResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TDescriptorTable;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TMVMaintenanceStartTask;
import com.starrocks.thrift.TMVMaintenanceStopTask;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TQueryGlobals;
import com.starrocks.thrift.TQueryOptions;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Long-running job responsible for MV incremental maintenance.
 * <p>
 * Each job is event driven and single-thread execution:
 * 1. Event driven: job state machine and execution is separated, and the state machine is driven by events
 * 2. Execution: the job is executed in JobExecutor, at most one thread could execute the job
 */
public class MVMaintenanceJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(MVMaintenanceJob.class);
    private static final int MV_QUERY_TIMEOUT = 120;

    // Persisted state
    @SerializedName("jobId")
    private final long jobId;
    @SerializedName("viewId")
    private final long viewId;
    @SerializedName("state")
    private JobState serializedState;
    @SerializedName("epoch")
    private final MVEpoch epoch;

    // TODO(murphy) serialize the plan, current we need to rebuild the plan for job
    private transient ExecPlan plan;

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

    public MVMaintenanceJob(MaterializedView view) {
        this.jobId = view.getId();
        this.viewId = view.getId();
        this.view = view;
        this.epoch = new MVEpoch(view.getId());
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
    private void prepare() throws Exception {
        this.state.set(JobState.PREPARING);
        try {
            // TODO(murphy) fill current user
            // Build connection context
            this.connectContext = StatisticUtils.buildConnectContext();
            Database db = GlobalStateMgr.getCurrentState().getDb(view.getDbId());
            this.connectContext.getSessionVariable().setQueryTimeoutS(MV_QUERY_TIMEOUT);
            if (db != null) {
                this.connectContext.setDatabase(db.getFullName());
            }
            TUniqueId queryId = connectContext.getExecutionId();

            // Build  query coordinator
            ExecPlan execPlan = this.view.getMaintenancePlan();
            List<PlanFragment> fragments = execPlan.getFragments();
            List<ScanNode> scanNodes = execPlan.getScanNodes();
            TDescriptorTable descTable = execPlan.getDescTbl().toThrift();
            TQueryGlobals queryGlobals =
                    CoordinatorPreprocessor.genQueryGlobals(connectContext.getStartTime(),
                            connectContext.getSessionVariable().getTimeZone());
            TQueryOptions queryOptions = connectContext.getSessionVariable().toThrift();
            this.queryCoordinator =
                    new CoordinatorPreprocessor(queryId, connectContext, fragments, scanNodes, descTable, queryGlobals,
                            queryOptions);
            this.epochCoordinator = new TxnBasedEpochCoordinator(this);

            // Build physical plan instance
            buildPhysicalTopology();

            // Build tasks
            deployTasks();
            this.state.set(JobState.RUN_EPOCH);
            LOG.info("MV maintenance job prepared: {}", this.view.getName());
        } catch (Exception e) {
            this.state.set(JobState.FAILED);
            throw e;
        }
    }

    /**
     * FIXME(murphy) build the real plan fragment params, this is not a valid plan fragment, since current coordinator
     * is hard to reuse
     * <p>
     * Build physical fragments for the maintenance plan
     */
    @VisibleForTesting
    private void buildPhysicalTopology() throws Exception {
        queryCoordinator.prepareExec();

        Map<PlanFragmentId, CoordinatorPreprocessor.FragmentExecParams> fragmentExecParams =
                queryCoordinator.getFragmentExecParamsMap();
        // FIXME(murphy) all of these are faked
        Set<TUniqueId> instanceIds = new HashSet<>();
        TDescriptorTable descTable = queryCoordinator.getDescriptorTable();
        Set<Long> dbIds = connectContext.getCurrentSqlDbIds();
        boolean enablePipeline = true;
        int tabletSinkDop = 1;

        // Group all fragment instances by BE id, and package them into a task
        Map<Long, MVMaintenanceTask> tasksByBe = new HashMap<>();
        int taskId = 0;
        for (Map.Entry<PlanFragmentId, CoordinatorPreprocessor.FragmentExecParams> kv : fragmentExecParams.entrySet()) {
            CoordinatorPreprocessor.FragmentExecParams execParams = kv.getValue();
            List<TExecPlanFragmentParams> tParams =
                    execParams.toThrift(instanceIds, descTable, dbIds, enablePipeline, tabletSinkDop, tabletSinkDop);
            Preconditions.checkState(tParams.size() == execParams.instanceExecParams.size());
            for (int i = 0; i < execParams.instanceExecParams.size(); i++) {
                long beId = execParams.instanceExecParams.get(i).getBackendNum();
                TNetworkAddress beHost = execParams.instanceExecParams.get(i).getHost();
                MVMaintenanceTask task =
                        tasksByBe.computeIfAbsent(beId,
                                k -> MVMaintenanceTask.build(this, taskId, beId, beHost, new ArrayList<>()));
                task.addFragmentInstance(tParams.get(i));
            }
        }
        this.taskMap = tasksByBe;
    }

    /**
     * FIXME(murphy) get real backend address for job topology
     * Deploy job on BE executors
     */
    private void deployTasks() throws Exception {
        List<Future<PMVMaintenanceTaskResult>> results = new ArrayList<>();
        for (MVMaintenanceTask task : taskMap.values()) {
            LOG.info("deployTasks: {}", task);
            long taskId = task.getTaskId();
            TNetworkAddress address = SystemInfoService.toBrpcHost(task.getBeHost());
            // Request information
            String dbName = GlobalStateMgr.getCurrentState().getDb(view.getDbId()).getFullName();

            // Build request
            TMVMaintenanceTasks request = new TMVMaintenanceTasks();
            request.setQuery_id(queryCoordinator.getQueryId());
            request.setTask_type(MVTaskType.START_MAINTENANCE);
            request.setJob_id(getJobId());
            request.setTask_id(taskId);
            request.setStart_maintenance(new TMVMaintenanceStartTask());
            request.setDb_name(dbName);
            request.setMv_name(view.getName());
            request.start_maintenance.setFragments(task.getFragmentInstances());

            try {
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

    private void stopTasks() throws Exception {
        List<Future<PMVMaintenanceTaskResult>> results = new ArrayList<>();
        for (MVMaintenanceTask task : taskMap.values()) {
            LOG.info("stopTasks: {}", task);
            long beId = task.getBeId();
            TMVMaintenanceTasks request = new TMVMaintenanceTasks();
            request.setQuery_id(connectContext.getExecutionId());
            request.setTask_type(MVTaskType.STOP_MAINTENANCE);
            request.setJob_id(getJobId());
            request.setTask_id(task.getTaskId());
            request.setStop_maintenance(new TMVMaintenanceStopTask());
            Backend backend =
                    Preconditions.checkNotNull(GlobalStateMgr.getCurrentSystemInfo().getBackend(beId),
                            "backend not found:" + beId);
            TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());

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

    public MVEpoch getEpoch() {
        return epoch;
    }

    @Override
    public String toString() {
        return String.format("MVJob id=%s,viewId=%d,epoch=%s, state=%s", jobId, viewId, epoch, state);
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
