// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.proto.PMVMaintenanceTaskResult;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.Coordinator;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.system.Backend;
import com.starrocks.thrift.MVTaskType;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TMVMaintenanceStartTask;
import com.starrocks.thrift.TMVMaintenanceTasks;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TUniqueId;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
@Data
public class MVMaintenanceJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(MVMaintenanceJob.class);

    // Persisted state
    @SerializedName("jobId")
    private final long jobId;
    @SerializedName("viewId")
    private final long viewId;
    @SerializedName("state")
    private final AtomicReference<JobState> state = new AtomicReference<>();
    @SerializedName("epoch")
    private final MVEpoch epoch;

    // TODO(murphy) serialize the plan, current we need to rebuild the plan for job
    private ExecPlan plan;

    // Runtime ephemeral state
    // At most one thread could execute this job, this flag indicates is someone scheduling this job
    private final AtomicBoolean inSchedule = new AtomicBoolean(false);
    private final MaterializedView view;
    private ConnectContext connectContext;
    // TODO(murphy) implement a real query coordinator
    private Coordinator queryCoordinator;
    private TxnBasedEpochCoordinator epochCoordinator;
    private List<MVMaintenanceTask> tasks;

    public MVMaintenanceJob(MaterializedView view) {
        this.jobId = view.getId();
        this.viewId = view.getId();
        this.view = view;
        this.epoch = new MVEpoch(view.getId());
        this.state.set(JobState.INIT);
        this.plan = Preconditions.checkNotNull(view.getMaintenancePlan());
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

    public void continueJob() {
        throw UnsupportedException.unsupportedException("TODO: implement continue action");
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
                    epochCoordinator.beginEpoch(epoch);
                    epochCoordinator.commitEpoch(epoch);
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
            // TODO(murphy) fill connection context
            this.connectContext = new ConnectContext();
            this.connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            this.queryCoordinator = new Coordinator();
            this.epochCoordinator = new TxnBasedEpochCoordinator(this);

            buildPhysicalTopology();
            deployTasks();
            this.state.set(JobState.RUN_EPOCH);
            LOG.info("MV maintenance job prepared: {}", this.view.getName());
        } catch (Exception e) {
            this.state.set(JobState.FAILED);
            throw e;
        }
    }

    /**
     * FIXME(murphy) build the real plan fragment params
     * <p>
     * Build physical fragments for the maintenance plan
     */
    private void buildPhysicalTopology() {
        List<TExecPlanFragmentParams> planFragmentParams = queryCoordinator.buildExecRequests();
        this.tasks = new ArrayList<>();
        for (int taskId = 0; taskId < planFragmentParams.size(); taskId++) {
            TExecPlanFragmentParams instance = planFragmentParams.get(taskId);
            TUniqueId instanceId = instance.params.fragment_instance_id;
            // TODO(murphy) retrieve actual id of plan
            PlanFragmentId fragmentId = new PlanFragmentId(0);
            long beId = 0;
            MVMaintenanceTask task = MVMaintenanceTask.build(this, taskId, beId, fragmentId, instanceId, instance);
            this.tasks.add(task);
        }
    }

    /**
     * FIXME(murphy) get real backend address for job topology
     * Deploy job on BE executors
     */
    private void deployTasks() throws Exception {
        List<Future<PMVMaintenanceTaskResult>> results = new ArrayList<>();
        for (MVMaintenanceTask task : tasks) {
            long beId = task.getBeId();
            long taskId = task.getTaskId();
            Backend backend =
                    Preconditions.checkNotNull(GlobalStateMgr.getCurrentSystemInfo().getBackend(beId),
                            "backend not found:" + beId);
            TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());
            // Request information
            String dbName = GlobalStateMgr.getCurrentState().getDb(view.getDbId()).getFullName();
            TExecPlanFragmentParams planParam = task.getFragmentInstance();

            // Build request
            TMVMaintenanceTasks request = new TMVMaintenanceTasks();
            request.setTask_type(MVTaskType.START_MAINTENANCE);
            request.setJob_id(getJobId());
            request.setTask_id(taskId);
            request.setStart_maintenance(new TMVMaintenanceStartTask());
            request.start_maintenance.setDb_name(dbName);
            request.start_maintenance.setMv_name(view.getName());
            request.start_maintenance.setPlan_params(planParam);

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
                future.wait();
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
        for (MVMaintenanceTask task : tasks) {
            long beId = task.getBeId();
            TMVMaintenanceTasks request = new TMVMaintenanceTasks();
            request.setTask_type(MVTaskType.STOP_MAINTENANCE);
            request.setJob_id(getJobId());
            request.setTask_id(task.getTaskId());
            request.setStart_maintenance(new TMVMaintenanceStartTask());
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
                future.wait();
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
        return view.getId();
    }

    public Coordinator getQueryCoordinator() {
        return queryCoordinator;
    }

    public List<MVMaintenanceTask> getTasks() {
        return tasks;
    }

    @Override
    public String toString() {
        return String.format("MVJob of %s/%s", view.getName(), view.getId());
    }

    public static MVMaintenanceJob read(DataInput input) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(input), MVMaintenanceJob.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
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
