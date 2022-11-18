// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.io.Writable;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Long-running job responsible for MV incremental maintenance.
 * <p>
 * Each job is event driven and single-thread execution:
 * 1. Event driven: transaction commitment drives the execution of job
 * 2. Execution: the job is executed in
 */
public class MVMaintenanceJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(MVMaintenanceJob.class);

    // Persisted state
    // TODO(murphy) persist these state through edit-log
    private final MaterializedView view;
    private ExecPlan plan;
    private final AtomicReference<JobState> state = new AtomicReference<>();
    private final MVEpoch epoch;

    // Runtime ephemeral state
    private ConnectContext connectContext;
    // TODO(murphy) implement a real query coordinator
    private Coordinator queryCoordinator;
    private TxnBasedEpochCoordinator epochCoordinator;

    public MVMaintenanceJob(MaterializedView view) {
        this.view = view;
        this.epoch = new MVEpoch(view.getDbId());
        this.state.set(JobState.INIT);
    }

    public void stopJob() {
        state.set(JobState.PAUSED);
    }

    public void pauseJob() {
        throw UnsupportedException.unsupportedException("TODO: implement pause action");
    }

    public void continueJob() {
        throw UnsupportedException.unsupportedException("TODO: implement continue action");
    }

    public void runDaemon() {
        throw UnsupportedException.unsupportedException("TODO: implement the daemon runner");
    }

    public void onSchedule() {
        switch (state.get()) {
            case INIT:
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
    private void prepare() {
        this.state.set(JobState.PREPARING);
        // TODO(murphy) fill connection context
        this.connectContext = new ConnectContext();
        this.connectContext.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        this.queryCoordinator = new Coordinator();
        this.epochCoordinator = new TxnBasedEpochCoordinator(this);
        deployJob();
        this.state.set(JobState.RUN_EPOCH);
    }

    /**
     * Build physical fragments for the maintenance plan
     */
    private void buildPhysicalTopology() {
        throw UnsupportedException.unsupportedException("TODO");
    }

    /**
     * Deploy job on BE executors
     */
    private void deployJob() {
        // FIXME(murphy) build the real plan fragment params
        // FIXME(murphy) get real backend address for job topology
        long beId = 0;
        Backend backend =
                Preconditions.checkNotNull(GlobalStateMgr.getCurrentSystemInfo().getBackend(beId),
                        "backend not found:" + beId);
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());

        // Request information
        String dbName = GlobalStateMgr.getCurrentState().getDb(view.getDbId()).getFullName();
        List<TExecPlanFragmentParams> planParams = queryCoordinator.buildExecRequests();
        long taskId = 0;
        TExecPlanFragmentParams planParam = planParams.get(0);

        // Build request
        TMVMaintenanceTasks request = new TMVMaintenanceTasks();
        request.setTask_type(MVTaskType.START_MAINTENANCE);
        request.setStart_maintenance(new TMVMaintenanceStartTask());
        request.start_maintenance.setJob_id(getJobId());
        request.start_maintenance.setTask_id(taskId);
        request.start_maintenance.setDb_name(dbName);
        request.start_maintenance.setMv_name(view.getName());
        request.start_maintenance.setPlan_params(planParam);

        try {
            BackendServiceClient.getInstance().submitMVMaintenanceTaskAsync(address, request);
        } catch (Exception e) {
            this.state.set(JobState.FAILED);
            LOG.warn("deploy job of MV {} failed: ", view.getName());
            throw new RuntimeException(e);
        }
    }

    public boolean isRunnable() {
        JobState jobState = state.get();
        switch (jobState) {
            case INIT:
            case PREPARING:
                return true;
            case PAUSED:
            case FAILED:
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

    @Override
    public String toString() {
        return String.format("MVJob of %s/%s", view.getName(), view.getId());
    }

    public static MVMaintenanceJob read(DataInput input) {
        throw UnsupportedException.unsupportedException("TODO");
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw UnsupportedException.unsupportedException("TODO");
    }

    public enum JobState {
        // Just initialized
        INIT,

        // Preparing for the job
        PREPARING,

        // Pause the job, waiting for the continue event
        PAUSED,

        // Running the epoch
        RUN_EPOCH,

        // Failed the whole job, needs reconstruction
        FAILED
    }

}
