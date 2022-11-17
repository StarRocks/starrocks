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

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Long-running job responsible for MV incremental maintenance
 */
public class MVMaintenanceJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(MVMaintenanceJob.class);

    // Static state
    private final MaterializedView view;
    private ExecPlan plan;

    // Runtime state
    private final AtomicReference<JobState> state = new AtomicReference<>();
    private ConnectContext connectContext;
    // TODO(murphy) implement a real query coordinator
    private Coordinator queryCoordinator;
    private TxnBasedEpochExecutor epochCoordinator;

    public MVMaintenanceJob(MaterializedView view) {
        this.view = view;
        this.state.set(JobState.INIT);
    }

    /**
     * Main entrance of the job:
     * 0. Generate the physical job structure, including fragment distribution, parallelism
     * 1. Deploy tasks to executors on BE
     * 2. Trigger the epoch
     */
    public void start() {
        this.state.set(JobState.PREPARING);
        this.connectContext = new ConnectContext();
        this.queryCoordinator = new Coordinator();
        this.epochCoordinator = new TxnBasedEpochExecutor(this);
        deployJob();
        this.state.set(JobState.RUN_EPOCH);
    }

    public void stop() {
        this.state.set(JobState.PAUSED);
    }

    /**
     * Destroy the job and correspond state
     */
    public void destroy() {
        throw UnsupportedException.unsupportedException("TODO: implement destroy action");
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

    /**
     * On EpochCoordinator schedule
     */
    public void onSchedule() {
        if (state.get().equals(JobState.RUN_EPOCH)) {
            // TODO(murphy) make sure it would not lose any update
            throw UnsupportedException.unsupportedException("TODO: job is running, don't push me");
        } else {
            throw UnsupportedException.unsupportedException("TODO: implement ");
        }
    }

    /**
     * Trigger the incremental maintenance by transaction publish
     */
    public void onTransactionPublish() {
        this.state.set(JobState.RUN_EPOCH);
        this.epochCoordinator.start();
        throw UnsupportedException.unsupportedException("TODO: implement ");
    }

    /**
     * Build physical fragments for the maintenance plan
     */
    private void buildPhysicalFragments() {
        throw UnsupportedException.unsupportedException("TODO");
    }

    private void deployJob() {
        // FIXME(murphy) get real backend address for this job
        long beId = 0;
        Backend backend =
                Preconditions.checkNotNull(GlobalStateMgr.getCurrentSystemInfo().getBackend(beId),
                        "backend not found:" + beId);
        TNetworkAddress address = new TNetworkAddress(backend.getHost(), backend.getBePort());

        // Request information
        String dbName = GlobalStateMgr.getCurrentState().getDb(view.getDbId()).getFullName();
        // FIXME(murphy) build the real plan fragment params
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

    private void runEpoch() {
        try {
            epochCoordinator.run();
            LOG.debug("[MVJob] finish execution of job epoch: " + this);
        } catch (Exception e) {
            LOG.warn("job {} run epoch failed: {}", this, e);
            throw e;
        }
    }

    public boolean isRunnable() {
        JobState jobState = state.get();
        if (jobState == JobState.INIT || jobState == JobState.RUN_EPOCH) {
            return true;
        }
        return false;
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

        // Failed the whole job, needs reconstruction (unsupported environment change would cause job failure
        FAILED;
    }

}
