// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.QueryableReentrantLock;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.common.UnsupportedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manage lifetime of MV, including create, build, refresh, destroy
 * TODO(Murphy) refactor all MV management code into here
 */
public class MVManager {
    private static final Logger LOG = LogManager.getLogger(LocalMetastore.class);
    private static final MVManager INSTANCE = new MVManager();

    // Protect all access into job states
    private QueryableReentrantLock lock = new QueryableReentrantLock();
    private final Map<MvId, MVMaintenanceJob> jobMap = new ConcurrentHashMap<>();

    private MVManager() {
    }

    public static MVManager getInstance() {
        return INSTANCE;
    }

    public MaterializedView createSinkTable(CreateMaterializedViewStatement stmt, long mvId, long dbId)
            throws DdlException {
        return IMTCreator.createSinkTable(stmt, mvId, dbId);
    }

    /**
     * Prepare maintenance work for materialized view
     * 1. Create Intermediate Materialized Table(IMT)
     * 2. Store metadata of Job in metastore
     * 3. Create job for MV maintenance
     * 4. Compute physical topology of MV job
     */
    public void prepareMaintenanceWork(CreateMaterializedViewStatement stmt, MaterializedView view)
            throws DdlException {
        if (!view.getRefreshScheme().isIncremental()) {
            return;
        }

        try {
            lock.lock();
            if (jobMap.get(view.getMvId()) != null) {
                throw new DdlException("MV already existed");
            }

            IMTCreator.createIMT(stmt, view);

            // Create the job but not execute it
            MVMaintenanceJob job = new MVMaintenanceJob(view);
            jobMap.put(view.getMvId(), job);

            // TODO(murphy) atomic persist the meta of MV (IMT, MaintenancePlan) along with materialized view
            GlobalStateMgr.getCurrentState().getEditLog().logMVJobState(job);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Start the maintenance job for MV after created
     * 1. Schedule the maintenance task in executor
     * 2. Coordinate the epoch/transaction progress: reading changelog of base table and incremental refresh the MV
     * 3. Maintain the visibility of MV and job metadata
     */
    public void startMaintainMV(MaterializedView view) {
        if (view.getRefreshScheme().isIncremental()) {
            return;
        }
        MVMaintenanceJob job = Preconditions.checkNotNull(getJob(view.getMvId()));
        job.startJob();
    }

    public void pauseMaintainMV(MaterializedView view) {
        if (!view.getRefreshScheme().isIncremental()) {
            return;
        }
        MVMaintenanceJob job = Preconditions.checkNotNull(getJob(view.getMvId()));
        job.pauseJob();
    }

    /**
     * Stop the maintenance job for MV after dropped
     */
    public void stopMaintainMV(MaterializedView view) {
        if (!view.getRefreshScheme().isIncremental()) {
            return;
        }
        MVMaintenanceJob job = Preconditions.checkNotNull(getJob(view.getMvId()));
        job.pauseJob();
        job.stopJob();
    }

    /**
     * Refresh the MV
     */
    public void onTxnPublish(MaterializedView view) {
        MVMaintenanceJob job = Preconditions.checkNotNull(getJob(view.getMvId()));
        job.onTransactionPublish();
    }

    /**
     * Re-Schedule the MV maintenance job
     * 1. Tablet migration
     * 2. Cluster rescale
     * 3. Temporary failure happens(FE or BE)
     */
    public void reScheduleMaintainMV(MvId mvId) {
        throw UnsupportedException.unsupportedException("re-schedule mv job is not supported");
    }

    /**
     * Rebuild the maintenance job for critical changes
     * 1. Schema change of MV (should be optimized later)
     * 2. Binlog lost
     * 3. Base table schema change (should be optimized later)
     */
    public void rebuildMaintainMV(MaterializedView view) {
        if (!view.getRefreshScheme().isIncremental()) {
            return;
        }
        throw UnsupportedException.unsupportedException("rebuild mv job is not supported");
    }

    private MVMaintenanceJob getJob(MvId mvId) {
        try {
            lock.lock();
            return jobMap.get(mvId);
        } finally {
            lock.unlock();
        }
    }

    public List<MVMaintenanceJob> getRunnableJobs() {
        try {
            lock.lock();
            return this.jobMap.values().stream().filter(MVMaintenanceJob::isRunnable).collect(Collectors.toList());
        } finally {
            lock.unlock();
        }
    }

}
