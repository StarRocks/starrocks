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


package com.starrocks.staros;

import com.staros.manager.StarManager;
import com.staros.manager.StarManagerServer;
import com.staros.metrics.MetricsSystem;
import com.starrocks.common.Config;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.StateChangeExecution;
import com.starrocks.journal.CheckpointWorker;
import com.starrocks.journal.StarMgrCheckpointWorker;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.leader.CheckpointController;
import com.starrocks.metric.MetricVisitor;
import com.starrocks.metric.PrometheusRegistryHelper;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class StarMgrServer {
    public static final String IMAGE_SUBDIR = "/starmgr"; // do not change this string!

    private static final Logger LOG = LogManager.getLogger(StarMgrServer.class);

    private static StarMgrServer CHECKPOINT = null;
    private CheckpointController checkpointController = null;
    private CheckpointWorker checkpointWorker = null;
    private boolean checkpointWorkerStarted = false;
    private static long checkpointThreadId = -1;
    private String imageDir;
    private StateChangeExecution execution;

    private static class SingletonHolder {
        private static final StarMgrServer INSTANCE = new StarMgrServer();
    }

    public static boolean isCheckpointThread() {
        return Thread.currentThread().getId() == checkpointThreadId;
    }

    public static void destroyCheckpoint() {
        if (CHECKPOINT != null) {
            CHECKPOINT = null;
        }
    }

    public static StarMgrServer getCurrentState() {
        if (isCheckpointThread()) {
            // only checkpoint thread it self will goes here.
            // so no need to care about thread safe.
            if (CHECKPOINT == null) {
                CHECKPOINT = new StarMgrServer(SingletonHolder.INSTANCE.getJournalSystem().getJournal());
            }
            return CHECKPOINT;
        } else {
            return SingletonHolder.INSTANCE;
        }
    }

    public static StarMgrServer getServingState() {
        return SingletonHolder.INSTANCE;
    }

    private StarManagerServer starMgrServer;
    private StarOSBDBJEJournalSystem journalSystem;

    public StarMgrServer() {
        execution = new StateChangeExecution() {
            @Override
            public void transferToLeader() {
                becomeLeader();
            }

            @Override
            public void transferToNonLeader(FrontendNodeType newType) {
                becomeFollower();
            }
        };
    }

    // for checkpoint thread only
    public StarMgrServer(BDBJEJournal journal) {
        journalSystem = new StarOSBDBJEJournalSystem(journal);
        starMgrServer = new StarManagerServer(journalSystem);
    }

    public StarManager getStarMgr() {
        return starMgrServer.getStarManager();
    }

    public StarOSBDBJEJournalSystem getJournalSystem() {
        return journalSystem;
    }

    public StateChangeExecution getStateChangeExecution() {
        return execution;
    }

    public void initialize(BDBEnvironment environment, String baseImageDir) throws IOException {
        journalSystem = new StarOSBDBJEJournalSystem(environment);
        imageDir = baseImageDir + IMAGE_SUBDIR;

        // TODO: remove separate deployment capability for now
        // necessary starMgr config setting
        com.staros.util.Config.STARMGR_IP = FrontendOptions.getLocalHostAddress();
        com.staros.util.Config.STARMGR_RPC_PORT = Config.cloud_native_meta_port;

        // Storage fs type
        com.staros.util.Config.DEFAULT_FS_TYPE = "";

        // use tablet_sched_disable_balance
        com.staros.util.Config.DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK = Config.tablet_sched_disable_balance;
        // turn on 0 as default worker group id, to be compatible with add/drop backend in FE
        com.staros.util.Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY = true;
        // set the same heartbeat configuration to starmgr, but not able to change in runtime.
        com.staros.util.Config.WORKER_HEARTBEAT_INTERVAL_SEC = Config.heartbeat_timeout_second;
        com.staros.util.Config.WORKER_HEARTBEAT_RETRY_COUNT = Config.heartbeat_retry_times;
        com.staros.util.Config.GRPC_RPC_TIME_OUT_SEC = Config.starmgr_grpc_timeout_seconds;
        com.staros.util.Config.ENABLE_BALANCE_SHARD_NUM_BETWEEN_WORKERS = Config.lake_enable_balance_tablets_between_workers;
        com.staros.util.Config.BALANCE_WORKER_SHARDS_THRESHOLD_IN_PERCENT = Config.lake_balance_tablets_threshold;
        com.staros.util.Config.SHARD_DEAD_REPLICA_EXPIRE_SECS = (int) Config.tablet_sched_be_down_tolerate_time_s;

        // sync the mutable configVar to StarMgr in case any changes
        GlobalStateMgr.getCurrentState().getConfigRefreshDaemon().registerListener(() -> {
            com.staros.util.Config.DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK = Config.tablet_sched_disable_balance;
            com.staros.util.Config.WORKER_HEARTBEAT_INTERVAL_SEC = Config.heartbeat_timeout_second;
            com.staros.util.Config.WORKER_HEARTBEAT_RETRY_COUNT = Config.heartbeat_retry_times;
            com.staros.util.Config.GRPC_RPC_TIME_OUT_SEC = Config.starmgr_grpc_timeout_seconds;
            com.staros.util.Config.ENABLE_BALANCE_SHARD_NUM_BETWEEN_WORKERS = Config.lake_enable_balance_tablets_between_workers;
            com.staros.util.Config.BALANCE_WORKER_SHARDS_THRESHOLD_IN_PERCENT = Config.lake_balance_tablets_threshold;
            com.staros.util.Config.SHARD_DEAD_REPLICA_EXPIRE_SECS = (int) Config.tablet_sched_be_down_tolerate_time_s;
        });
        // set the following config, in order to provide a customized worker group definition
        // com.staros.util.Config.RESOURCE_MANAGER_WORKER_GROUP_SPEC_RESOURCE_FILE = "";

        // use external resource provisioner service to provision/release worker group resource.
        // Keep this empty if using builtin one for testing
        // com.staros.util.Config.RESOURCE_PROVISIONER_ADDRESS = "";

        // turn on the following config, in case to use starmgr for internal multi-cluster testing
        // com.staros.util.Config.ENABLE_BUILTIN_RESOURCE_PROVISIONER_FOR_TEST = true;

        // set the following config, in order to enable the builtin test resource provisioner dump its meta to disk
        // com.staros.util.Config.BUILTIN_PROVISION_SERVER_DATA_DIR = "./";

        // start rpc server
        starMgrServer = new StarManagerServer(journalSystem);
        starMgrServer.start(com.staros.util.Config.STARMGR_RPC_PORT);

        StarOSAgent starOsAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        if (starOsAgent != null && !starOsAgent.init(starMgrServer)) {
            LOG.error("init star os agent failed.");
            System.exit(-1);
        }

        // load meta
        loadImage(imageDir);
    }

    private void becomeLeader() {
        getStarMgr().becomeLeader();
    }

    public void startCheckpointController() {
        // start checkpoint thread after everything is ready
        checkpointController = new CheckpointController(
                "star_os_checkpoint_controller", getJournalSystem().getJournal(), IMAGE_SUBDIR);
        checkpointController.start();
    }

    private void becomeFollower() {
        getStarMgr().becomeFollower();
    }

    public void startCheckpointWorker() {
        if (!checkpointWorkerStarted) {
            checkpointWorker = new StarMgrCheckpointWorker(getJournalSystem().getJournal());
            checkpointThreadId = checkpointWorker.getId();
            checkpointWorker.start();
            checkpointWorkerStarted = true;
            LOG.info("star mgr checkpoint worker thread started. thread id is {}.", checkpointThreadId);
        }
    }

    private void loadImage(String imageDir) throws IOException {
        Storage storage = new Storage(imageDir);
        File curFile = storage.getCurrentImageFile();
        if (!curFile.exists()) {
            // image.0 may not exist
            LOG.info("star mgr image does not exist in {}.", curFile.getAbsolutePath());
            return;
        }
        DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(curFile)));
        try {
            getStarMgr().loadMeta(in);
        } catch (EOFException eof) {
            LOG.warn("load star mgr image eof.");
        } finally {
            in.close();
        }
    }

    public void replayAndGenerateImage(String imageDir, long checkPointVersion) throws IOException {
        // 1. load base image
        loadImage(imageDir);

        // 2. replay incremental journal
        getJournalSystem().replayTo(checkPointVersion);
        if (getJournalSystem().getReplayId() != checkPointVersion) {
            throw new IOException(String.format("star mgr checkpoint version should be %d, actual replayed journal id is %d",
                    checkPointVersion, getJournalSystem().getReplayId()));
        }

        // 3. write new image
        // Write image.ckpt
        Storage storage = new Storage(imageDir);
        File imageFile = storage.getImageFile(getJournalSystem().getReplayId());
        File ckpt = new File(imageDir, Storage.IMAGE_NEW);
        if (!ckpt.exists()) {
            if (!ckpt.getParentFile().exists()) {
                LOG.info("create image dir for star mgr, {}.", ckpt.getParentFile().getAbsolutePath());
                if (!ckpt.getParentFile().mkdir()) {
                    String errorMessage = String.format("fail to create image dir %s for star mgr.",
                            ckpt.getAbsolutePath());
                    LOG.warn(errorMessage);
                    throw new IOException(errorMessage);
                }
            }
            if (!ckpt.createNewFile()) {
                LOG.warn("middle star mgr image {} already existed.", ckpt.getAbsolutePath());
            }
        }
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(ckpt))) {
            getStarMgr().dumpMeta(out);
        }
        // Move image.ckpt to image.dataVersion
        LOG.info("move star mgr " + ckpt.getAbsolutePath() + " to " + imageFile.getAbsolutePath());
        if (!ckpt.renameTo(imageFile)) {
            if (!ckpt.delete()) {
                LOG.warn("rename failed, fail to delete middle star mgr image " + ckpt.getAbsolutePath() + ".");
            }
            throw new IOException(String.format("failed to remove file %s to %s",
                    ckpt.getAbsolutePath(), imageFile.getAbsolutePath()));
        }
    }

    public void visitMetrics(MetricVisitor visitor) {
        if (starMgrServer == null) {
            return;
        }
        PrometheusRegistryHelper.visitPrometheusRegistry(MetricsSystem.METRIC_REGISTRY, visitor);
    }

    public long getMaxJournalId() {
        return getJournalSystem().getJournal().getMaxJournalId();
    }

    public long getReplayId() {
        return getJournalSystem().getReplayId();
    }

    public CheckpointController getCheckpointController() {
        return checkpointController;
    }

    public CheckpointWorker getCheckpointWorker() {
        return checkpointWorker;
    }

    public void triggerNewImage() {
        journalSystem.getJournalWriter().setForceRollJournal();
    }
}
