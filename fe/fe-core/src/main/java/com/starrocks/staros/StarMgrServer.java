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
import com.starrocks.common.Config;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.StateChangeExecution;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.leader.Checkpoint;
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
    private Checkpoint checkpointer = null;
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

    private StarManagerServer starMgrServer;
    private BDBJEJournalSystem journalSystem;

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
        journalSystem = new BDBJEJournalSystem(journal);
        starMgrServer = new StarManagerServer(journalSystem);
    }

    public StarManager getStarMgr() {
        return starMgrServer.getStarManager();
    }

    public BDBJEJournalSystem getJournalSystem() {
        return journalSystem;
    }

    public StateChangeExecution getStateChangeExecution() {
        return execution;
    }

    public void initialize(BDBEnvironment environment, String baseImageDir) throws IOException {
        journalSystem = new BDBJEJournalSystem(environment);
        imageDir = baseImageDir + IMAGE_SUBDIR;

        // TODO: remove separate deployment capability for now
        // necessary starMgr config setting
        com.staros.util.Config.STARMGR_IP = FrontendOptions.getLocalHostAddress();
        com.staros.util.Config.STARMGR_RPC_PORT = Config.cloud_native_meta_port;

        // Storage fs type
        com.staros.util.Config.DEFAULT_FS_TYPE = Config.cloud_native_storage_type;
        if (!com.staros.util.Config.DEFAULT_FS_TYPE.equals("HDFS") && !com.staros.util.Config.DEFAULT_FS_TYPE.equals("S3")) {
            LOG.error("invalid cloud native storage type: {}, must be HDFS or S3", com.staros.util.Config.DEFAULT_FS_TYPE);
            System.exit(-1);
        }
        // HDFS related configuration
        com.staros.util.Config.HDFS_URL = Config.cloud_native_hdfs_url;
        if (com.staros.util.Config.DEFAULT_FS_TYPE.equals("HDFS") && com.staros.util.Config.HDFS_URL.isEmpty()) {
            LOG.error("HDFS url is empty.");
            System.exit(-1);
        }
        // AWS related configuration
        String[] bucketAndPrefix = getBucketAndPrefix();
        com.staros.util.Config.S3_BUCKET = bucketAndPrefix[0];
        com.staros.util.Config.S3_PATH_PREFIX = bucketAndPrefix[1];
        com.staros.util.Config.S3_REGION = Config.aws_s3_region;
        com.staros.util.Config.S3_ENDPOINT = Config.aws_s3_endpoint;
        if (com.staros.util.Config.DEFAULT_FS_TYPE.equals("S3") && com.staros.util.Config.S3_BUCKET.isEmpty()) {
            LOG.error("S3 bucket is empty.");
            System.exit(-1);
        }
        // aws credential related configuration
        String credentialType = getAwsCredentialType();
        if (credentialType == null) {
            LOG.error("invalid aws credential configuration.");
            System.exit(-1);
        }
        com.staros.util.Config.AWS_CREDENTIAL_TYPE = credentialType;
        com.staros.util.Config.SIMPLE_CREDENTIAL_ACCESS_KEY_ID = Config.aws_s3_access_key;
        com.staros.util.Config.SIMPLE_CREDENTIAL_ACCESS_KEY_SECRET = Config.aws_s3_secret_key;
        com.staros.util.Config.ASSUME_ROLE_CREDENTIAL_ARN = Config.aws_s3_iam_role_arn;
        com.staros.util.Config.ASSUME_ROLE_CREDENTIAL_EXTERNAL_ID = Config.aws_s3_external_id;
        // use tablet_sched_disable_balance
        com.staros.util.Config.DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK = Config.tablet_sched_disable_balance;
        // turn on 0 as default worker group id, to be compatible with add/drop backend in FE
        com.staros.util.Config.ENABLE_ZERO_WORKER_GROUP_COMPATIBILITY = true;
        // set the same heartbeat configuration to starmgr, but not able to change in runtime.
        com.staros.util.Config.WORKER_HEARTBEAT_INTERVAL_SEC = Config.heartbeat_timeout_second;
        com.staros.util.Config.WORKER_HEARTBEAT_RETRY_COUNT = Config.heartbeat_retry_times;

        // sync the mutable configVar to StarMgr in case any changes
        GlobalStateMgr.getCurrentState().getConfigRefreshDaemon().registerListener(() -> {
            com.staros.util.Config.DISABLE_BACKGROUND_SHARD_SCHEDULE_CHECK = Config.tablet_sched_disable_balance;
            com.staros.util.Config.WORKER_HEARTBEAT_INTERVAL_SEC = Config.heartbeat_timeout_second;
            com.staros.util.Config.WORKER_HEARTBEAT_RETRY_COUNT = Config.heartbeat_retry_times;
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

        // start checkpoint thread after everything is ready
        checkpointer = new Checkpoint("star mgr LeaderCheckpointer", getJournalSystem().getJournal(), IMAGE_SUBDIR,
                false /* belongToGlobalStateMgr */);
        checkpointThreadId = checkpointer.getId();
        checkpointer.start();
        LOG.info("star mgr checkpointer thread started. thread id is {}.", checkpointThreadId);
    }

    private void becomeFollower() {
        getStarMgr().becomeFollower();
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

    public boolean replayAndGenerateImage(String imageDir, long checkPointVersion) throws IOException {
        // 1. load base image
        loadImage(imageDir);

        // 2. replay incremental journal
        getJournalSystem().replayTo(checkPointVersion);
        if (getJournalSystem().getReplayId() != checkPointVersion) {
            LOG.error("star mgr checkpoint version should be {}, actual replayed journal id is {}",
                    checkPointVersion, getJournalSystem().getReplayId());
            return false;
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
                    LOG.warn("fail to create image dir {} for star mgr." + ckpt.getAbsolutePath());
                    throw new IOException();
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
            if (ckpt.delete()) {
                LOG.warn("rename failed, fail to delete middle star mgr image " + ckpt.getAbsolutePath() + ".");
            }
            throw new IOException();
        }

        return true;
    }

    public long getMaxJournalId() {
        return getJournalSystem().getJournal().getMaxJournalId();
    }

    public long getReplayId() {
        return getJournalSystem().getReplayId();
    }

    public static String[] getBucketAndPrefix() {
        int index = Config.aws_s3_path.indexOf('/');
        if (index < 0) {
            return new String[] {Config.aws_s3_path, ""};
        }

        return new String[] {Config.aws_s3_path.substring(0, index), 
                Config.aws_s3_path.substring(index + 1)};
    }

    public static String getAwsCredentialType() {
        if (Config.aws_s3_use_aws_sdk_default_behavior) {
            return "default";
        }

        if (Config.aws_s3_use_instance_profile) {
            if (Config.aws_s3_iam_role_arn.isEmpty()) {
                return "instance_profile";
            }

            return "assume_role";
        }

        if (Config.aws_s3_access_key.isEmpty() || Config.aws_s3_secret_key.isEmpty()) {
            // invalid credential configuration
            return null;
        }

        if (Config.aws_s3_iam_role_arn.isEmpty()) {
            return "simple";
        }

        //assume_role with ak sk, not supported now, just return null
        return null;
    }
}
