// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.staros;

import com.staros.manager.StarManager;
import com.staros.manager.StarManagerServer;
import com.starrocks.common.Config;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.StateChangeExecution;
import com.starrocks.journal.bdbje.BDBEnvironment;
import com.starrocks.journal.bdbje.BDBJEJournal;
import com.starrocks.leader.Checkpoint;
import com.starrocks.persist.Storage;
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

        String[] starMgrAddr = Config.starmgr_address.split(":");
        if (starMgrAddr.length != 2) {
            LOG.fatal("Config.starmgr_address {} bad format.", Config.starmgr_address);
            System.exit(-1);
        }
        int port = Integer.parseInt(starMgrAddr[1]);

        // necessary starMgr config setting
        com.staros.util.Config.STARMGR_IP = FrontendOptions.getLocalHostAddress();
        com.staros.util.Config.STARMGR_RPC_PORT = port;
        com.staros.util.Config.S3_BUCKET = Config.starmgr_s3_bucket;
        com.staros.util.Config.S3_REGION = Config.starmgr_s3_region;
        com.staros.util.Config.S3_ENDPOINT = Config.starmgr_s3_endpoint;
        com.staros.util.Config.S3_AK = Config.starmgr_s3_ak;
        com.staros.util.Config.S3_SK = Config.starmgr_s3_sk;

        // start rpc server
        starMgrServer = new StarManagerServer(journalSystem);
        starMgrServer.start(com.staros.util.Config.STARMGR_RPC_PORT);

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
}
