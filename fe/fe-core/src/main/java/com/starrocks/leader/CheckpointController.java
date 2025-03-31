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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/master/Checkpoint.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.leader;

import com.google.common.base.Strings;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.NetUtils;
import com.starrocks.http.meta.MetaService;
import com.starrocks.journal.CheckpointException;
import com.starrocks.journal.CheckpointWorker;
import com.starrocks.journal.Journal;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.MetaCleaner;
import com.starrocks.persist.Storage;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TStartCheckpointRequest;
import com.starrocks.thrift.TStartCheckpointResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * CheckpointController daemon is running on master node. handle the checkpoint work for starrocks.
 */
public class CheckpointController extends FrontendDaemon {
    public static final Logger LOG = LogManager.getLogger(CheckpointController.class);
    private static final int PUT_TIMEOUT_SECOND = 3600;
    private static final int CONNECT_TIMEOUT_SECOND = 1;
    private static final int READ_TIMEOUT_SECOND = 1;
    private static final ReentrantReadWriteLock RW_LOCK = new ReentrantReadWriteLock();

    private String imageDir;
    private final Journal journal;
    // subDir comes after base imageDir, to distinguish different module's image dir
    private final String subDir;
    private final boolean belongToGlobalStateMgr;

    private final Set<String> nodesToPushImage;

    private volatile String workerNodeName;
    private volatile long workerSelectedTime;
    private volatile long journalId;
    private volatile BlockingQueue<Pair<Boolean, String>> result;

    public CheckpointController(String name, Journal journal, String subDir) {
        super(name, FeConstants.checkpoint_interval_second * 1000L);
        this.journal = journal;
        this.subDir = subDir;
        this.belongToGlobalStateMgr = Strings.isNullOrEmpty(subDir);
        nodesToPushImage = new HashSet<>();
    }

    public static void exclusiveLock() {
        RW_LOCK.writeLock().lock();
    }

    public static void exclusiveUnlock() {
        RW_LOCK.writeLock().unlock();
    }

    @Override
    protected void runAfterCatalogReady() {
        RW_LOCK.readLock().lock();
        try {
            runCheckpointController();
        } finally {
            RW_LOCK.readLock().unlock();
        }
    }

    protected void runCheckpointController() {
        init();

        // ignore return value in normal checkpoint controller
        runCheckpointControllerWithIds(getImageJournalId(), getCheckpointJournalId());
    }

    public long getCheckpointJournalId() {
        return journal.getFinalizedJournalId();
    }

    public long getImageJournalId() {
        long imageJournalId = 0;
        try {
            Storage storage = new Storage(imageDir);
            // get max image version
            imageJournalId = storage.getImageJournalId();
        } catch (IOException e) {
            LOG.error("Failed to get storage info", e);
        }
        return imageJournalId;
    }

    public Pair<Boolean, String> runCheckpointControllerWithIds(long imageJournalId, long maxJournalId) {
        LOG.info("checkpoint imageJournalId {}, logJournalId {}", imageJournalId, maxJournalId);

        // Step 1: create image
        Pair<Boolean, String> createImageRet = Pair.create(false, "");
        if (imageJournalId < maxJournalId) {
            this.journalId = maxJournalId;
            createImageRet = createImage();
        }
        if (createImageRet.first) {
            // Push the image file to all other nodes
            // NOTE: Do not get other nodes from HaProtocol, because the node may not be in bdbje replication group yet.
            for (Frontend frontend : GlobalStateMgr.getServingState().getNodeMgr().getOtherFrontends()) {
                // do not push to the worker node
                if (!frontend.getNodeName().equals(createImageRet.second)) {
                    nodesToPushImage.add(frontend.getNodeName());
                }
            }
        }

        // Step2: push image
        int needToPushCnt = nodesToPushImage.size();
        long newImageVersion = createImageRet.first ? maxJournalId : imageJournalId;
        if (needToPushCnt > 0) {
            pushImage(newImageVersion);
        }

        // Step3: Delete old journals
        // conditions: 1. new image created and no others node to push, this means there is only one FE in the cluster,
        //                delete the old journals immediately.
        //             2. needToPushCnt > 0 means there are other nodes in the cluster,
        //                we must make sure all the other nodes have got the new image and then delete old journals.
        if ((createImageRet.first && needToPushCnt == 0)
                || (needToPushCnt > 0 && nodesToPushImage.isEmpty())) {
            deleteOldJournals(newImageVersion);
        }

        return createImageRet;
    }

    private void init() {
        this.imageDir = GlobalStateMgr.getServingState().getImageDir() + subDir;
    }

    private Pair<Boolean, String> createImage() {
        result = new ArrayBlockingQueue<>(1);
        workerNodeName = selectWorker();
        if (workerNodeName == null) {
            LOG.warn("Failed to select worker to do checkpoint, journalId: {}", journalId);
            return Pair.create(false, workerNodeName);
        }
        workerSelectedTime = System.currentTimeMillis();

        // check the worker node is available
        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getFeByName(workerNodeName);
        if (frontend == null || !frontend.isAlive()) {
            LOG.warn("worker node: {} is not available", workerNodeName);
            return Pair.create(false, workerNodeName);
        }

        try {
            Pair<Boolean, String> ret = result.poll(Config.checkpoint_timeout_seconds, TimeUnit.SECONDS);
            if (ret == null) {
                LOG.warn("do checkpoint timeout on node: {}", workerNodeName);
                return Pair.create(false, workerNodeName);
            }
            if (!ret.first) {
                LOG.warn("do checkpoint failed on node: {}, reason: {}", workerNodeName, ret.second);
                return Pair.create(false, workerNodeName);
            }

            // download Image
            downloadImage();
            return Pair.create(true, workerNodeName);
        } catch (Exception e) {
            LOG.warn("create image failed", e);
            return Pair.create(false, workerNodeName);
        } finally {
            workerNodeName = null;
        }
    }

    private void downloadImage() throws IOException {
        // if worker is self, do not download.
        if (workerNodeName.equals(GlobalStateMgr.getCurrentState().getNodeMgr().getNodeName())) {
            return;
        }

        try {
            downloadImage(ImageFormatVersion.v1, imageDir);
        } catch (IOException e) {
            if (belongToGlobalStateMgr && e instanceof FileNotFoundException) {
                LOG.warn("download image of v1 version failed, ignore", e);
            } else {
                throw e;
            }
        }

        if (belongToGlobalStateMgr) {
            downloadImage(ImageFormatVersion.v2, imageDir + "/v2");
            GlobalStateMgr.getCurrentState().setImageJournalId(journalId);
        }
    }

    private void downloadImage(ImageFormatVersion imageFormatVersion, String imageDir) throws IOException {
        Frontend frontend = GlobalStateMgr.getCurrentState().getNodeMgr().getFeByName(workerNodeName);
        if (frontend == null || !frontend.isAlive()) {
            String errMessage = String.format("worker node: %s not available", workerNodeName);
            LOG.warn(errMessage);
            throw new IOException(errMessage);
        }
        File dir = new File(imageDir);
        String url = "http://" + NetUtils.getHostPortInAccessibleFormat(frontend.getHost(), Config.http_port) +
                "/image?version=" + journalId
                + "&subdir=" + subDir
                + "&image_format_version=" + imageFormatVersion;
        MetaHelper.downloadImageFile(url, MetaService.DOWNLOAD_TIMEOUT_SECOND * 1000, String.valueOf(journalId), dir);

        // clean the old images
        MetaCleaner cleaner = new MetaCleaner(imageDir);
        cleaner.clean();
    }

    private String selectWorker() {
        List<Frontend> workers;
        if (Config.checkpoint_only_on_leader) {
            workers = new ArrayList<>();
        } else {
            workers = GlobalStateMgr.getServingState().getNodeMgr().getOtherFrontends();
            // sort workers by heap used percent asc
            workers.sort((fe1, fe2) -> {
                if (Math.abs(fe1.getHeapUsedPercent() - fe2.getHeapUsedPercent()) < 1e-6) {
                    return 0;
                } else if (fe1.getHeapUsedPercent() > fe2.getHeapUsedPercent()) {
                    return 1;
                } else {
                    return -1;
                }
            });
        }

        // put the leader node to the end
        workers.add(GlobalStateMgr.getServingState().getNodeMgr().getMySelf());

        for (Frontend frontend : workers) {
            LOG.info("frontend: {} heap used percent: {}", frontend.getNodeName(), frontend.getHeapUsedPercent());
        }

        for (Frontend frontend : workers) {
            if (frontend.isAlive() && doCheckpoint(frontend)) {
                LOG.info("select worker: {} to do checkpoint", frontend.getNodeName());
                return frontend.getNodeName();
            }
        }

        return null;
    }

    private boolean doCheckpoint(Frontend frontend) {
        String selfName = GlobalStateMgr.getServingState().getNodeMgr().getNodeName();
        long epoch = GlobalStateMgr.getCurrentState().getEpoch();
        if (selfName.equals(frontend.getNodeName())) {
            CheckpointWorker worker = getCheckpointWorker();
            try {
                worker.setNextCheckpoint(epoch, journalId);
                return true;
            } catch (CheckpointException e) {
                LOG.warn("set next checkpoint failed", e);
                return false;
            }
        } else {
            try {
                // call doCheckpoint rpc
                TStartCheckpointRequest request = new TStartCheckpointRequest();
                request.setEpoch(epoch);
                request.setJournal_id(journalId);
                request.setIs_global_state_mgr(belongToGlobalStateMgr);
                TStartCheckpointResponse response = ThriftRPCRequestExecutor.call(
                        ThriftConnectionPool.frontendPool,
                        new TNetworkAddress(frontend.getHost(), frontend.getRpcPort()),
                        Config.thrift_rpc_timeout_ms,
                        client -> client.startCheckpoint(request));
                TStatus status = response.getStatus();
                if (status.getStatus_code() != TStatusCode.OK) {
                    String errMessage = "";
                    if (status.getError_msgs() != null && !status.getError_msgs().isEmpty()) {
                        errMessage = String.join(",", status.getError_msgs());
                    }
                    LOG.warn("call doCheckpoint failed for node: {}, error message: {}",
                            frontend.getNodeName(), errMessage);
                    return false;
                } else {
                    return true;
                }
            } catch (TException e) {
                LOG.warn("call doCheckpoint failed for node: {}", frontend.getNodeName(), e);
                return false;
            }
        }
    }

    private CheckpointWorker getCheckpointWorker() {
        if (belongToGlobalStateMgr) {
            return GlobalStateMgr.getCurrentState().getCheckpointWorker();
        } else {
            return StarMgrServer.getCurrentState().getCheckpointWorker();
        }
    }

    private void deleteOldJournals(long imageVersion) {
        // To ensure that all nodes will not lose data,
        // deleteVersion should be the minimum value of imageVersion and replayedJournalId.
        long minReplayedJournalId = getMinReplayedJournalId();
        long deleteVersion = Math.min(imageVersion, minReplayedJournalId);
        journal.deleteJournals(deleteVersion + 1);
        LOG.info("journals <= {} with prefix [{}] are deleted. image version {}, other nodes min version {}",
                deleteVersion, journal.getPrefix(), imageVersion, minReplayedJournalId);

    }

    private void pushImage(long imageVersion) {
        Iterator<String> iterator = nodesToPushImage.iterator();
        int needToPushCnt = nodesToPushImage.size();
        int successPushedCnt = 0;
        while (iterator.hasNext()) {
            String nodeName = iterator.next();

            Frontend frontend = GlobalStateMgr.getServingState().getNodeMgr().getFeByName(nodeName);
            if (frontend == null) {
                iterator.remove();
                continue;
            }

            boolean allFormatSuccess = true;
            for (ImageFormatVersion formatVersion : getImageFormatVersionToPush(imageVersion)) {
                String url = "http://" + NetUtils.getHostPortInAccessibleFormat(frontend.getHost(), Config.http_port)
                        + "/put?version=" + imageVersion
                        + "&port=" + Config.http_port
                        + "&subdir=" + subDir
                        + "&for_global_state=" + belongToGlobalStateMgr
                        + "&image_format_version=" + formatVersion.toString();
                try {
                    MetaHelper.httpGet(url, PUT_TIMEOUT_SECOND * 1000);

                    LOG.info("push image successfully, url = {}", url);
                    if (MetricRepo.hasInit) {
                        MetricRepo.COUNTER_IMAGE_PUSH.increase(1L);
                    }
                } catch (IOException e) {
                    allFormatSuccess = false;
                    LOG.error("Exception when pushing image file. url = {}", url, e);
                }
            }
            if (allFormatSuccess) {
                iterator.remove();
                successPushedCnt++;
            }
        }

        LOG.info("push image.{} from subdir [{}] to other nodes. totally {} nodes, push succeeded {} nodes",
                imageVersion, subDir, needToPushCnt, successPushedCnt);
    }

    private List<ImageFormatVersion> getImageFormatVersionToPush(long imageVersion) {
        List<ImageFormatVersion> result = new ArrayList<>();
        if (belongToGlobalStateMgr) {
            // for global state mgr, the creation of v1 format image may fail, so check the existence of image file
            if (Files.exists(Path.of(imageDir + "/image." + imageVersion))) {
                result.add(ImageFormatVersion.v1);
            }
            result.add(ImageFormatVersion.v2);
        } else {
            // for staros mgr, there is only v1 format image.
            result.add(ImageFormatVersion.v1);
        }
        return result;
    }

    private long getMinReplayedJournalId() {
        long minReplayedJournalId = Long.MAX_VALUE;
        for (Frontend fe : GlobalStateMgr.getServingState().getNodeMgr().getOtherFrontends()) {
            String host = fe.getHost();
            int port = Config.http_port;
            URL idURL;
            HttpURLConnection conn = null;
            try {
                /*
                 * get current replayed journal id of each non-master nodes.
                 * when we delete bdb database, we cannot delete db newer than
                 * any non-master node's current replayed journal id. otherwise,
                 * this lagging node can never get the deleted journal.
                 */
                idURL = new URL("http://" + NetUtils.getHostPortInAccessibleFormat(host, port) + "/journal_id?prefix=" + journal.getPrefix());
                conn = (HttpURLConnection) idURL.openConnection();
                conn.setConnectTimeout(CONNECT_TIMEOUT_SECOND * 1000);
                conn.setReadTimeout(READ_TIMEOUT_SECOND * 1000);
                String idString = conn.getHeaderField("id");
                long id = Long.parseLong(idString);
                if (minReplayedJournalId > id) {
                    minReplayedJournalId = id;
                }
            } catch (IOException e) {
                LOG.error("Exception when getting current replayed journal id. host={}, port={}",
                        host, port, e);
                minReplayedJournalId = 0;
                break;
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
        }

        return minReplayedJournalId;
    }

    public void finishCheckpoint(long journalId, String nodeName) throws CheckpointException {
        if (!nodeName.equals(workerNodeName)) {
            throw new CheckpointException(String.format("worker node name node match, current worker is: %s, param worker is: %s",
                    workerNodeName, nodeName));
        }
        if (journalId != this.journalId) {
            throw new CheckpointException(String.format("journalId not match, current journalId is: %d, param journalId is: %d",
                    this.journalId, journalId));
        }

        if (result.offer(Pair.create(true, ""))) {
            LOG.info("finish checkpoint successfully, journalId: {}, nodeName: {}", journalId, nodeName);
        } else {
            LOG.warn("There are already other values in the result queue");
        }
    }

    public void cancelCheckpoint(String nodeName, String reason) {
        if (nodeName.equals(workerNodeName)) {
            result.offer(Pair.create(false, reason));
            LOG.warn("cancel checkpoint on node: {}, because: {}", nodeName, reason);
        }
    }

    public void workerRestarted(String nodeName, long startTime) {
        if (startTime > workerSelectedTime) {
            cancelCheckpoint(nodeName, "worker restarted");
        }
    }

    public Journal getJournal() {
        return journal;
    }
}
