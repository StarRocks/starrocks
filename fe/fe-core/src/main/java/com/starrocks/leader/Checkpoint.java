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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.NetUtils;
import com.starrocks.journal.Journal;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageFormatVersion;
import com.starrocks.persist.MetaCleaner;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.staros.StarMgrServer;
import com.starrocks.system.Frontend;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

/**
 * Checkpoint daemon is running on master node. handle the checkpoint work for starrocks.
 */
public class Checkpoint extends FrontendDaemon {
    public static final Logger LOG = LogManager.getLogger(Checkpoint.class);
    private static final int PUT_TIMEOUT_SECOND = 3600;
    private static final int CONNECT_TIMEOUT_SECOND = 1;
    private static final int READ_TIMEOUT_SECOND = 1;

    private GlobalStateMgr globalStateMgr;
    private final String imageDir;
    private final Journal journal;
    // subDir comes after base imageDir, to distinguish different module's image dir
    private final String subDir;
    private final boolean belongToGlobalStateMgr;

    private final Set<String> nodesToPushImage;

    public Checkpoint(Journal journal) {
        this("leaderCheckpointer", journal, "" /* subDir */, true /* belongToGlobalStateMgr */);
    }

    public Checkpoint(String name, Journal journal, String subDir, boolean belongToGlobalStateMgr) {
        super(name, FeConstants.checkpoint_interval_second * 1000L);
        this.imageDir = GlobalStateMgr.getServingState().getImageDir() + subDir;
        this.journal = journal;
        this.subDir = subDir;
        this.belongToGlobalStateMgr = belongToGlobalStateMgr;
        nodesToPushImage = new HashSet<>();
    }

    @Override
    protected void runAfterCatalogReady() {
        long imageVersion = 0;
        long logVersion = 0;
        try {
            Storage storage = new Storage(imageDir);
            // get max image version
            imageVersion = storage.getImageJournalId();
            // get max finalized journal id
            logVersion = journal.getFinalizedJournalId();
            LOG.info("checkpoint imageVersion {}, logVersion {}", imageVersion, logVersion);
        } catch (IOException e) {
            LOG.error("Failed to get storage info", e);
            return;
        }

        // Step 1: create image
        boolean newImageCreated = false;
        if (imageVersion < logVersion) {
            newImageCreated = createImage(logVersion);
        }
        if (newImageCreated) {
            // Push the image file to all other nodes
            // NOTE: Do not get other nodes from HaProtocol, because the node may not be in bdbje replication group yet.
            for (Frontend frontend : GlobalStateMgr.getServingState().getNodeMgr().getOtherFrontends()) {
                nodesToPushImage.add(frontend.getNodeName());
            }
        }

        // Step2: push image
        int needToPushCnt = nodesToPushImage.size();
        long newImageVersion = newImageCreated ? logVersion : imageVersion;
        if (needToPushCnt > 0) {
            pushImage(newImageVersion);
        }

        // Step3: Delete old journals
        // conditions: 1. new image created and no others node to push, this means there is only one FE in the cluster,
        //                delete the old journals immediately.
        //             2. needToPushCnt > 0 means there are other nodes in the cluster,
        //                we must make sure all the other nodes have got the new image and then delete old journals.
        if ((newImageCreated && needToPushCnt == 0)
                || (needToPushCnt > 0 && nodesToPushImage.size() == 0)) {
            deleteOldJournals(newImageVersion);
        }

        // Step4: Delete old image files from local storage.
        if (newImageCreated) {
            List<String> dirsToClean = Lists.newArrayList(imageDir);
            if (belongToGlobalStateMgr) {
                dirsToClean.add(imageDir + "/v2");
            }
            for (String dirToClean : dirsToClean) {
                MetaCleaner cleaner = new MetaCleaner(dirToClean);
                try {
                    cleaner.clean();
                } catch (IOException e) {
                    LOG.error("Leader delete old image file from dir {} fail.", dirsToClean, e);
                }
            }
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
                    MetaHelper.getRemoteFile(url, PUT_TIMEOUT_SECOND * 1000, new NullOutputStream());
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

    private boolean createImage(long logVersion) {
        if (belongToGlobalStateMgr) {
            return replayAndGenerateGlobalStateMgrImage(logVersion);
        } else {
            return replayAndGenerateStarMgrImage(logVersion);
        }
    }

    private boolean replayAndGenerateGlobalStateMgrImage(long logVersion) {
        assert belongToGlobalStateMgr;
        long replayedJournalId = -1;
        // generate new image file
        LOG.info("begin to generate new image: image.{}", logVersion);
        globalStateMgr = GlobalStateMgr.getCurrentState();
        globalStateMgr.setEditLog(new EditLog(null));
        globalStateMgr.setJournal(journal);
        try {
            globalStateMgr.loadImage(imageDir);
            globalStateMgr.initDefaultWarehouse();
            globalStateMgr.replayJournal(logVersion);
            globalStateMgr.clearExpiredJobs();
            globalStateMgr.saveImage();
            replayedJournalId = globalStateMgr.getReplayedJournalId();
            if (MetricRepo.hasInit) {
                MetricRepo.COUNTER_IMAGE_WRITE.increase(1L);
            }
            GlobalStateMgr.getServingState().setImageJournalId(logVersion);
            LOG.info("checkpoint finished save image.{}", replayedJournalId);
            return true;
        } catch (Exception e) {
            LOG.error("Exception when generate new image file", e);
            return false;
        } finally {
            // destroy checkpoint globalStateMgr, reclaim memory
            globalStateMgr = null;
            GlobalStateMgr.destroyCheckpoint();
        }
    }

    private boolean replayAndGenerateStarMgrImage(long logVersion) {
        assert !belongToGlobalStateMgr;
        StarMgrServer starMgrServer = StarMgrServer.getCurrentState();
        try {
            return starMgrServer.replayAndGenerateImage(imageDir, logVersion);
        } catch (Exception e) {
            LOG.error("Exception when generate new star mgr image file", e);
            return false;
        } finally {
            // destroy checkpoint, reclaim memory
            StarMgrServer.destroyCheckpoint();
        }
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
}
