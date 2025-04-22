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

package com.starrocks.journal;

import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.leader.CheckpointController;
import com.starrocks.leader.MetaHelper;
import com.starrocks.persist.MetaCleaner;
import com.starrocks.persist.Storage;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TFinishCheckpointRequest;
import com.starrocks.thrift.TFinishCheckpointResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

public abstract class CheckpointWorker extends FrontendDaemon {
    public static final Logger LOG = LogManager.getLogger(CheckpointWorker.class);

    protected final Journal journal;

    // the next checkpoint task(epoch, journalId) to do
    private final AtomicReference<NextPoint> nextPoint = new AtomicReference<>();
    protected GlobalStateMgr servingGlobalState;

    public CheckpointWorker(String name, Journal journal) {
        super(name, FeConstants.checkpoint_interval_second * 1000L);
        this.journal = journal;
    }

    abstract void doCheckpoint(long epoch, long journalId) throws Exception;
    abstract CheckpointController getCheckpointController();
    abstract boolean isBelongToGlobalStateMgr();

    public void setNextCheckpoint(long epoch, long journalId) throws CheckpointException {
        if (servingGlobalState == null) {
            throw new CheckpointException("worker not initialize");
        }
        if (epoch != servingGlobalState.getEpoch()) {
            throw new CheckpointException(String.format("epoch: %d is not equal to current epoch: %d",
                    epoch, servingGlobalState.getEpoch()));
        }
        if (journalId > journal.getMaxJournalId()) {
            throw new CheckpointException(String.format("can not find journal id: %d , current max journal id is: %d",
                    journalId, journal.getMaxJournalId()));
        }

        nextPoint.set(new NextPoint(epoch, journalId));
        LOG.info("set next point to epoch:{}, journalId:{}", epoch, journalId);
    }

    @Override
    protected void runAfterCatalogReady() {
        init();

        NextPoint np = nextPoint.getAndSet(null);
        if (np == null) {
            return;
        }

        createImage(np.epoch, np.journalId);
    }

    private void init() {
        this.servingGlobalState = GlobalStateMgr.getServingState();
    }

    private void createImage(long epoch, long journalId) {
        if (!preCheckParamValid(epoch, journalId)) {
            return;
        }

        try {
            doCheckpoint(epoch, journalId);
        } catch (Exception e) {
            LOG.warn("create image failed", e);
            finishCheckpoint(epoch, journalId, false, e.getMessage());
            return;
        }

        cleanOldImages();

        finishCheckpoint(epoch, journalId, true, "success");
    }

    private boolean preCheckParamValid(long epoch, long journalId) {
        if (journalId < getImageJournalId()) {
            finishCheckpoint(epoch, journalId, false, "journalId is too small");
            return false;
        }
        if (journalId == getImageJournalId()) {
            finishCheckpoint(epoch, journalId, true, "success");
            return false;
        }
        if (epoch != servingGlobalState.getEpoch()) {
            finishCheckpoint(epoch, journalId, false, "epoch outdated");
            return false;
        }
        return true;
    }

    private void cleanOldImages() {
        String dirToClean = MetaHelper.getImageFileDir(isBelongToGlobalStateMgr());
        MetaCleaner cleaner = new MetaCleaner(dirToClean);
        try {
            cleaner.clean();
        } catch (IOException e) {
            LOG.error("Delete old image file from dir {} fail.", dirToClean, e);
        }
    }

    private void finishCheckpoint(long epoch, long journalId, boolean isSuccess, String message) {
        if (epoch != servingGlobalState.getEpoch()) {
            LOG.warn("epoch outdated, do not finish checkpoint");
            return;
        }

        String nodeName = servingGlobalState.getNodeMgr().getNodeName();
        if (servingGlobalState.isLeader()) {
            CheckpointController controller = getCheckpointController();
            if (isSuccess) {
                try {
                    controller.finishCheckpoint(journalId, nodeName);
                } catch (CheckpointException e) {
                    LOG.warn("finish checkpoint failed", e);
                }
            } else {
                controller.cancelCheckpoint(nodeName, message);
            }
        } else {
            TFinishCheckpointRequest request = new TFinishCheckpointRequest();
            request.setJournal_id(journalId);
            request.setNode_name(nodeName);
            request.setIs_success(isSuccess);
            request.setMessage(message);
            request.setIs_global_state_mgr(isBelongToGlobalStateMgr());

            try {
                TFinishCheckpointResponse response = ThriftRPCRequestExecutor.call(
                        ThriftConnectionPool.frontendPool,
                        servingGlobalState.getNodeMgr().getLeaderRpcEndpoint(),
                        Config.thrift_rpc_timeout_ms,
                        client -> client.finishCheckpoint(request));
                TStatus status = response.getStatus();
                if (status.getStatus_code() != TStatusCode.OK) {
                    String errMessage = "";
                    if (status.getError_msgs() != null && !status.getError_msgs().isEmpty()) {
                        errMessage = String.join(",", status.getError_msgs());
                    }
                    LOG.warn("call finishCheckpoint failed, error message: {}",  errMessage);
                }
            } catch (TException e) {
                LOG.warn("call finishCheckpoint failed", e);
            }
        }
    }

    private long getImageJournalId() {
        try {
            Storage storage = new Storage(MetaHelper.getImageFileDir(isBelongToGlobalStateMgr()));
            return storage.getImageJournalId();
        } catch (IOException e) {
            LOG.warn("get image journal id failed", e);
            return 0;
        }
    }

    static class NextPoint {
        private final long epoch;
        private final long journalId;

        public NextPoint(long epoch, long journalId) {
            this.epoch = epoch;
            this.journalId = journalId;
        }
    }
}
