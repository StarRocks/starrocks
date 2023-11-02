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

package com.starrocks.task;

import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.Status;
import com.starrocks.common.util.concurrent.MarkedCountDownLatch;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.TCreateTableReq;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReq;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class CreateTableTask implements Runnable {
    private static final Logger LOG = LogManager.getLogger(CreateReplicaTask.class);

    private List<CreateReplicaTask> tasks = new ArrayList<>();

    private long backendId;

    private long txnId;

    private MarkedCountDownLatch<Long, Long> latch;

    public void addReplicaTask(CreateReplicaTask task) {
        tasks.add(task);
    }

    public void setBackendId(long backendId) {
        this.backendId = backendId;
    }

    public long getBackendId() {
        return backendId;
    }

    public void setTxnId(long txnId) {
        this.txnId = txnId;
    }

    public long getTxnId() {
        return txnId;
    }

    public List<CreateReplicaTask> getTasks() {
        return tasks;
    }

    public void countDownLatch(long backendId, long txnId) {
        if (this.latch != null) {
            if (latch.markedCountDown(backendId, txnId)) {
                LOG.info("CreateReplicaTask current latch count: {}, backend: {}, tablet:{}",
                        latch.getCount(), backendId, txnId);
            }
        }
        LOG.info("count: {}", this.latch.getCount());
    }

    // call this always means one of tasks is failed. count down to zero to finish entire task
    public void countDownToZero(String errMsg) {
        if (this.latch != null) {
            latch.countDownToZero(new Status(TStatusCode.CANCELLED, errMsg));
            LOG.info("CreateReplicaTask download to zero. error msg: {}", errMsg);
        }
    }

    public void setLatch(MarkedCountDownLatch<Long, Long> latch) {
        this.latch = latch;
    }

    @Override
    public void run() {
        BackendService.Client client = null;
        TNetworkAddress address = null;
        boolean ok = false;
        try {
            ComputeNode computeNode = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendId);
            if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA && computeNode == null) {
                computeNode = GlobalStateMgr.getCurrentSystemInfo().getComputeNode(backendId);
            }

            if (computeNode == null || !computeNode.isAlive()) {
                return;
            }

            address = new TNetworkAddress(computeNode.getHost(), computeNode.getBePort());
            client = ClientPool.backendPool.borrowObject(address);

            TCreateTableReq createTableReq = new TCreateTableReq();
            createTableReq.setTimeout(Config.create_table_timeout_second);
            for (CreateReplicaTask task : tasks) {
                createTableReq.addToCreate_tablet_reqs(task.toThrift());
                LOG.info("tablet_id: {}", task.getTabletId());
            }

            TReq req = new TReq();
            req.setTxn_id(txnId);
            req.setTask_type(TTaskType.CREATE);
            req.setCreate_table_req(createTableReq);
            client.submit_req(req);
            LOG.info("Req type: {}, backend: {}, txnId: {}", req.getTask_type(), backendId, req.getTxn_id());
            ok = true;
        } catch (Exception e) {
            LOG.warn("task exec error. backend[{}]", backendId, e);
        } finally {
            if (ok) {
                ClientPool.backendPool.returnObject(address, client);
            } else {
                // TODO: notify tasks rpc failed in trace
                ClientPool.backendPool.invalidateObject(address, client);
            }
        }
    }
}
