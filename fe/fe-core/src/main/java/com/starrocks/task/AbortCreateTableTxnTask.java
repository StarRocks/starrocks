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

package com.starrocks.task;

import com.starrocks.common.ClientPool;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.BackendService;
import com.starrocks.thrift.TAbortTxnReq;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TReq;
import com.starrocks.thrift.TTaskType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AbortCreateTableTxnTask implements Runnable {
    private static final Logger LOG = LogManager.getLogger(AbortCreateTableTxnTask.class);

    private long txnId;

    private long backendId;

    public AbortCreateTableTxnTask(long txnId, long backendId) {
        this.txnId = txnId;
        this.backendId = backendId;
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

            TAbortTxnReq abortTxnReq = new TAbortTxnReq();
            abortTxnReq.setTxn_id(txnId);

            TReq req = new TReq();
            req.setTxn_id(txnId);
            req.setTask_type(TTaskType.CLEAR_TRANSACTION_TASK);
            req.setAbort_req(abortTxnReq);
            client.submit_req(req);
            LOG.info("Req type: {}, backend: {}, txnId: {}", req.getTask_type(), address, req.getTxn_id());
            ok = true;
        } catch (Exception e) {
            LOG.warn("task exec error. backend[{}]", address, e);
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
