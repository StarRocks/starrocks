// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/LoadAction.java

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

package com.starrocks.http.rest;

<<<<<<< HEAD
import com.google.common.base.Strings;
import com.starrocks.cluster.ClusterNamespace;
=======
>>>>>>> 7edc3b808 ([Refactor] Refactor transaction stream load interface (#8344))
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TransactionLoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(TransactionLoadAction.class);
    private static final String TXN_OP_KEY = "txn_op";
    private static final String TXN_BEGIN = "begin";

    private Map<String, Long> txnBackendMap = new LinkedHashMap<String, Long>(512, 0.75f, true) {
        protected boolean removeEldestEntry(Map.Entry<String, Long> eldest) {
            return size() > GlobalStateMgr.getCurrentSystemInfo().backendSize() * 512;
        }
    };

    public TransactionLoadAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        TransactionLoadAction ac = new TransactionLoadAction(controller);
        controller.registerHandler(HttpMethod.POST, "/api/transaction/{" + TXN_OP_KEY + "}", ac);
        controller.registerHandler(HttpMethod.PUT,
                "/api/transaction/{" + TXN_OP_KEY + "}", ac);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
<<<<<<< HEAD

        final String clusterName = ConnectContext.get().getClusterName();
        if (Strings.isNullOrEmpty(clusterName)) {
            throw new DdlException("No cluster selected.");
        }

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String fullDbName = ClusterNamespace.getFullName(clusterName, dbName);

=======
        String dbName = request.getRequest().headers().get(DB_KEY);
        String tableName = request.getRequest().headers().get(TABLE_KEY);
>>>>>>> 7edc3b808 ([Refactor] Refactor transaction stream load interface (#8344))
        String label = request.getRequest().headers().get(LABEL_KEY);
        String op = request.getSingleParameter(TXN_OP_KEY);
        Long backendID = null;

        synchronized (this) {
            if (op.equalsIgnoreCase(TXN_BEGIN)) {
                // Choose a backend sequentially.
                List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().seqChooseBackendIds(1, true, false, clusterName);
                if (backendIds == null) {
                    throw new DdlException("No backend alive.");
                }
                backendID = backendIds.get(0);
                txnBackendMap.put(label, backendID);
            } else {
                backendID = txnBackendMap.get(label);
            }
        }

        if (backendID == null) {
            throw new DdlException("transaction with op " + op + " label " + label + " has no backend");
        }

        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendID);
        if (backend == null) {
            throw new DdlException("Backend " + backendID + " is not alive");
        }

        TNetworkAddress redirectAddr = new TNetworkAddress(backend.getHost(), backend.getHttpPort());

        LOG.info("redirect transaction action to destination={}, db: {}, table: {}, op: {}, label: {}",
                redirectAddr, dbName, tableName, op, label);
        redirectTo(request, response, redirectAddr);
    }
}

