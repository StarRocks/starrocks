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

package com.starrocks.http.rest;

import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.rpc.ThriftRPCRequestExecutor;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This action is used to invalidate thread pool connection.
 *
 * /api/_invalidate_connection?host=some_backend_ip&port=9050&type=heartbeat
 */
public class ThreadPoolOpAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ThreadPoolOpAction.class);
    private static final String HOST_PARAM = "host";
    private static final String PORT_PARAM = "port";
    private static final String POOL_TYPE = "type";

    public ThreadPoolOpAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ThreadPoolOpAction action = new ThreadPoolOpAction(controller);
        controller.registerHandler(HttpMethod.GET, "/api/_invalidate_connection", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException,
            AccessDeniedException {
        LOG.info("received invalidate thread pool connection http request, {}", request.getRequest().uri());
        // check authority
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String host = request.getSingleParameter(HOST_PARAM);
        int port = Integer.parseInt(request.getSingleParameter(PORT_PARAM));
        String type = request.getSingleParameter(POOL_TYPE);
        LOG.info("invalidate client, host: {}, port: {}, type: {}", host, port, type);
        try {
            switch (type) {
                case "backend":
                    ThriftRPCRequestExecutor.invalidateClient(ThriftConnectionPool.backendPool,
                            new TNetworkAddress(host, port), 10000);
                    break;
                case "heartbeat":
                    ThriftRPCRequestExecutor.invalidateClient(ThriftConnectionPool.beHeartbeatPool,
                            new TNetworkAddress(host, port), 10000);
                    break;
                case "broker":
                    ThriftRPCRequestExecutor.invalidateClient(ThriftConnectionPool.brokerPool,
                            new TNetworkAddress(host, port), 10000);
                    break;
                case "frontend":
                    ThriftRPCRequestExecutor.invalidateClient(ThriftConnectionPool.frontendPool,
                            new TNetworkAddress(host, port), 10000);
                    break;
                default:
                    throw new IllegalArgException("unknown pool type: " + type);
            }
            LOG.info("invalidate client succeed, host: {}, port: {}, type: {}", host, port, type);
        } catch (Exception e) {
            LOG.warn("invalidate client error", e);
            throw new RuntimeException(e);
        }
    }
}
