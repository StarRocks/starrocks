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

package com.starrocks.qe;

import com.starrocks.common.UserException;
import com.starrocks.proto.ExecuteCommandRequestPB;
import com.starrocks.proto.ExecuteCommandResultPB;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExecuteScriptExecutor {
    private static final Logger LOG = LogManager.getLogger(ExecuteScriptExecutor.class);

    public static void execute(ExecuteScriptStmt stmt, ConnectContext ctx) throws UserException {
        Backend be = GlobalStateMgr.getCurrentSystemInfo().getBackend(stmt.getBeId());
        TNetworkAddress address = new TNetworkAddress(be.getHost(), be.getBrpcPort());
        ExecuteCommandRequestPB request = new ExecuteCommandRequestPB();
        request.command = "execute_script";
        request.params = stmt.getScript();
        try {
            Future<ExecuteCommandResultPB> future = BackendServiceClient.getInstance().executeCommand(address, request);
            ExecuteCommandResultPB result = future.get(stmt.getTimeoutSec(), TimeUnit.SECONDS);
            if (result.status.statusCode != 0) {
                LOG.warn("execute script error BE: {} script:{} result: {}", stmt.getBeId(),
                        StringUtils.abbreviate(stmt.getScript(), 1000),
                        result.status.errorMsgs);
                throw new UserException(result.status.toString());
            } else {
                LOG.info("execute script ok BE: {} script:{} result: {}", stmt.getBeId(),
                        StringUtils.abbreviate(stmt.getScript(), 1000), StringUtils.abbreviate(result.result, 1000));
                ctx.getState().setOk(0, 0, result.result);
            }
        } catch (InterruptedException ie) {
            LOG.warn("got interrupted exception when sending proxy request to " + address);
            Thread.currentThread().interrupt();
            throw new UserException("got interrupted exception when sending proxy request to " + address);
        } catch (Exception e) {
            throw new UserException("executeCommand RPC failed BE:" + address + " err " + e.getMessage());
        }
    }
}
