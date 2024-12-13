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

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
<<<<<<< HEAD
import com.starrocks.common.UserException;
=======
import com.starrocks.common.StarRocksException;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.proto.ExecuteCommandRequestPB;
import com.starrocks.proto.ExecuteCommandResultPB;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.thrift.TNetworkAddress;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ExecuteScriptExecutor {
    private static final Logger LOG = LogManager.getLogger(ExecuteScriptExecutor.class);

    static ShowResultSet makeResultSet(String result) {
        ShowResultSetMetaData meta =
                ShowResultSetMetaData.builder().addColumn(new Column("result", Type.STRING)).build();
        List<List<String>> rowset = Lists.newArrayList();
        String[] lines = result.split("\n");
        for (String line : lines) {
            rowset.add(Lists.newArrayList(line));
        }
        return new ShowResultSet(meta, rowset);
    }

<<<<<<< HEAD
    public static ShowResultSet execute(ExecuteScriptStmt stmt, ConnectContext ctx) throws UserException {
=======
    public static ShowResultSet execute(ExecuteScriptStmt stmt, ConnectContext ctx) throws StarRocksException {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        if (stmt.isFrontendScript()) {
            return executeFrontendScript(stmt, ctx);
        } else {
            return executeBackendScript(stmt, ctx);
        }
    }

    private static ShowResultSet executeFrontendScript(ExecuteScriptStmt stmt, ConnectContext ctx)
<<<<<<< HEAD
            throws UserException {
        if (!Config.enable_execute_script_on_frontend) {
            throw new UserException("execute script on frontend is disabled");
=======
            throws StarRocksException {
        if (!Config.enable_execute_script_on_frontend) {
            throw new StarRocksException("execute script on frontend is disabled");
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
        try {
            StringBuilder sb = new StringBuilder();
            Binding binding = new Binding();
            binding.setVariable("LOG", LOG);
            binding.setVariable("out", sb);
            binding.setVariable("globalState", GlobalStateMgr.getCurrentState());
            GroovyShell shell = new GroovyShell(binding);
            shell.evaluate(stmt.getScript());
            ctx.getState().setOk();
            return makeResultSet(sb.toString());
        } catch (Exception e) {
<<<<<<< HEAD
            throw new UserException("execute script failed: " + e.getMessage());
        }
    }

    private static ShowResultSet executeBackendScript(ExecuteScriptStmt stmt, ConnectContext ctx) throws UserException {
        ComputeNode be = GlobalStateMgr.getCurrentSystemInfo().getBackendOrComputeNode(stmt.getBeId());
        if (be == null) {
            throw new UserException("node not found: " + stmt.getBeId());
=======
            throw new StarRocksException("execute script failed: " + e.getMessage());
        }
    }

    private static ShowResultSet executeBackendScript(ExecuteScriptStmt stmt, ConnectContext ctx) throws
            StarRocksException {
        ComputeNode be = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackendOrComputeNode(stmt.getBeId());
        if (be == null) {
            throw new StarRocksException("node not found: " + stmt.getBeId());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
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
<<<<<<< HEAD
                throw new UserException(result.status.toString());
=======
                throw new StarRocksException(result.status.toString());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            } else {
                LOG.info("execute script ok BE: {} script:{} result: {}", stmt.getBeId(),
                        StringUtils.abbreviate(stmt.getScript(), 1000), StringUtils.abbreviate(result.result, 1000));
                ctx.getState().setOk();
                return makeResultSet(result.result);
            }
        } catch (InterruptedException ie) {
            LOG.warn("got interrupted exception when sending proxy request to " + address);
            Thread.currentThread().interrupt();
<<<<<<< HEAD
            throw new UserException("got interrupted exception when sending proxy request to " + address);
        } catch (Exception e) {
            throw new UserException("executeCommand RPC failed BE:" + address + " err " + e.getMessage());
=======
            throw new StarRocksException("got interrupted exception when sending proxy request to " + address);
        } catch (Exception e) {
            throw new StarRocksException("executeCommand RPC failed BE:" + address + " err " + e.getMessage());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        }
    }
}
