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
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.proto.ExecuteCommandRequestPB;
import com.starrocks.proto.ExecuteCommandResultPB;
import com.starrocks.rpc.BackendServiceClient;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ExecuteScriptStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.type.StringType;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExecuteScriptExecutor {
    private static final Logger LOG = LogManager.getLogger(ExecuteScriptExecutor.class);

    static ShowResultSet makeResultSet(String result) {
        ShowResultSetMetaData meta =
                ShowResultSetMetaData.builder().addColumn(new Column("result", StringType.STRING)).build();
        List<List<String>> rowset = Lists.newArrayList();
        String[] lines = result.split("\n");
        for (String line : lines) {
            rowset.add(Lists.newArrayList(line));
        }
        return new ShowResultSet(meta, rowset);
    }

    static ShowResultSet makeMultiNodeResultSet(List<List<String>> rows) {
        ShowResultSetMetaData meta = ShowResultSetMetaData.builder()
                .addColumn(new Column("node_id", StringType.STRING))
                .addColumn(new Column("status", StringType.STRING))
                .addColumn(new Column("result", StringType.STRING))
                .build();
        return new ShowResultSet(meta, rows);
    }

    public static ShowResultSet execute(ExecuteScriptStmt stmt, ConnectContext ctx) throws StarRocksException {
        if (stmt.isFrontendScript()) {
            return executeFrontendScript(stmt, ctx);
        } else {
            return executeBackendScript(stmt, ctx);
        }
    }

    private static ShowResultSet executeFrontendScript(ExecuteScriptStmt stmt, ConnectContext ctx)
            throws StarRocksException {
        if (!Config.enable_execute_script_on_frontend) {
            throw new StarRocksException("execute script on frontend is disabled");
        }
        try {
            StringBuilder sb = new StringBuilder();
            Binding binding = new Binding();
            binding.setVariable("LOG", LOG);
            binding.setVariable("out", sb);
            binding.setVariable("globalState", GlobalStateMgr.getCurrentState());
            binding.setVariable("metastore", GlobalStateMgr.getCurrentState().getLocalMetastore());
            GroovyShell shell = new GroovyShell(binding);
            shell.evaluate(stmt.getScript());
            ctx.getState().setOk();
            return makeResultSet(sb.toString());
        } catch (Exception e) {
            throw new StarRocksException("execute script failed: " + e.getMessage());
        }
    }

    private static List<Long> resolveTargetNodes(ExecuteScriptStmt stmt) throws StarRocksException {
        SystemInfoService cluster = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<Long> ids;
        switch (stmt.getTargetType()) {
            case ALL_BACKENDS:
                ids = cluster.getBackendIds(false);
                break;
            case ALL_COMPUTE_NODES:
                ids = cluster.getComputeNodeIds(false);
                break;
            case NODES:
            default:
                ids = stmt.getNodeIds();
                break;
        }
        if (ids == null || ids.isEmpty()) {
            throw new StarRocksException("no target nodes resolved for ADMIN EXECUTE");
        }
        // Stable order helps deterministic output.
        List<Long> sorted = new ArrayList<>(ids);
        Collections.sort(sorted);
        return sorted;
    }

    private static ShowResultSet executeBackendScript(ExecuteScriptStmt stmt, ConnectContext ctx) throws
            StarRocksException {
        List<Long> nodeIds = resolveTargetNodes(stmt);

        // Single-target: preserve the legacy single-column "result" output and fail-fast behavior.
        if (nodeIds.size() == 1) {
            return executeOnSingleNode(stmt, ctx, nodeIds.get(0));
        }

        return executeOnMultipleNodes(stmt, ctx, nodeIds);
    }

    private static ShowResultSet executeOnSingleNode(ExecuteScriptStmt stmt, ConnectContext ctx, long nodeId)
            throws StarRocksException {
        SystemInfoService cluster = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        ComputeNode be = cluster.getBackendOrComputeNode(nodeId);
        if (be == null) {
            throw new StarRocksException("node not found: " + nodeId);
        }
        TNetworkAddress address = new TNetworkAddress(be.getHost(), be.getBrpcPort());
        ExecuteCommandRequestPB request = new ExecuteCommandRequestPB();
        request.command = "execute_script";
        request.params = stmt.getScript();
        try {
            Future<ExecuteCommandResultPB> future = BackendServiceClient.getInstance().executeCommand(address, request);
            ExecuteCommandResultPB result = future.get(stmt.getTimeoutSec(), TimeUnit.SECONDS);
            if (result.status.statusCode != 0) {
                LOG.warn("execute script error BE: {} script:{} result: {}", nodeId,
                        StringUtils.abbreviate(stmt.getScript(), 1000),
                        result.status.errorMsgs);
                throw new StarRocksException(result.status.toString());
            } else {
                LOG.info("execute script ok BE: {} script:{} result: {}", nodeId,
                        StringUtils.abbreviate(stmt.getScript(), 1000), StringUtils.abbreviate(result.result, 1000));
                ctx.getState().setOk();
                return makeResultSet(result.result);
            }
        } catch (InterruptedException ie) {
            LOG.warn("got interrupted exception when sending proxy request to " + address);
            Thread.currentThread().interrupt();
            throw new StarRocksException("got interrupted exception when sending proxy request to " + address);
        } catch (Exception e) {
            throw new StarRocksException("executeCommand RPC failed BE:" + address + " err " + e.getMessage());
        }
    }

    // Fan out to all target nodes in parallel; aggregate one row per node with status + result.
    // A failure on one node is reported as an error row, not a hard abort, so operators can see
    // which nodes succeeded.
    private static ShowResultSet executeOnMultipleNodes(ExecuteScriptStmt stmt, ConnectContext ctx, List<Long> nodeIds) {
        SystemInfoService cluster = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        int n = nodeIds.size();
        List<Future<ExecuteCommandResultPB>> futures = new ArrayList<>(n);
        List<TNetworkAddress> addresses = new ArrayList<>(n);

        // Fire all RPCs first. brpc returns a Future per call so this is naturally concurrent
        // without an extra thread pool.
        for (long id : nodeIds) {
            ComputeNode node = cluster.getBackendOrComputeNode(id);
            if (node == null) {
                futures.add(null);
                addresses.add(null);
                continue;
            }
            TNetworkAddress address = new TNetworkAddress(node.getHost(), node.getBrpcPort());
            ExecuteCommandRequestPB request = new ExecuteCommandRequestPB();
            request.command = "execute_script";
            request.params = stmt.getScript();
            try {
                futures.add(BackendServiceClient.getInstance().executeCommand(address, request));
                addresses.add(address);
            } catch (Exception e) {
                LOG.warn("submit execute_command to {} failed: {}", address, e.getMessage());
                futures.add(null);
                addresses.add(address);
            }
        }

        long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(stmt.getTimeoutSec());
        List<List<String>> rows = new ArrayList<>(n);
        int okCount = 0;
        int failCount = 0;
        for (int i = 0; i < n; i++) {
            long nodeId = nodeIds.get(i);
            String idStr = Long.toString(nodeId);
            Future<ExecuteCommandResultPB> future = futures.get(i);
            TNetworkAddress address = addresses.get(i);

            if (future == null) {
                rows.add(Lists.newArrayList(idStr, "ERROR",
                        address == null ? "node not found" : "submit failed for " + address));
                failCount++;
                continue;
            }

            long waitMs = Math.max(0, deadlineMs - System.currentTimeMillis());
            try {
                ExecuteCommandResultPB result = future.get(waitMs, TimeUnit.MILLISECONDS);
                if (result.status.statusCode != 0) {
                    LOG.warn("execute script error node:{} script:{} result:{}", nodeId,
                            StringUtils.abbreviate(stmt.getScript(), 1000), result.status.errorMsgs);
                    rows.add(Lists.newArrayList(idStr, "ERROR", String.valueOf(result.status)));
                    failCount++;
                } else {
                    LOG.info("execute script ok node:{} script:{} result:{}", nodeId,
                            StringUtils.abbreviate(stmt.getScript(), 1000),
                            StringUtils.abbreviate(result.result, 1000));
                    rows.add(Lists.newArrayList(idStr, "OK", result.result == null ? "" : result.result));
                    okCount++;
                }
            } catch (TimeoutException te) {
                rows.add(Lists.newArrayList(idStr, "ERROR", "timeout after " + stmt.getTimeoutSec() + "s"));
                failCount++;
                future.cancel(true);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                rows.add(Lists.newArrayList(idStr, "ERROR", "interrupted"));
                failCount++;
            } catch (Exception e) {
                rows.add(Lists.newArrayList(idStr, "ERROR",
                        "rpc failed " + (address == null ? "" : address.toString()) + ": " + e.getMessage()));
                failCount++;
            }
        }

        LOG.info("ADMIN EXECUTE fan-out done: ok={} fail={} total={}", okCount, failCount, n);
        ctx.getState().setOk();
        return makeMultiNodeResultSet(rows);
    }
}
