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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/ProcService.java

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

package com.starrocks.common.proc;

import com.google.common.base.Strings;
import com.starrocks.cloudnative.warehouse.WarehouseProcDir;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// proc service entry
public final class ProcService {
    private static final Logger LOG = LogManager.getLogger(ProcService.class);
    private static ProcService INSTANCE;

    private BaseProcDir root;

    private ProcService() {
        root = new BaseProcDir();
        root.register("backends", new BackendsProcDir(GlobalStateMgr.getCurrentSystemInfo()));
        root.register("compute_nodes", new ComputeNodeProcDir(GlobalStateMgr.getCurrentSystemInfo()));
        root.register("dbs", new DbsProcDir(GlobalStateMgr.getCurrentState()));
        root.register("jobs", new JobsDbProcDir(GlobalStateMgr.getCurrentState()));
        root.register("statistic", new StatisticProcDir(GlobalStateMgr.getCurrentState()));
        root.register("tasks", new TasksProcDir());
        root.register("frontends", new FrontendsProcNode(GlobalStateMgr.getCurrentState()));
        root.register("brokers", GlobalStateMgr.getCurrentState().getBrokerMgr().getProcNode());
        root.register("resources", GlobalStateMgr.getCurrentState().getResourceMgr().getProcNode());
        root.register("load_error_hub", new LoadErrorHubProcNode(GlobalStateMgr.getCurrentState()));
        root.register("transactions", new TransDbProcDir());
        root.register("monitor", new MonitorProcDir());
        root.register("current_queries", new CurrentQueryStatisticsProcDir());
        root.register("current_backend_instances", new CurrentQueryBackendInstanceProcDir());
        root.register("cluster_balance", new ClusterBalanceProcDir());
        root.register("routine_loads", new RoutineLoadsProcDir());
        root.register("stream_loads", new StreamLoadsProcDir());
        root.register("colocation_group", new ColocationGroupProcDir());
        root.register("catalog", GlobalStateMgr.getCurrentState().getCatalogMgr().getProcNode());
        root.register("compactions", new CompactionsProcNode());
        root.register("warehouses", new WarehouseProcDir(GlobalStateMgr.getCurrentWarehouseMgr()));
    }

    // Get the corresponding PROC Node by the specified path
    // Currently, "..." is not supported , "." These wildcard characters are currently not
    // supported, but can handle cases like '//'
    // For space: the previous space field can be filtered out, and the space field will be
    // terminated when it is encountered later
    public ProcNodeInterface open(String path) throws AnalysisException {
        // input is invalid
        if (Strings.isNullOrEmpty(path)) {
            throw new AnalysisException("Path is null");
        }

        int last = 0;
        int pos = 0;
        int len = path.length();
        boolean meetRoot = false;
        boolean meetEnd = false;
        ProcNodeInterface curNode = null;

        while (pos < len && !meetEnd) {
            char ch = path.charAt(pos);
            switch (ch) {
                case '/':
                    if (!meetRoot) {
                        curNode = root;
                        meetRoot = true;
                    } else {
                        String name = path.substring(last, pos);
                        if (!(curNode instanceof ProcDirInterface)) {
                            String errMsg = path.substring(0, pos) + " is not a directory";
                            LOG.warn(errMsg);
                            throw new AnalysisException(errMsg);
                        }
                        curNode = ((ProcDirInterface) curNode).lookup(name);
                    }
                    // swallow next "/"
                    while (pos < len && path.charAt(pos) == '/') {
                        pos++;
                    }
                    // now assign last from pos(new start of file name)
                    last = pos;
                    break;
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    if (meetRoot) {
                        meetEnd = true;
                    } else {
                        last++;
                        pos++;
                    }
                    break;
                default:
                    if (!meetRoot) {
                        // starts without '/'
                        String errMsg = "Path(" + path + ") does not start with '/'";
                        LOG.warn(errMsg);
                        throw new AnalysisException(errMsg);
                    }
                    pos++;
                    break;
            }
        }
        // the last character of path is '/', the current is must a directory
        if (pos == last) {
            // now pos == path.length()
            if (curNode == null || !(curNode instanceof ProcDirInterface)) {
                String errMsg = path + " is not a directory";
                LOG.warn(errMsg);
                throw new AnalysisException(errMsg);
            }
            return curNode;
        }

        if (!(curNode instanceof ProcDirInterface)) {
            String errMsg = path.substring(0, pos) + " is not a directory";
            LOG.warn(errMsg);
            throw new AnalysisException(errMsg);
        }

        // Pos is used here because there is a possibility that the space field after path will be truncated early
        curNode = ((ProcDirInterface) curNode).lookup(path.substring(last, pos));
        if (curNode == null) {
            throw new AnalysisException("Cannot find path: " + path);
        }
        return curNode;
    }

    // Register node to name under root node
    public synchronized boolean register(String name, ProcNodeInterface node) {
        if (Strings.isNullOrEmpty(name) || node == null) {
            LOG.warn("register porc service invalid input.");
            return false;
        }
        if (root.lookup(name) != null) {
            LOG.warn("node(" + name + ") already exists.");
            return false;
        }
        return root.register(name, node);
    }

    public static ProcService getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ProcService();
        }
        return INSTANCE;
    }

    // Used to empty the registered content during testing
    public static void destroy() {
        INSTANCE = null;
    }

}
