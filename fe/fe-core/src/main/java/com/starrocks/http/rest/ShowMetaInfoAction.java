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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/ShowMetaInfoAction.java

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.ha.HAProtocol;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowMetaInfoAction extends RestBaseAction {
    private enum Action {
        SHOW_DB_SIZE,
        // show db size with all replicas included
        SHOW_FULL_DB_SIZE,
        SHOW_HA,
        INVALID;

        public static Action getAction(String str) {
            try {
                return valueOf(str);
            } catch (Exception ex) {
                return INVALID;
            }
        }
    }

    private static final Logger LOG = LogManager.getLogger(ShowMetaInfoAction.class);

    public ShowMetaInfoAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/show_meta_info",
                new ShowMetaInfoAction(controller));
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String action = request.getSingleParameter("action");
        // check param empty
        if (Strings.isNullOrEmpty(action)) {
            response.appendContent("Missing parameter");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }
        Gson gson = new Gson();
        response.setContentType("application/json");

        switch (Action.getAction(action.toUpperCase())) {
            case SHOW_DB_SIZE:
                response.getContent().append(gson.toJson(getDataSize(true)));
                break;
            case SHOW_FULL_DB_SIZE:
                response.getContent().append(gson.toJson(getDataSize(false)));
                break;
            case SHOW_HA:
                response.getContent().append(gson.toJson(getHaInfo()));
                break;
            default:
                // clear content type set above
                response.setContentType(null);
                response.appendContent("Invalid parameter");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
        }
        sendResult(request, response);
    }

    public Map<String, String> getHaInfo() {
        HashMap<String, String> feInfo = new HashMap<String, String>();
        feInfo.put("role", GlobalStateMgr.getCurrentState().getFeType().toString());
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            feInfo.put("current_journal_id",
                    String.valueOf(GlobalStateMgr.getCurrentState().getMaxJournalId()));
        } else {
            feInfo.put("current_journal_id",
                    String.valueOf(GlobalStateMgr.getCurrentState().getReplayedJournalId()));
        }

        HAProtocol haProtocol = GlobalStateMgr.getCurrentState().getHaProtocol();
        if (haProtocol != null) {

            InetSocketAddress master = null;
            try {
                master = haProtocol.getLeader();
            } catch (Exception e) {
                // this may happen when the majority of FOLLOWERS are down and no MASTER right now.
                LOG.warn("failed to get leader: {}", e.getMessage());
            }
            if (master != null) {
                feInfo.put("master", master.getHostString());
            } else {
                feInfo.put("master", "unknown");
            }

            List<InetSocketAddress> electableNodes = haProtocol.getElectableNodes(false);
            ArrayList<String> electableNodeNames = new ArrayList<String>();
            if (!electableNodes.isEmpty()) {
                for (InetSocketAddress node : electableNodes) {
                    electableNodeNames.add(node.getHostString());
                }
                feInfo.put("electable_nodes", StringUtils.join(electableNodeNames.toArray(), ","));
            }

            List<InetSocketAddress> observerNodes = haProtocol.getObserverNodes();
            ArrayList<String> observerNodeNames = new ArrayList<String>();
            if (observerNodes != null) {
                for (InetSocketAddress node : observerNodes) {
                    observerNodeNames.add(node.getHostString());
                }
                feInfo.put("observer_nodes", StringUtils.join(observerNodeNames.toArray(), ","));
            }
        }

        feInfo.put("can_read", String.valueOf(GlobalStateMgr.getCurrentState().canRead()));
        feInfo.put("is_ready", String.valueOf(GlobalStateMgr.getCurrentState().isReady()));
        try {
            Storage storage = new Storage(Config.meta_dir + "/image");
            feInfo.put("last_checkpoint_version", String.valueOf(storage.getImageJournalId()));
            long lastCheckpointTime = storage.getCurrentImageFile().lastModified();
            feInfo.put("last_checkpoint_time", String.valueOf(lastCheckpointTime));
        } catch (IOException e) {
            LOG.warn(e.getMessage(), e);
        }
        return feInfo;
    }

    public Map<String, Long> getDataSize(boolean singleReplica) {
        Map<String, Long> result = new HashMap<String, Long>();

        for (Map.Entry<String, Database> dbs : GlobalStateMgr.getCurrentState().getLocalMetastore().getFullNameToDb()
                .entrySet()) {
            String dbName = dbs.getKey();
            Database db = dbs.getValue();

            long totalSize = 0;
            List<Table> tables = GlobalStateMgr.getCurrentState().getLocalMetastore().getTables(db.getId());
            for (int j = 0; j < tables.size(); j++) {
                Table table = tables.get(j);
                if (table.isNativeTableOrMaterializedView()) {
                    // in implementation, cloud native table is a subtype of olap table
                    totalSize += calculateSizeForOlapTable((OlapTable) table, singleReplica);
                }
            }
            result.put(dbName, totalSize);
        } // end for dbs
        return result;
    }

    @VisibleForTesting
    public long calculateSizeForOlapTable(OlapTable olapTable, boolean singleReplica) {
        long tableSize = 0;
        for (PhysicalPartition partition : olapTable.getAllPhysicalPartitions()) {
            long partitionSize = 0;
            for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                partitionSize += mIndex.getDataSize(singleReplica);
            } // end for indexes
            tableSize += partitionSize;
        } // end for tables
        return tableSize;
    }
}
