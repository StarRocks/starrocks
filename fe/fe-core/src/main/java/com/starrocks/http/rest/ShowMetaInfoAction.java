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

import com.google.gson.Gson;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.ha.HAProtocol;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
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
        Gson gson = new Gson();
        response.setContentType("application/json");

        switch (Action.getAction(action.toUpperCase())) {
            case SHOW_DB_SIZE:
                response.getContent().append(gson.toJson(getDataSize()));
                break;
            case SHOW_HA:
                response.getContent().append(gson.toJson(getHaInfo()));
                break;
            default:
                break;
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
                // this may happen when majority of FOLLOWERS are down and no MASTER right now.
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
            LOG.warn(e.getMessage());
        }
        return feInfo;
    }

    public Map<String, Long> getDataSize() {
        Map<String, Long> result = new HashMap<String, Long>();
        List<String> dbNames = GlobalStateMgr.getCurrentState().getLocalMetastore().listDbNames();

        for (int i = 0; i < dbNames.size(); i++) {
            String dbName = dbNames.get(i);
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);

            long totalSize = 0;
            List<Table> tables = db.getTables();
            for (int j = 0; j < tables.size(); j++) {
                Table table = tables.get(j);
                if (table.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTable = (OlapTable) table;
                long tableSize = 0;
                for (PhysicalPartition partition : olapTable.getAllPhysicalPartitions()) {
                    long partitionSize = 0;
                    for (MaterializedIndex mIndex : partition.getMaterializedIndices(IndexExtState.VISIBLE)) {
                        long indexSize = 0;
                        for (Tablet tablet : mIndex.getTablets()) {
                            indexSize += tablet.getDataSize(true);
                        } // end for tablets
                        partitionSize += indexSize;
                    } // end for tables
                    tableSize += partitionSize;
                } // end for partitions
                totalSize += tableSize;
            } // end for tables
            result.put(dbName, Long.valueOf(totalSize));
        } // end for dbs
        return result;
    }
}
