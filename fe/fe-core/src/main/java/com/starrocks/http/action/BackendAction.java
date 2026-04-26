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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/action/BackendAction.java

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

package com.starrocks.http.action;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BackendAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(BackendAction.class);

    private static final ArrayList<String> BACKEND_TABLE_HEADER = Lists.newArrayList();

    static {
        BACKEND_TABLE_HEADER.add("HostName");
        BACKEND_TABLE_HEADER.add("Id");
        BACKEND_TABLE_HEADER.add("Host");
        BACKEND_TABLE_HEADER.add("HeartbeatPort");
        BACKEND_TABLE_HEADER.add("BePort");
        BACKEND_TABLE_HEADER.add("HttpPort");
        BACKEND_TABLE_HEADER.add("BrpcPort");
        BACKEND_TABLE_HEADER.add("State");
        BACKEND_TABLE_HEADER.add("StartTime");
        BACKEND_TABLE_HEADER.add("LastReportTabletsTime");
        BACKEND_TABLE_HEADER.add("Version");
    }

    public BackendAction(ActionController controller) {
        super(controller);
        // TODO Auto-generated constructor stub
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/backend", new BackendAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());

        appendKnownBackendsInfo(response.getContent());

        getPageFooter(response.getContent());
        writeResponse(request, response);
    }

    private void appendKnownBackendsInfo(StringBuilder buffer) {
        ImmutableMap<Long, Backend> backendMap =
                GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getIdToBackend();

        List<List<String>> backendRows = new ArrayList<List<String>>();
        for (Backend backend : backendMap.values()) {
            List<String> row = new ArrayList<String>();
            InetAddress address = null;
            try {
                address = InetAddress.getByName(backend.getHost());
            } catch (UnknownHostException e) {
                LOG.warn("unknown host: " + backend.getHost(), e);
                continue;
            }
            row.add(address.getHostName());
            row.add(String.valueOf(backend.getId()));
            row.add(backend.getHost());
            row.add(String.valueOf(backend.getHeartbeatPort()));
            row.add(String.valueOf(backend.getBePort()));
            row.add(String.valueOf(backend.getHttpPort()));
            row.add(String.valueOf(backend.getBrpcPort()));
            row.add(backend.getBackendState().toString());
            row.add(TimeUtils.longToTimeString(backend.getLastStartTime()));
            row.add(String.valueOf(backend.getBackendStatus().lastSuccessReportTabletsTime));
            row.add(backend.getVersion());

            backendRows.add(row);
        }

        // sort by id
        Collections.sort(backendRows, (a, b) -> {
            try {
                long idA = Long.parseLong(a.get(1));
                long idB = Long.parseLong(b.get(1));
                return Long.compare(idA, idB);
            } catch (NumberFormatException e) {
                return a.get(1).compareTo(b.get(1));
            }
        });

        // set result
        buffer.append("<h2>Known Backends(" + backendMap.size() + ")</h2>");

        appendTableHeader(buffer, BACKEND_TABLE_HEADER);
        appendTableBody(buffer, backendRows);
        appendTableFooter(buffer);
    }

}
