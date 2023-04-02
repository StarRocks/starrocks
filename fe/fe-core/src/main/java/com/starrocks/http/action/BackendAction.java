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
import com.starrocks.common.util.ListComparator;
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
        ImmutableMap<Long, Backend> backendMap = GlobalStateMgr.getCurrentSystemInfo().getIdToBackend();

        List<List<Comparable>> backendInfos = new ArrayList<List<Comparable>>();
        for (Backend backend : backendMap.values()) {
            List<Comparable> backendInfo = new ArrayList<Comparable>();
            InetAddress address = null;
            try {
                address = InetAddress.getByName(backend.getHost());
            } catch (UnknownHostException e) {
                LOG.warn("unknown host: " + backend.getHost(), e);
                continue;
            }
            backendInfo.add(address.getHostName());
            backendInfo.add(backend.getId());
            backendInfo.add("host: " + backend.getHost()
                    + ", heart_port: " + backend.getHeartbeatPort()
                    + ", be_port: " + backend.getBePort()
                    + ", http_port: " + backend.getHttpPort()
                    + ", brpc_port: " + backend.getBrpcPort()
                    + ", state: " + backend.getBackendState()
                    + ", start_time: " + TimeUtils.longToTimeString(backend.getLastStartTime())
                    + ", last_report_tablets_time: " + backend.getBackendStatus().lastSuccessReportTabletsTime
                    + ", version: " + backend.getVersion());

            backendInfos.add(backendInfo);
        }

        // sort by id
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(1);
        Collections.sort(backendInfos, comparator);

        // set result
        buffer.append("<h2>Known Backends(" + backendMap.size() + ")</h2>");

        buffer.append("<pre>");
        for (List<Comparable> info : backendInfos) {
            buffer.append(info.get(0));
            buffer.append(" [id: " + info.get(1));
            buffer.append(", " + info.get(2) + "]");
            buffer.append(System.getProperty("line.separator"));
        }
        buffer.append("</pre>");
    }

}
