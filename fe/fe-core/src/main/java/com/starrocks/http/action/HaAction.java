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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/action/HaAction.java

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

import com.starrocks.common.Config;
import com.starrocks.ha.HAProtocol;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.persist.Storage;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Date;
import java.util.List;

public class HaAction extends WebBaseAction {

    private static final Logger LOG = LogManager.getLogger(HaAction.class);

    public HaAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/ha", new HaAction(controller));
    }

    @Override
    public void executeGet(BaseRequest request, BaseResponse response) {
        getPageHeader(request, response.getContent());

        appendRoleInfo(response.getContent());
        appendJournalInfo(response.getContent());
        appendCanReadInfo(response.getContent());
        appendNodesInfo(response.getContent());
        appendImageInfo(response.getContent());
        appendDbNames(response.getContent());
        appendFe(response.getContent());
        appendRemovedFe(response.getContent());

        getPageFooter(response.getContent());
        writeResponse(request, response);
    }

    private void appendRoleInfo(StringBuilder buffer) {
        buffer.append("<h2>Frontend Role</h2>");
        buffer.append("<pre>");
        buffer.append("<p>" + GlobalStateMgr.getCurrentState().getFeType() + "</p>");
        buffer.append("</pre>");
    }

    private void appendJournalInfo(StringBuilder buffer) {
        buffer.append("<h2>Current Journal Id</h2>");
        buffer.append("<pre>");
        if (GlobalStateMgr.getCurrentState().isLeader()) {
            buffer.append("<p>" + GlobalStateMgr.getCurrentState().getMaxJournalId() + "</p>");
        } else {
            buffer.append("<p>" + GlobalStateMgr.getCurrentState().getReplayedJournalId() + "</p>");
        }
        buffer.append("</pre>");
    }

    private void appendNodesInfo(StringBuilder buffer) {
        HAProtocol haProtocol = GlobalStateMgr.getCurrentState().getHaProtocol();
        if (haProtocol == null) {
            return;
        }
        List<InetSocketAddress> electableNodes = haProtocol.getElectableNodes(true);
        if (electableNodes.isEmpty()) {
            return;
        }
        buffer.append("<h2>Electable nodes</h2>");
        buffer.append("<pre>");
        for (InetSocketAddress node : electableNodes) {
            buffer.append("<p>" + node.getAddress() + "</p>");

        }
        buffer.append("</pre>");

        List<InetSocketAddress> observerNodes = haProtocol.getObserverNodes();
        if (observerNodes == null) {
            return;
        }
        buffer.append("<h2>Observer nodes</h2>");
        buffer.append("<pre>");
        for (InetSocketAddress node : observerNodes) {
            buffer.append("<p>" + node.getHostString() + "</p>");
        }
        buffer.append("</pre>");
    }

    private void appendCanReadInfo(StringBuilder buffer) {
        buffer.append("<h2>Can Read</h2>");
        buffer.append("<pre>");
        buffer.append("<p>" + GlobalStateMgr.getCurrentState().canRead() + "</p>");

        buffer.append("</pre>");
    }

    private void appendImageInfo(StringBuilder buffer) {
        try {
            Storage storage = new Storage(Config.meta_dir + "/image");
            buffer.append("<h2>Checkpoint Info</h2>");
            buffer.append("<pre>");
            buffer.append("<p>last checkpoint version:" + storage.getImageJournalId() + "</p>");
            long lastCheckpointTime = storage.getCurrentImageFile().lastModified();
            Date date = new Date(lastCheckpointTime);
            buffer.append("<p>last checkpoint time: " + date + "</p>");
            buffer.append("</pre>");
        } catch (IOException e) {
            LOG.warn(e);
        }
    }

    private void appendDbNames(StringBuilder buffer) {
        List<Long> names = GlobalStateMgr.getCurrentState().getJournal().getDatabaseNames();
        if (names == null) {
            return;
        }

        String msg = "";
        for (long name : names) {
            msg += name + " ";
        }
        buffer.append("<h2>Database names</h2>");
        buffer.append("<pre>");
        buffer.append("<p>" + msg + "</p>");
        buffer.append("</pre>");
    }

    private void appendFe(StringBuilder buffer) {
        List<Frontend> fes = GlobalStateMgr.getCurrentState().getFrontends(null /* all */);
        if (fes == null) {
            return;
        }

        buffer.append("<h2>Allowed Frontends</h2>");
        buffer.append("<pre>");
        for (Frontend fe : fes) {
            buffer.append("<p>" + fe.toString() + "</p>");
        }
        buffer.append("</pre>");
    }

    private void appendRemovedFe(StringBuilder buffer) {
        List<String> feNames = GlobalStateMgr.getCurrentState().getRemovedFrontendNames();
        buffer.append("<h2>Removed Frontends</h2>");
        buffer.append("<pre>");
        for (String feName : feNames) {
            buffer.append("<p>" + feName + "</p>");
        }
        buffer.append("</pre>");
    }

}