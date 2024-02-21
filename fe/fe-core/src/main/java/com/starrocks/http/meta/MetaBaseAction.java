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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/meta/MetaBaseAction.java

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

package com.starrocks.http.meta;

import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.action.WebBaseAction;
import com.starrocks.leader.MetaHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Frontend;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;

public class MetaBaseAction extends WebBaseAction {
    private static final Logger LOG = LogManager.getLogger(MetaBaseAction.class);
    private static String CONTENT_DISPOSITION = "Content-disposition";

    public static final String CLUSTER_ID = "cluster_id";
    public static final String TOKEN = "token";
    public static final String RUN_MODE = "run_mode";

    protected File imageDir;

    public MetaBaseAction(ActionController controller, File imageDir) {
        super(controller);
        this.imageDir = imageDir;
    }

    @Override
    public boolean needAdmin() {
        return false;
    }

    @Override
    public boolean needPassword() {
        return false;
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        if (needCheckClientIsFe()) {
            try {
                checkFromValidFe(request, response);
            } catch (InvalidClientException e) {
                response.appendContent("invalid client host.");
                writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                return;
            }
        }
        super.execute(request, response);
    }

    protected boolean needCheckClientIsFe() {
        return true;
    }

    protected void writeFileResponse(BaseRequest request, BaseResponse response, File file) {
        if (file == null || !file.exists()) {
            response.appendContent("File does not exist.");
            writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
            return;
        }

        // add custom header
        response.updateHeader(CONTENT_DISPOSITION, "attachment; filename=" + file.getName());
        response.updateHeader(MetaHelper.X_IMAGE_SIZE, String.valueOf(file.length()));

        writeObjectResponse(request, response, HttpResponseStatus.OK, file, file.getName(), true);
        return;
    }

    private boolean isFromValidFe(BaseRequest request) {
        String clientHost = request.getHostString();
        Frontend fe = GlobalStateMgr.getCurrentState().getNodeMgr().getFeByHost(clientHost);
        if (fe == null) {
            LOG.warn("request is not from valid FE. client: {}", clientHost);
            return false;
        }
        return true;
    }

    private void checkFromValidFe(BaseRequest request, BaseResponse response)
            throws InvalidClientException {
        if (!isFromValidFe(request)) {
            throw new InvalidClientException("invalid client host");
        }
    }
}
