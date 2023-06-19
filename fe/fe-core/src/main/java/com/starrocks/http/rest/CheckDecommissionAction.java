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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/CheckDecommissionAction.java

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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.alter.SystemHandler;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.system.SystemInfoService;
import io.netty.handler.codec.http.HttpMethod;

import java.util.List;

/*
 * calc row count from replica to table
 * fe_host:fe_http_port/api/check_decommission?host_ports=host:port,host2:port2...
 * return:
 * {"status":"OK","msg":"Success"}
 * {"status":"FAILED","msg":"err info..."}
 */
public class CheckDecommissionAction extends RestBaseAction {
    public static final String HOST_PORTS = "host_ports";

    public CheckDecommissionAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/check_decommission", new CheckDecommissionAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response)
            throws DdlException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
            checkActionOnSystem(currentUser, PrivilegeType.OPERATE);
        } else {
            checkGlobalAuth(currentUser, PrivPredicate.OPERATOR);
        }

        String hostPorts = request.getSingleParameter(HOST_PORTS);
        if (Strings.isNullOrEmpty(hostPorts)) {
            throw new DdlException("No host:port specified.");
        }

        String[] hostPortArr = hostPorts.split(",");
        if (hostPortArr.length == 0) {
            throw new DdlException("No host:port specified.");
        }

        List<Pair<String, Integer>> hostPortPairs = Lists.newArrayList();
        for (String hostPort : hostPortArr) {
            Pair<String, Integer> pair;
            try {
                pair = SystemInfoService.validateHostAndPort(hostPort, false);
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
            hostPortPairs.add(pair);
        }

        SystemHandler.checkDecommission(hostPortPairs);

        // to json response
        RestBaseResult result = new RestBaseResult();

        // send result
        response.setContentType("application/json");
        response.getContent().append(result.toJson());
        sendResult(request, response);
    }
}
