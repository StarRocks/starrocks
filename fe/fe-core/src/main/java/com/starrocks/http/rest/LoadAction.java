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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/LoadAction.java

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
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.thrift.TNetworkAddress;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class LoadAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(LoadAction.class);

    public LoadAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.PUT,
                "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_stream_load",
                new LoadAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {

        // A 'Load' request must have 100-continue header
        if (!request.getRequest().headers().contains(HttpHeaders.Names.EXPECT)) {
            throw new DdlException("There is no 100-continue header");
        }

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String tableName = request.getSingleParameter(TABLE_KEY);
        if (Strings.isNullOrEmpty(tableName)) {
            throw new DdlException("No table selected.");
        }

        String label = request.getRequest().headers().get(LABEL_KEY);

        // check auth
        if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
            checkTableAction(ConnectContext.get(), dbName, tableName, PrivilegeType.INSERT);
        } else {
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), dbName, tableName, PrivPredicate.LOAD);
        }

        // Choose a backend sequentially.
        List<Long> backendIds = GlobalStateMgr.getCurrentSystemInfo().seqChooseBackendIds(1, true, false);
        if (CollectionUtils.isEmpty(backendIds)) {
            throw new DdlException("No backend alive.");
        }

        Backend backend = GlobalStateMgr.getCurrentSystemInfo().getBackend(backendIds.get(0));
        if (backend == null) {
            throw new DdlException("No backend alive.");
        }

        TNetworkAddress redirectAddr = new TNetworkAddress(backend.getHost(), backend.getHttpPort());

        LOG.info("redirect load action to destination={}, db: {}, tbl: {}, label: {}",
                redirectAddr.toString(), dbName, tableName, label);
        redirectTo(request, response, redirectAddr);
    }
}

