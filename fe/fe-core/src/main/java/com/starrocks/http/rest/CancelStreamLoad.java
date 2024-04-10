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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/CancelStreamLoad.java

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
import com.starrocks.catalog.Database;
import com.starrocks.common.DdlException;
import com.starrocks.common.UserException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;

public class CancelStreamLoad extends RestBaseAction {
    public CancelStreamLoad(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller)
            throws IllegalArgException {
        CancelStreamLoad action = new CancelStreamLoad(controller);
        controller.registerHandler(HttpMethod.POST, "/api/{" + DB_KEY + "}/{" + LABEL_KEY + "}/_cancel", action);
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {

        if (redirectToLeader(request, response)) {
            return;
        }

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("No database selected.");
        }

        String label = request.getSingleParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            throw new DdlException("No label selected.");
        }

        // FIXME(cmy)
        // checkWritePriv(authInfo.fullUserName, fullDbName);

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new DdlException("unknown database, database=" + dbName);
        }

        try {
            GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(db.getId(), label, "user cancel");
        } catch (UserException e) {
            throw new DdlException(e.getMessage());
        }

        sendResult(request, response, new RestBaseResult());
    }
}
