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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/StorageTypeCheckAction.java

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
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TStorageType;
import io.netty.handler.codec.http.HttpMethod;
import org.json.JSONObject;

import java.util.List;
import java.util.Map;

public class StorageTypeCheckAction extends RestBaseAction {
    public StorageTypeCheckAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        StorageTypeCheckAction action = new StorageTypeCheckAction(controller);
        controller.registerHandler(HttpMethod.GET, "/api/_check_storagetype", action);
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
        checkUserOwnsAdminRole(currentUser);

        String dbName = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(dbName)) {
            throw new DdlException("Parameter db is missing");
        }

        Database db = globalStateMgr.getDb(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist");
        }

        JSONObject root = new JSONObject();
        db.readLock();
        try {
            List<Table> tbls = db.getTables();
            for (Table tbl : tbls) {
                if (tbl.getType() != TableType.OLAP) {
                    continue;
                }

                OlapTable olapTbl = (OlapTable) tbl;
                JSONObject indexObj = new JSONObject();
                for (Map.Entry<Long, MaterializedIndexMeta> entry : olapTbl.getIndexIdToMeta().entrySet()) {
                    MaterializedIndexMeta indexMeta = entry.getValue();
                    if (indexMeta.getStorageType() == TStorageType.ROW) {
                        indexObj.put(olapTbl.getIndexNameById(entry.getKey()), indexMeta.getStorageType().name());
                    }
                }
                root.put(tbl.getName(), indexObj);
            }
        } finally {
            db.readUnlock();
        }

        // to json response
        String result = root.toString();

        // send result
        response.setContentType("application/json");
        response.getContent().append(result);
        sendResult(request, response);
    }
}
