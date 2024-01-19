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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/ShowDataAction.java

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

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.meta.lock.LockType;
import com.starrocks.meta.lock.Locker;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class ShowDataAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ShowDataAction.class);

    public ShowDataAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/show_data", new ShowDataAction(controller));
    }

    public long getDataSizeOfDatabase(Database db) {
        long totalSize = 0;
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            // sort by table name
            List<Table> tables = db.getTables();
            for (Table table : tables) {
                if (!table.isNativeTableOrMaterializedView()) {
                    continue;
                }
                long tableSize = ((OlapTable) table).getDataSize();
                totalSize += tableSize;
            } // end for tables
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
        return totalSize;
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String dbName = request.getSingleParameter("db");
        ConcurrentHashMap<String, Database> fullNameToDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getFullNameToDb();
        long totalSize = 0;
        if (dbName != null) {
            Database db = fullNameToDb.get(dbName);
            if (db == null) {
                response.getContent().append("database " + dbName + " not found.");
                sendResult(request, response, HttpResponseStatus.NOT_FOUND);
                return;
            }
            totalSize = getDataSizeOfDatabase(db);
        } else {
            for (Database db : fullNameToDb.values()) {
                LOG.info("database name: {}", db.getOriginName());
                totalSize += getDataSizeOfDatabase(db);
            }
        }
        response.getContent().append(String.valueOf(totalSize));
        sendResult(request, response);
    }
}
