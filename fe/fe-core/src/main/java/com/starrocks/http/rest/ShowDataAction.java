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
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class ShowDataAction extends RestBaseAction {

    public ShowDataAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/show_data", new ShowDataAction(controller));
    }

    public long getDataSizeOfDatabase(Database db) {
        Locker locker = new Locker();
        locker.lockDatabase(db, LockType.READ);
        try {
            return db.getTables().stream()
                    .filter(Table::isNativeTableOrMaterializedView)
                    .mapToLong(table -> ((OlapTable) table).getDataSize())
                    .sum();
        } finally {
            locker.unLockDatabase(db, LockType.READ);
        }
    }

    @Override
    public void execute(BaseRequest request, BaseResponse response) {
        String dbName = request.getSingleParameter("db");
        ConcurrentHashMap<String, Database> fullNameToDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getFullNameToDb();
        String tableName = request.getSingleParameter("table");
        long totalSize;
        if (dbName != null) {
            Database db = fullNameToDb.get(dbName);
            if (db == null) {
                response.getContent().append("database ").append(dbName).append(" not found.");
                sendResult(request, response, HttpResponseStatus.NOT_FOUND);
                return;
            }
            if (tableName == null) {
                totalSize = getDataSizeOfDatabase(db);
            } else {
                Optional<Table> tableOptional = db.getTables().stream()
                        .filter(t -> t.getName().equals(tableName))
                        .findAny();
                if (tableOptional.isEmpty()) {
                    response.getContent().append("table ").append(tableName).append(" not found.");
                    sendResult(request, response, HttpResponseStatus.NOT_FOUND);
                    return;
                } else {
                    totalSize = getDataSizeOfTable(tableOptional.get());
                }
            }
        } else {
            if (tableName == null) {
                totalSize = getDataSizeOfDatabases(fullNameToDb.values());
            } else {
                totalSize = fullNameToDb.values()
                        .parallelStream()
                        .map(Database::getTables)
                        .flatMap(Collection::stream)
                        .filter(Table::isNativeTableOrMaterializedView)
                        .filter(t -> t.getName().equals(tableName))
                        .mapToLong(this::getDataSizeOfTable)
                        .sum();
            }
        }
        response.getContent().append(totalSize);
        sendResult(request, response);
    }

    private long getDataSizeOfTable(Table table) {
        if (!table.isNativeTableOrMaterializedView()) {
            return 0;
        }
        return ((OlapTable) table).getDataSize();
    }

    private long getDataSizeOfDatabases(Collection<Database> dbs) {
        return dbs.parallelStream()
                .mapToLong(this::getDataSizeOfDatabase)
                .sum();
    }
}
