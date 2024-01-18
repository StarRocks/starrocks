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

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.clone.DynamicPartitionScheduler;
import com.starrocks.common.concurrent.locks.LockType;
import com.starrocks.common.concurrent.locks.Locker;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

/*
 *  get log file infos:
 *      curl -I http://fe_host:http_port/api/trigger?type=dynamic_partition&db=db_name&tbl=tbl_name
 *      return:
 *          HTTP/1.1 200 OK
 *          Success
 *          content-type: text/html
 *          connection: keep-alive
 */
public class TriggerAction extends RestBaseAction {

    private static final String DB_PARAM = "db";
    private static final String TABLE_PARAM = "tbl";
    private static final String SUPPORTED_TRIGGER_TYPE = "dynamic_partition";

    public TriggerAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/trigger", new TriggerAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) {
        String triggerType = request.getSingleParameter("type");

        if (Strings.isNullOrEmpty(triggerType)) {
            response.appendContent("Miss type parameter");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        // check type valid or not
        if (!triggerType.equalsIgnoreCase(SUPPORTED_TRIGGER_TYPE)) {
            response.appendContent("trigger type: " + triggerType + " is invalid!" +
                    "only support " + SUPPORTED_TRIGGER_TYPE);
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        String dbName = request.getSingleParameter(DB_PARAM);
        String tableName = request.getSingleParameter(TABLE_PARAM);

        if (Strings.isNullOrEmpty(dbName)) {
            response.appendContent("Missing params. Need database name");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            response.appendContent("Database[" + dbName + "] does not exist");
            writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }


        HttpMethod method = request.getRequest().method();
        if (method.equals(HttpMethod.GET)) {
            DynamicPartitionScheduler dynamicPartitionScheduler = globalStateMgr.getDynamicPartitionScheduler();
            Table table = null;
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                if (!Strings.isNullOrEmpty(tableName)) {
                    table = db.getTable(tableName);
                }

                if (table == null) {
                    response.appendContent("Table[" + tableName + "] does not exist");
                    writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                    return;
                }
            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
            dynamicPartitionScheduler.executeDynamicPartitionForTable(db.getId(), table.getId());
            response.appendContent("Success");
            writeResponse(request, response, HttpResponseStatus.OK);
        } else {
            response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
            writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
        }
    }
}
