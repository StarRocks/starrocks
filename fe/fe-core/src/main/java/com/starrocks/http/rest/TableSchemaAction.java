// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/http/rest/TableSchemaAction.java

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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Get table schema for specified cluster.database.table with privilege checking
 */
public class TableSchemaAction extends RestBaseAction {

    public TableSchemaAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        // the extra `/api` path is so disgusting
        controller.registerHandler(HttpMethod.GET, "/api/{" + DB_KEY + "}/{" + TABLE_KEY + "}/_schema",
                new TableSchemaAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        // just allocate 2 slot for top holder map
        Map<String, Object> resultMap = new HashMap<>(2);
        String dbName = request.getSingleParameter(DB_KEY);
        String tableName = request.getSingleParameter(TABLE_KEY);
        try {
            if (Strings.isNullOrEmpty(dbName)
                    || Strings.isNullOrEmpty(tableName)) {
                throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "No database or table selected.");
            }
            // check privilege for select, otherwise return 401 HTTP status
            checkTblAuth(ConnectContext.get().getCurrentUserIdentity(), dbName, tableName, PrivPredicate.SELECT);
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (db == null) {
                throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                        "Database [" + dbName + "] " + "does not exists");
            }
            db.readLock();
            try {
                Table table = db.getTable(tableName);
                if (table == null) {
                    throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                            "Table [" + tableName + "] " + "does not exists");
                }
                // just only support OlapTable, ignore others such as ESTable
                if (!(table instanceof OlapTable)) {
                    // Forbidden
                    throw new StarRocksHttpException(HttpResponseStatus.FORBIDDEN, "Table [" + tableName + "] "
                            + "is not a OlapTable, only support OlapTable currently");
                }
                try {
                    List<Column> columns = table.getBaseSchema();
                    List<Map<String, String>> propList = new ArrayList(columns.size());
                    for (Column column : columns) {
                        Map<String, String> baseInfo = new HashMap<>(2);
                        Type colType = column.getType();
                        PrimitiveType primitiveType = colType.getPrimitiveType();
                        if (colType.isDecimalOfAnyVersion()) {
                            ScalarType scalarType = (ScalarType) colType;
                            baseInfo.put("precision", scalarType.getPrecision() + "");
                            baseInfo.put("scale", scalarType.getScalarScale() + "");
                        }
                        baseInfo.put("type", primitiveType.toString());
                        baseInfo.put("comment", column.getComment());
                        baseInfo.put("name", column.getName());
                        propList.add(baseInfo);
                    }
                    resultMap.put("status", 200);
                    resultMap.put("properties", propList);
                } catch (Exception e) {
                    // Transform the general Exception to custom StarRocksHttpException
                    throw new StarRocksHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                            e.getMessage() == null ? "Null Pointer Exception" : e.getMessage());
                }
            } finally {
                db.readUnlock();
            }
        } catch (StarRocksHttpException e) {
            // status code  should conforms to HTTP semantic
            resultMap.put("status", e.getCode().code());
            resultMap.put("exception", e.getMessage());
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            String result = mapper.writeValueAsString(resultMap);
            // send result with extra information
            response.setContentType("application/json");
            response.getContent().append(result);
            sendResult(request, response,
                    HttpResponseStatus.valueOf(Integer.parseInt(String.valueOf(resultMap.get("status")))));
        } catch (Exception e) {
            // may be this never happen
            response.getContent().append(e.getMessage());
            sendResult(request, response, HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
