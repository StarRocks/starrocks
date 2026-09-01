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


package com.starrocks.http.meta;

import com.google.common.base.Strings;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * eg:
 * POST    /api/global_dict/table/enable?db_name=test&table_name=test_basic&enable=false
 * (mark disable test_basic use global dict)
 * POST    /api/global_dict/table/enable?db_name=test&table_name=test_basic&enable=true
 * (mark enable test_basic use global dict)
 * GET     /api/global_dict/table/no_dict_columns?db_name=test&table_name=test_basic
 * (list the columns whose low-cardinality global dict collection is forbidden on test_basic)
 */

public class GlobalDictMetaService {
    private static final String DB_NAME = "db_name";
    private static final String TABLE_NAME = "table_name";
    private static final String ENABLE = "enable";
    private static final Logger LOG = LogManager.getLogger(GlobalDictMetaService.class);

    public static class GlobalDictMetaServiceBaseAction extends RestBaseAction {
        GlobalDictMetaServiceBaseAction(ActionController controller) {
            super(controller);
        }

        @Override
        public void executeWithoutPassword(BaseRequest request, BaseResponse response)
                throws DdlException, AccessDeniedException {
            if (redirectToLeader(request, response)) {
                return;
            }
            UserIdentity currentUser = ConnectContext.get().getCurrentUserIdentity();
            checkUserOwnsAdminRole(currentUser);
            executeInLeaderWithAdmin(request, response);
        }

        // implement in derived classes
        protected void executeInLeaderWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            throw new DdlException("Not implemented");
        }
    }

    public static class ForbitTableAction extends GlobalDictMetaServiceBaseAction {
        ForbitTableAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            ForbitTableAction action = new ForbitTableAction(controller);
            controller.registerHandler(HttpMethod.POST, "/api/global_dict/table/enable", action);
        }

        @Override
        public void executeInLeaderWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            HttpMethod method = request.getRequest().method();
            if (method.equals(HttpMethod.POST)) {
                String tableName = request.getSingleParameter(TABLE_NAME);
                String dbName = request.getSingleParameter(DB_NAME);
                String enableParam = request.getSingleParameter(ENABLE);
                if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName) || Strings.isNullOrEmpty(enableParam)) {
                    response.appendContent("Missing db_name, table_name, or enable parameter");
                    writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                    return;
                }
                if (!enableParam.trim().equalsIgnoreCase("true") && !enableParam.trim().equalsIgnoreCase("false")) {
                    response.appendContent("Invalid enable parameter. It should be either 'true' or 'false'");
                    writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                    return;
                }
                boolean isEnable = Boolean.parseBoolean(enableParam.trim());
                GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .setHasForbiddenGlobalDict(dbName, tableName, isEnable);
                response.appendContent(new RestBaseResult("apply success").toJson());
            } else {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
                writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
                return;
            }
            sendResult(request, response);
        }
    }

    /**
     * List the columns whose low-cardinality global dict collection is forbidden. With db_name and
     * table_name it returns that table's list; without them it returns every table that has any
     * forbidden column. The list is the persisted per-column forbid set (auto-populated by the
     * dictionary thrash guard, or set manually).
     */
    public static class ListNoDictColumnsAction extends GlobalDictMetaServiceBaseAction {
        ListNoDictColumnsAction(ActionController controller) {
            super(controller);
        }

        public static void registerAction(ActionController controller) throws IllegalArgException {
            ListNoDictColumnsAction action = new ListNoDictColumnsAction(controller);
            controller.registerHandler(HttpMethod.GET, "/api/global_dict/table/no_dict_columns", action);
        }

        @Override
        public void executeInLeaderWithAdmin(BaseRequest request, BaseResponse response)
                throws DdlException {
            HttpMethod method = request.getRequest().method();
            if (!method.equals(HttpMethod.GET)) {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
                writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
                return;
            }

            String dbName = request.getSingleParameter(DB_NAME);
            String tableName = request.getSingleParameter(TABLE_NAME);
            GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
            List<Map<String, Object>> result = new ArrayList<>();

            if (!Strings.isNullOrEmpty(dbName) && !Strings.isNullOrEmpty(tableName)) {
                Database db = globalStateMgr.getLocalMetastore().getDb(dbName);
                if (db == null) {
                    response.appendContent("db " + dbName + " not found");
                    writeResponse(request, response, HttpResponseStatus.NOT_FOUND);
                    return;
                }
                Table table = globalStateMgr.getLocalMetastore().getTable(dbName, tableName);
                if (table instanceof OlapTable olapTable && !olapTable.getNoDictColumns().isEmpty()) {
                    result.add(entry(dbName, tableName, olapTable.getNoDictColumns()));
                }
            } else {
                // global scan: enumerate all olap tables that have at least one forbidden column
                for (Long dbId : globalStateMgr.getLocalMetastore().getDbIds()) {
                    Database db = globalStateMgr.getLocalMetastore().getDb(dbId);
                    if (db == null) {
                        continue;
                    }
                    for (Table table : db.getTables()) {
                        if (table instanceof OlapTable olapTable && !olapTable.getNoDictColumns().isEmpty()) {
                            result.add(entry(db.getFullName(), table.getName(), olapTable.getNoDictColumns()));
                        }
                    }
                }
            }

            response.appendContent(GsonUtils.GSON.toJson(result));
            sendResult(request, response);
        }

        private static Map<String, Object> entry(String dbName, String tableName, java.util.Set<String> columns) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("db_name", dbName);
            m.put("table_name", tableName);
            m.put("no_dict_columns", new ArrayList<>(new TreeSet<>(columns)));
            return m;
        }
    }
}
