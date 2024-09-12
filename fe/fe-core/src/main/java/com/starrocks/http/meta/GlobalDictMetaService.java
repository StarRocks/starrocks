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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.persist.ModifyTablePropertyOperationLog;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * eg:
 * POST    /api/global_dict/table/enable?db_name=test&table_name=test_basic&enable=false
 * (mark disable test_basic use global dict)
 * POST    /api/global_dict/table/enable?db_name=test&table_name=test_basic&enable=true
 * (mark enable test_basic use global dict)
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
                setHasForbiddenGlobalDict(dbName, tableName, isEnable);
                response.appendContent(new RestBaseResult("apply success").toJson());
            } else {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
                writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
                return;
            }
            sendResult(request, response);
        }

        public void setHasForbiddenGlobalDict(String dbName, String tableName, boolean isForbit) throws DdlException {
            Map<String, String> property = new HashMap<>();
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
            if (db == null) {
                throw new DdlException("the DB " + dbName + " is not exist");
            }
            Locker locker = new Locker();
            locker.lockDatabase(db.getId(), LockType.READ);
            try {
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), tableName);
                if (table == null) {
                    throw new DdlException("the DB " + dbName + " table: " + tableName + "isn't  exist");
                }

                if (table instanceof OlapTable) {
                    OlapTable olapTable = (OlapTable) table;
                    olapTable.setHasForbiddenGlobalDict(isForbit);
                    if (isForbit) {
                        property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.DISABLE_LOW_CARD_DICT);
                        IDictManager.getInstance().disableGlobalDict(olapTable.getId());
                    } else {
                        property.put(PropertyAnalyzer.ENABLE_LOW_CARD_DICT_TYPE, PropertyAnalyzer.ABLE_LOW_CARD_DICT);
                        IDictManager.getInstance().enableGlobalDict(olapTable.getId());
                    }
                    ModifyTablePropertyOperationLog info =
                            new ModifyTablePropertyOperationLog(db.getId(), table.getId(), property);
                    GlobalStateMgr.getCurrentState().getEditLog().logSetHasForbiddenGlobalDict(info);
                }
            } finally {
                locker.unLockDatabase(db.getId(), LockType.READ);
            }
        }
    }
}
