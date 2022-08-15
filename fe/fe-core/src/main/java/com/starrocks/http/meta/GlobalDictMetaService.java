// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.http.meta;

import com.google.common.base.Strings;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
                throws DdlException {
            if (redirectToLeader(request, response)) {
                return;
            }
            checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
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
                if (Strings.isNullOrEmpty(dbName) || Strings.isNullOrEmpty(tableName)) {
                    response.appendContent("Miss db_name parameter or table_name");
                    writeResponse(request, response, HttpResponseStatus.BAD_REQUEST);
                    return;
                }

                boolean isEnable = "true".equalsIgnoreCase(request.getSingleParameter(ENABLE).trim());

                GlobalStateMgr.getCurrentState()
                        .setHasForbitGlobalDict(dbName, tableName, isEnable);
                response.appendContent(new RestBaseResult("apply success").toJson());
            } else {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
            }
            writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
            sendResult(request, response);
        }
    }
}