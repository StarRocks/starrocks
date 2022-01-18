// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.http.meta;

import com.google.common.base.Strings;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.rest.RestBaseAction;
import com.starrocks.http.rest.RestBaseResult;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *  eg:
 *       POST    /api/global_dict/table/forbit?db_name=default_cluster:test&table_name=test_basic&enable=0
 *               (mark forbit test_basic use global dict)
 *       POST    /api/global_dict/table/forbit?db_name=default_cluster:test&table_name=test_basic&enable=1
 *               (mark enable test_basic use global dict)
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
            if (redirectToMaster(request, response)) {
                return;
            }
            checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);
            executeInMasterWithAdmin(request, response);
        }

        // implement in derived classes
        protected void executeInMasterWithAdmin(BaseRequest request, BaseResponse response)
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
            controller.registerHandler(HttpMethod.POST, "/api/global_dict/table/forbit", action);
        }

        @Override
        public void executeInMasterWithAdmin(BaseRequest request, BaseResponse response)
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

                long isEnable =  Long.valueOf(request.getSingleParameter(ENABLE).trim());

                Catalog.getCurrentCatalog().setHasForbitGlobalDict(dbName, tableName, isEnable == 0);
            } else {
                response.appendContent(new RestBaseResult("HTTP method is not allowed.").toJson());
                writeResponse(request, response, HttpResponseStatus.METHOD_NOT_ALLOWED);
            }
            sendResult(request, response);
        }
    }
}