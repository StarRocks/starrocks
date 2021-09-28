// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.starrocks.analysis.StatementBase;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.statistic.StatisticExecutor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/* Usage:
   eg:
        POST  /api/query_dump?db=test  post_data=query
 return:
        {"statement": "...", "table_meta" : {..}, "table_row_count" : {...}, "session_variables" : "...",
         "column_statistics" : {...}}
 */

public class QueryDumpAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(QueryDumpAction.class);
    private static final String DB = "db";

    public QueryDumpAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/query_dump", new QueryDumpAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        ConnectContext context = ConnectContext.get();
        String dbName = request.getSingleParameter(DB);
        if (!Strings.isNullOrEmpty(dbName)) {
            String fullDbName = ClusterNamespace.getFullName(context.getClusterName(), dbName);
            Database db = Catalog.getCurrentCatalog().getDb(fullDbName);
            if (db == null) {
                response.getContent().append("Database [" + fullDbName + "] does not exists");
                sendResult(request, response, HttpResponseStatus.NOT_FOUND);
                return;
            }
            context.setDatabase(fullDbName);
        }
        context.setIsQueryDump(true);

        String query = request.getContent();
        if (Strings.isNullOrEmpty(query)) {
            response.getContent().append("not valid parameter");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        StatementBase parsedStmt;
        try {
            parsedStmt = StatisticExecutor.parseSQL(query, context);
            StmtExecutor executor = new StmtExecutor(context, parsedStmt);
            executor.execute();
        } catch (Exception e) {
            LOG.warn("execute query failed. " + e);
            response.getContent().append("execute query failed. " + e.getMessage());
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
            return;
        }

        DumpInfo dumpInfo = context.getDumpInfo();
        if (dumpInfo != null) {
            response.getContent().append(GsonUtils.GSON.toJson(dumpInfo, QueryDumpInfo.class));
            sendResult(request, response);
        } else {
            response.getContent().append("not use cbo planner, try again.");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
        }
    }
}
