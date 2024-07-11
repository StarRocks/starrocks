// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.http.rest;

import com.google.common.base.Strings;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.dump.QueryDumper;
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

    public static final String URL = "/api/query_dump";
    private static final String DB = "db";
<<<<<<< HEAD
    private static final Gson GSON = new GsonBuilder()
            .addSerializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
            .addDeserializationExclusionStrategy(new GsonUtils.HiddenAnnotationExclusionStrategy())
            .enableComplexMapKeySerialization()
            .disableHtmlEscaping()
            .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpSerializer())
            .registerTypeAdapter(QueryDumpInfo.class, new QueryDumpDeserializer())
            .create();
=======
    private static final String MOCK = "mock";
>>>>>>> d42320f9a2 ([Feature] Add function get_query_dump (#48105))

    public QueryDumpAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, URL, new QueryDumpAction(controller));
    }

    @Override
    public void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        ConnectContext context = ConnectContext.get();
        String catalogDbName = request.getSingleParameter(DB);

        String catalogName = "";
        String dbName = "";
        if (!Strings.isNullOrEmpty(catalogDbName)) {
            String[] catalogDbNames = catalogDbName.split("\\.");

            catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
            if (catalogDbNames.length == 2) {
                catalogName = catalogDbNames[0];
            }
            dbName = catalogDbNames[catalogDbNames.length - 1];
        }
        context.setIsHTTPQueryDump(true);

        String query = request.getContent();

<<<<<<< HEAD
        StatementBase parsedStmt;
        try {
            parsedStmt = SqlParser.parse(query, context.getSessionVariable()).get(0);
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
            response.getContent().append(GSON.toJson(dumpInfo, QueryDumpInfo.class));
            sendResult(request, response);
        } else {
            response.getContent().append("not use cbo planner, try again.");
            sendResult(request, response, HttpResponseStatus.BAD_REQUEST);
        }
=======
        Pair<HttpResponseStatus, String> statusAndRes = QueryDumper.dumpQuery(catalogName, dbName, query, enableMock);

        response.getContent().append(statusAndRes.second);
        sendResult(request, response, statusAndRes.first);
>>>>>>> d42320f9a2 ([Feature] Add function get_query_dump (#48105))
    }
}
