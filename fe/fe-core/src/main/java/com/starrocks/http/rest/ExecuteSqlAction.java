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

/* Usage:
   eg:
     curl -X POST '${url}/api/v2/default_catalog/${db[0]}/sql' -u 'root:'
     -d '{"query": "select * from duplicate_table_with_null order by k6;"}'
     --header "Content-Type: application/json"

   response is in form of ndjson, which means json objects Separated by newlines：

    {"connectionId":70}
    {"meta":[{"name":"k1","type":"date"},{"name":"k2","type":"datetime"},{"name":"k3","type":"varchar"}]}
    {"data":[null,null,null]}
    {"data":["2020-01-25","2022-12-26 09:06:09","anhui"]}
    {"data":["2020-01-26","2022-12-26 09:06:10","beijin"]}
    {"data":["2020-01-27","2022-12-26 09:06:11","chengdu"]}
    {"statistics":{"scanRows":0,"scanBytes":0,"returnRows":4}}

 */

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.common.util.LogUtil;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.HttpConnectContext;
import com.starrocks.http.HttpConnectProcessor;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectScheduler;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.service.ExecuteEnv;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.parser.ParsingException;
import com.starrocks.thrift.TResultSinkFormatType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.AttributeKey;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

public class ExecuteSqlAction extends RestBaseAction {

    private static final AttributeKey<HttpConnectContext> HTTP_CONNECT_CONTEXT_ATTRIBUTE_KEY =
            AttributeKey.valueOf("httpContextKey");

    private static final Logger LOG = LogManager.getLogger(ExecuteSqlAction.class);

    public ExecuteSqlAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/v2/{" + CATALOG_KEY + "}/{" + DB_KEY + "}/sql",
                new ExecuteSqlAction(controller));
        controller.registerHandler(HttpMethod.POST, "/api/v2/{" + CATALOG_KEY + "}/sql",
                new ExecuteSqlAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        StatementBase parsedStmt;

        response.setContentType("application/x-ndjson; charset=utf-8");

        HttpConnectContext context = request.getConnectContext();

        String catalogName = request.getSingleParameter(CATALOG_KEY);
        String databaseName = request.getSingleParameter(DB_KEY);

        boolean keepAlive = HttpUtil.isKeepAlive(request.getRequest());
        if (keepAlive) {
            context.setKeepAlive(true);
        }

        try {
            changeCatalogAndDB(catalogName, databaseName, context);
            SqlRequest requestBody = validatePostBody(request.getContent(), context);
            // set result format as json,
            context.setResultSinkFormatType(TResultSinkFormatType.JSON);
            checkSessionVariable(requestBody.sessionVariables, context);
            // parse the sql here, for the convenience of verification of http request
            parsedStmt = parse(requestBody.query, context.getSessionVariable());
            context.setStatement(parsedStmt);

            // only register connectContext once for one channel
            if (!context.isInitialized()) {
                registerContext(requestBody.query, context);
                context.setInitialized(true);
            }

            // process this request
            HttpConnectProcessor connectProcessor = new HttpConnectProcessor(context);
            try {
                connectProcessor.processOnce();
            } catch (Exception e) {
                // just for safe. most Exception is handled in execute(), and set error code in context
                throw new StarRocksHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
            }

            // finalize just send 200 for kill, and throw StarRocksHttpException if context's error is set
            finalize(request, response, parsedStmt, context);

        } catch (StarRocksHttpException e) {
            RestBaseResult failResult = new RestBaseResult(e.getMessage());
            response.getContent().append(failResult.toJsonString());
            sendResult(request, response, HttpResponseStatus.valueOf(e.getCode().code()));
        }
    }

    private void changeCatalogAndDB(String catalogName, String databaseName, HttpConnectContext context)
            throws StarRocksHttpException {
        if (!catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "only support default_catalog right now");
        }

        try {
            context.getGlobalStateMgr().changeCatalog(context, catalogName);
            if (databaseName != null) {
                context.getGlobalStateMgr().changeCatalogDb(context, databaseName);
            }
        } catch (Exception e) {
            // 403 Forbidden DdlException
            throw new StarRocksHttpException(HttpResponseStatus.FORBIDDEN, "set catalog or db failed");
        }
    }

    private SqlRequest validatePostBody(String postContent, HttpConnectContext context) throws StarRocksHttpException {
        SqlRequest requestBody;
        StatementBase parsedStmt;
        try {
            Type type = new TypeToken<SqlRequest>() {
            }.getType();
            requestBody = new Gson().fromJson(postContent, type);
        } catch (JsonSyntaxException e) {
            throw new StarRocksHttpException(BAD_REQUEST, "malformed json [ " + postContent + " ]");
        }

        if (Strings.isNullOrEmpty(requestBody.query) || Strings.isNullOrEmpty(requestBody.query.trim())) {
            throw new StarRocksHttpException(BAD_REQUEST, "\"query can not be empty\"");
        }

        if (requestBody.disablePrintConnectionId) {
            context.set_disable_print_connection_id(true);
        }

        return requestBody;
    }

    private StatementBase parse(String sql, SessionVariable sessionVariables) throws StarRocksHttpException {
        StatementBase parsedStmt;
        List<StatementBase> stmts;
        try {
            stmts = com.starrocks.sql.parser.SqlParser
                    .parse(sql, sessionVariables);
        } catch (ParsingException parsingException) {
            throw new StarRocksHttpException(HttpResponseStatus.INTERNAL_SERVER_ERROR, parsingException.getMessage());
        }

        if (stmts.size() > 1) {
            throw new StarRocksHttpException(BAD_REQUEST,
                    "/api/v2/<catalog_name>/<database_name>/query does not support execute multiple query");
        }

        parsedStmt = stmts.get(0);
        if (!(parsedStmt instanceof QueryStatement
                || parsedStmt instanceof ShowStmt || parsedStmt instanceof KillStmt)) {
            throw new StarRocksHttpException(BAD_REQUEST,
                    "/api/v2/<catalog_name>/<database_name>/query only support SELECT, SHOW, EXPLAIN, DESC, KILL statement");
        }

        if (((parsedStmt instanceof QueryStatement) && ((QueryStatement) parsedStmt).hasOutFileClause())) {
            throw new StarRocksHttpException(BAD_REQUEST,
                    "/api/v2/<catalog_name>/<database_name>/query does not support a query with OUTFILE clause");
        }

        parsedStmt.setOrigStmt(new OriginStatement(sql));
        return parsedStmt;
    }

    // refer to AcceptListener.handleEvent
    private void registerContext(String sql, HttpConnectContext context) throws StarRocksHttpException {
        // now register this request in connectScheduler
        ConnectScheduler connectScheduler = ExecuteEnv.getInstance().getScheduler();
        connectScheduler.submit(context);

        context.setConnectScheduler(connectScheduler);
        // mark as registered
        boolean registered = connectScheduler.registerConnection(context);
        if (!registered) {
            throw new StarRocksHttpException(HttpResponseStatus.SERVICE_UNAVAILABLE, "Reach limit of connections");
        }
        context.setStartTime();
        LogUtil.logConnectionInfoToAuditLogAndQueryQueue(context, null);
    }

    // when connect is closed, this function will be called
    protected void handleChannelInactive(ChannelHandlerContext ctx) {
        LOG.info("Netty channel is closed");
        HttpConnectContext context = ctx.channel().attr(HTTP_CONNECT_CONTEXT_ATTRIBUTE_KEY).get();
        context.getConnectScheduler().unregisterConnection(context);
    }

    private void checkSessionVariable(Map<String, String> customVariable, HttpConnectContext context) {
        if (customVariable != null) {
            try {
                for (String key : customVariable.keySet()) {
                    VariableMgr.setSystemVariable(context.getSessionVariable(),
                            new SystemVariable(key, new StringLiteral(customVariable.get(key))), true);
                }
                context.setThreadLocalInfo();
            } catch (DdlException e) {
                throw new StarRocksHttpException(INTERNAL_SERVER_ERROR, context.getState().getErrorMessage());
            }
        }
    }

    // Currently finalize just send kill's result. But any other statement which only send state information can use finalize to send result
    private void finalize(BaseRequest request, BaseResponse response, StatementBase parsedStmt,
                          HttpConnectContext context)
            throws StarRocksHttpException, DdlException {

        // need forwarding to leader
        if (context.isForwardToLeader()) {
            redirectToLeader(request, response);
            return;
        }

        // exception was caught in StmtExecutor and set Error info in QueryState, so just send status 500 with exception info
        if (!context.getState().getErrorMessage().isEmpty()) {
            // for queryStatement, if some data already sent, we just close the channel
            if (parsedStmt instanceof QueryStatement && context.getSendDate()) {
                context.getNettyChannel().close();
                return;
            }
            // send error message：500 {"exception":"error message"}
            throw new StarRocksHttpException(INTERNAL_SERVER_ERROR, context.getState().getErrorMessage());
        }

        // right now, select and show will send out result in StmtExecutor.execute in streaming mode
        if (parsedStmt instanceof QueryStatement || parsedStmt instanceof ShowStmt) {
            return;
        }

        // only happend when commit suicid,same as mysql's \q command，send status 200, then close the channel
        // but why client will kill themselves instead of closing the channel directly?
        if (context.isKilled()) {
            HttpUtil.setKeepAlive(request.getRequest(), false);
        }

        // 200 OK for killStatement
        sendResult(request, response);
    }

    private static class SqlRequest {
        public String query;
        public Map<String, String> sessionVariables;
        public boolean disablePrintConnectionId;
    }
}
