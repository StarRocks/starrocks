// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.http.rest;

/* Usage:
   eg:
        POST  /api/sql
        post_data={"query": "select count(*) from information_schema.engines, "context": {"sqlTimeZone": "Asia/Shanghai"}}
 return:
{
    "meta": [
        {
            "name": "count(*)",
            "type": "bigint(20)"
        }
    ],
    "data": [
        {
            "count(*)": 0
        }
    ],
    "statistics": {
        "scanRows": 0,
        "scanBytes": 0,
        "returnRows": 1
    }
}
 */

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.starrocks.analysis.QueryStmt;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.DdlException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.OriginStatement;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.ParsingException;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;

public class ExecuteSqlAction extends RestBaseAction {
    private static final Logger LOG = LogManager.getLogger(ExecuteSqlAction.class);

    public ExecuteSqlAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.POST, "/api/sql", new ExecuteSqlAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        response.setContentType("application/json");

        String postContent = request.getContent();
        ConnectContext context = ConnectContext.get();
        SqlRequest requestBody;
        StatementBase parsedStmt;
        try {
            Type type = new TypeToken<SqlRequest>() {
            }.getType();
            requestBody = new Gson().fromJson(postContent, type);
            if (Strings.isNullOrEmpty(requestBody.query) || Strings.isNullOrEmpty(requestBody.query.trim())) {
                response.appendContent(new RestBaseResult("query can not be empty").toJson());
                writeResponse(request, response, BAD_REQUEST);
                return;
            }

            checkSessionVariable(context, requestBody.context);

            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser
                    .parse(requestBody.query, context.getSessionVariable().getSqlMode());
            if (stmts.size() > 1) {
                response.appendContent(new RestBaseResult("/api/sql not support execute multiple query").toJson());
                writeResponse(request, response, BAD_REQUEST);
                return;
            }

            parsedStmt = stmts.get(0);
            if (!(parsedStmt instanceof QueryStmt
                    || parsedStmt instanceof QueryStatement
                    || parsedStmt instanceof ShowStmt)) {
                response.appendContent(
                        new RestBaseResult("/api/sql only support SELECT, SHOW, EXPLAIN, DESC statement").toJson());
                writeResponse(request, response, BAD_REQUEST);
                return;
            }

            if (((parsedStmt instanceof QueryStmt) && ((QueryStmt) parsedStmt).hasOutFileClause())
                    || ((parsedStmt instanceof QueryStatement) && ((QueryStatement) parsedStmt).hasOutFileClause())) {
                response.appendContent(
                        new RestBaseResult("/api/sql does not support a query with OUTFILE clause").toJson());
                writeResponse(request, response, BAD_REQUEST);
                return;
            }

            PrivilegeChecker.check(parsedStmt, context);
        } catch (JsonSyntaxException e) {
            response.appendContent(new RestBaseResult("malformed json [ " + postContent + " ]").toJson());
            writeResponse(request, response, BAD_REQUEST);
            return;
        } catch (ParsingException parsingException) {
            response.appendContent(new RestBaseResult(parsingException.getErrorMessage()).toJson());
            writeResponse(request, response, BAD_REQUEST);
            return;
        } catch (Throwable e) {
            response.appendContent(new RestBaseResult(e.getMessage() + "\n " + e.getCause()).toJson());
            writeResponse(request, response, INTERNAL_SERVER_ERROR);
            return;
        }

        parsedStmt.setOrigStmt(new OriginStatement(requestBody.query));

        StmtExecutor executor = new StmtExecutor(context, parsedStmt);
        try {
            executor.execute();
        } catch (Exception e) {
            response.appendContent(new RestBaseResult(e.getMessage()).toJson());
            writeResponse(request, response, INTERNAL_SERVER_ERROR);
            return;
        }

        if (context.getState().getErrType() != null) {
            response.appendContent(new RestBaseResult(context.getState().getErrorMessage()).toJson());
            writeResponse(request, response, INTERNAL_SERVER_ERROR);
        }
    }

    private void checkSessionVariable(ConnectContext connectContext, SessionVariable customVariable) {
        if (customVariable != null) {
            connectContext.setSessionVariable(customVariable);
        }
    }

    private static class SqlRequest {
        public String query;
        public SessionVariable context;

        @Override
        public String toString() {
            String sessionVariable = "{}";
            try {
                sessionVariable = context.getJsonString();
            } catch (Exception e) {
                // ignore
            }
            return "SqlRequest{" +
                    "query='" + query + '\'' +
                    ", context=" + sessionVariable +
                    '}';
        }
    }
}
