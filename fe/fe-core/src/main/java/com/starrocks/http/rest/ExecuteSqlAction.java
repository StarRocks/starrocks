// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.http.rest;

/* Usage:
   eg:
        POST  /api/sql  post_data=query
 return:
        [{rowObj},{rowObj},...]
 */

import com.google.common.base.Strings;
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
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.analyzer.PrivilegeChecker;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.parser.ParsingException;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        String originStmt = request.getContent();
        if (Strings.isNullOrEmpty(originStmt)) {
            response.appendContent(new RestBaseResult("query can not empty").toJson());
            writeResponse(request, response, BAD_REQUEST);
            return;
        }

        ConnectContext context = ConnectContext.get();
        StatementBase parsedStmt;
        try {
            List<StatementBase> stmts = com.starrocks.sql.parser.SqlParser.parse(originStmt, context.getSessionVariable().getSqlMode());
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
                        new RestBaseResult("/api/sql only support SELECT, SHOW, EXPLAIN statement").toJson());
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
        } catch (ParsingException parsingException) {
            response.appendContent(new RestBaseResult(parsingException.getErrorMessage()).toJson());
            writeResponse(request, response, BAD_REQUEST);
            return;
        } catch (Throwable e) {
            response.appendContent(new RestBaseResult(e.getMessage() + "\n " + e.getCause()).toJson());
            writeResponse(request, response, INTERNAL_SERVER_ERROR);
            return;
        }

        parsedStmt.setOrigStmt(new OriginStatement(originStmt));

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
}
