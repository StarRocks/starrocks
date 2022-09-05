// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

// used to add regular expression from sql.
public class AddSqlBlackListStmt extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(AddSqlBlackListStmt.class);

    private String sql;

    public String getSql() {
        return sql;
    }

    private Pattern sqlPattern;

    public Pattern getSqlPattern() {
        return sqlPattern;
    }

    public AddSqlBlackListStmt(String sql) {
        this.sql = sql;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        super.analyze(analyzer);

        sql = sql.trim().toLowerCase().replaceAll(" +", " ");
        if (sql != null && sql.length() > 0) {
            try {
                sqlPattern = Pattern.compile(sql);
            } catch (PatternSyntaxException e) {
                ErrorReport.reportAnalysisException(e.getMessage());
                return;
            }
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}

