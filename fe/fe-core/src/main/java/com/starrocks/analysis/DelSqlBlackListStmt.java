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

import java.util.List;

// use for delete sql's blacklist by ids.
// indexs is the ids of regular expression's sql
public class DelSqlBlackListStmt extends StatementBase {

    private List<Long> indexs;

    public List<Long> getIndexs() {
        return indexs;
    }

    public DelSqlBlackListStmt(List<Long> indexs) {
        this.indexs = indexs;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        super.analyze(analyzer);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}

