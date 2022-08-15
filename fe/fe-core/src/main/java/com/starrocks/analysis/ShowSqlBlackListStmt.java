// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.analysis;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

// used to show sql's blacklist
// format is 
// Id | Forbidden SQL
// 
// for example:
// +-------+--------------------------------------+
// | Id | Forbidden SQL                        |
// +-------+--------------------------------------+
// | 1     | select count *\(\*\) from .+         |
// | 2     | select count 2342423 *\(\*\) from .+ |
// +-------+--------------------------------------+
public class ShowSqlBlackListStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowSqlBlackListStmt.class);

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Id", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Forbidden SQL", ScalarType.createVarchar(100)))
                    .build();

    private Set<String> sqlBlackListSet;

    public void setSqlBlackListSet(Set<String> sqlBlackListSet) {
        this.sqlBlackListSet = sqlBlackListSet;
    }

    public Set<String> getSqlBlackListSet() {
        return sqlBlackListSet;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (!GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        super.analyze(analyzer);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
