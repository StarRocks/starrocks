// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.execution;

import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.StatementBase;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.load.EtlJobType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ShowResultSet;

public class LoadExecutor implements DataDefinitionExecutor {

    public ShowResultSet execute(StatementBase stmt, ConnectContext context) throws DdlException {
        LoadStmt loadStmt = (LoadStmt) stmt;
        EtlJobType jobType = loadStmt.getEtlJobType();
        if (jobType == EtlJobType.UNKNOWN) {
            throw new DdlException("Unknown load job type");
        }
        if (jobType == EtlJobType.HADOOP && Config.disable_hadoop_load) {
            throw new DdlException("Load job by hadoop cluster is disabled."
                    + " Try using broker load. See 'help broker load;'");
        }

        context.getGlobalStateMgr().getLoadManager().createLoadJobFromStmt(loadStmt, context);
        return null;
    }
}
