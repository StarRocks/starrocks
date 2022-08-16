// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.starrocks.analysis.SetStmt;
import com.starrocks.analysis.SetVar;
import com.starrocks.qe.ConnectContext;

import java.util.List;

public class SetStmtAnalyzer {
    public static void analyze(SetStmt setStmt, ConnectContext session) {
        List<SetVar> setVars = setStmt.getSetVars();
        for (SetVar var : setVars) {
            var.analyze();
        }
    }
}
