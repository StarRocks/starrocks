// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.ShowUserPropertyStmt;
import com.starrocks.qe.ConnectContext;

public class ShowUserPropertyAnalyzer {

    public static void analyze(ShowUserPropertyStmt statment, ConnectContext context) {
        String user = statment.getUser();
        if (Strings.isNullOrEmpty(user)) {
            statment.setUser(context.getQualifiedUser());
        } else {
            statment.setUser(user);
        }
        statment.setPattern(Strings.emptyToNull(statment.getPatter()));
    }
}
