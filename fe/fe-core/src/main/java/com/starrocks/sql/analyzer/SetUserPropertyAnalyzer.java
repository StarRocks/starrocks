// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.analyzer;

import com.google.common.base.Strings;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.qe.ConnectContext;

public class SetUserPropertyAnalyzer {

    public static void analyze(SetUserPropertyStmt statement, ConnectContext context) {
        String user = statement.getUser();
        if (Strings.isNullOrEmpty(user)) {
            // If param 'user' is not set, use the login user name.
            // The login user name is full-qualified with cluster name.
            statement.setUser(ConnectContext.get().getQualifiedUser());
        } else {
            // If param 'user' is set, check if it need to be full-qualified
            if (!user.equals(Auth.ROOT_USER)) {
                statement.setUser(user);
            }
        }
    }

}
