// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Strings;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.mysql.privilege.UserProperty;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SetUserPropertyVar extends SetVar {
    public static final String DOT_SEPARATOR = ".";

    private final String key;
    private final String value;

    public SetUserPropertyVar(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getPropertyKey() {
        return key;
    }

    public String getPropertyValue() {
        return value;
    }

    public void analyze(boolean isSelf) throws AnalysisException {
        if (Strings.isNullOrEmpty(key)) {
            throw new AnalysisException("User property key is null");
        }

        if (value == null) {
            throw new AnalysisException("User property value is null");
        }

        checkAccess(isSelf);
    }

    private void checkAccess(boolean isSelf) throws AnalysisException {
        for (Pattern advPattern : UserProperty.ADVANCED_PROPERTIES) {
            Matcher matcher = advPattern.matcher(key);
            if (matcher.find()) {
                if (!GlobalStateMgr.getCurrentState().getAuth()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "ADMIN");
                }
                return;
            }
        }

        for (Pattern commPattern : UserProperty.COMMON_PROPERTIES) {
            Matcher matcher = commPattern.matcher(key);
            if (matcher.find()) {
                if (!isSelf && !GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(),
                        PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "GRANT");
                }
                return;
            }
        }

        throw new AnalysisException("Unknown property key: " + key);
    }
}
