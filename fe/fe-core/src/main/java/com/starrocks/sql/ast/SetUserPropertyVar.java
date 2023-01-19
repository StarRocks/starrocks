// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


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
                // In new RBAC framework, set user property will be checked in PrivilegeCheckerV2
                if (!GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
                    if (!GlobalStateMgr.getCurrentState().getAuth()
                            .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                "ADMIN");
                    }
                }
                return;
            }
        }

        for (Pattern commPattern : UserProperty.COMMON_PROPERTIES) {
            Matcher matcher = commPattern.matcher(key);
            if (matcher.find()) {
                // In new RBAC framework, set user property will be checked in PrivilegeCheckerV2
                if (!GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
                    if (!isSelf && !GlobalStateMgr.getCurrentState().getAuth().checkGlobalPriv(ConnectContext.get(),
                            PrivPredicate.ADMIN)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                "GRANT");
                    }
                }
                return;
            }
        }

        throw new AnalysisException("Unknown property key: " + key);
    }
}
