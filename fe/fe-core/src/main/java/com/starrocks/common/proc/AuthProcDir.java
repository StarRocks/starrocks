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

package com.starrocks.common.proc;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;

/*
 * Shows information about authorization (privileges) and authentication (users)
 * SHOW PROC "/auth"
 */
public class AuthProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("UserIdentity").add("Password").add("AuthPlugin").add("Roles").add("GlobalPrivs")
            .add("CatalogPrivs").add("DatabasePrivs").add("TablePrivs").add("ResourcePrivs")
            .build();

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String userIdent) throws AnalysisException {
        if (Strings.isNullOrEmpty(userIdent)) {
            throw new AnalysisException("User not specified");
        }

        UserIdentity userIdentity = UserIdentity.fromString(userIdent);

        if (userIdentity == null) {
            // Check if it's just a username without quotes and host
            if (!userIdent.contains("'") && !userIdent.contains("@")) {
                // Try formatting as 'username'@'%' (default host)
                userIdentity = UserIdentity.fromString("'" + userIdent + "'@'%'");
            } else if (userIdent.contains("@") && !userIdent.startsWith("'")) {
                // Try to handle username@host format without proper quotes
                String[] parts = userIdent.split("@", 2);
                if (parts.length == 2) {
                    String username = parts[0];
                    String host = parts[1];

                    username = username.replace("'", "");
                    host = host.replace("'", "");

                    // Format properly
                    userIdentity = UserIdentity.fromString("'" + username + "'@'" + host + "'");
                }
            }
        }

        if (userIdentity == null) {
            throw new AnalysisException("Invalid user identity: " + userIdent +
                    ". Expected format: 'username'@'host' or simply username");
        }

        return new AuthProcNode(userIdentity);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        result.setRows(GlobalStateMgr.getCurrentState().getAuthProcSupplier().getAuthInfo());
        return result;
    }
} 