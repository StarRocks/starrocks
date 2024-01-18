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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/AuthProcDir.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.common.proc;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;

/*
 * It describes the information about the authorization(privilege) and the authentication(user)
 * SHOW PROC /auth/
 */
public class AuthProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("UserIdentity").add("Password").add("AuthPlugin").add("UserForAuthPlugin").add("GlobalPrivs")
            .add("DatabasePrivs")
            .add("TablePrivs").add("ResourcePrivs").build();

    private Auth auth;

    public AuthProcDir(Auth auth) {
        this.auth = auth;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String userIdent) throws AnalysisException {
        if (Strings.isNullOrEmpty(userIdent)) {
            throw new AnalysisException("User is not specified");
        }

        UserIdentity userIdentity = UserIdentity.fromString(userIdent);
        if (userIdentity == null) {
            throw new AnalysisException("Invalid user ident: " + userIdent);
        }

        return new UserPropertyProcNode(auth, userIdentity.getUser());
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        result.setRows(GlobalStateMgr.getCurrentState().getAuth().getAuthInfo(null /* get all user */));
        return result;
    }
}

