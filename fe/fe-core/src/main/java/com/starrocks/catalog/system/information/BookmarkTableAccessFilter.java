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

package com.starrocks.catalog.system.information;

import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TAuthInfo;

import java.util.Set;

/**
 * Per-row authorization filter for (dbId, tableId) pairs surfaced by the
 * bookmark information_schema tables. {@link Authorizer#checkAnyActionOnTableLikeObject}
 * already covers both table-level and inherited db-level grants, so no
 * separate db-allowlist pre-pass is needed.
 */
public final class BookmarkTableAccessFilter {
    private final UserIdentity userIdentity;
    private final Set<Long> currentRoleIds;

    public BookmarkTableAccessFilter(TAuthInfo authInfo) {
        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, authInfo);
        this.userIdentity = context.getCurrentUserIdentity();
        this.currentRoleIds = context.getCurrentRoleIds();
    }

    public boolean isAuthorized(long dbId, long tableId) {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
        if (db == null) {
            return false;
        }
        Table t = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
        if (t == null) {
            return false;
        }
        try {
            Authorizer.checkAnyActionOnTableLikeObject(buildAuthorizerContext(), db.getFullName(), t);
            return true;
        } catch (AccessDeniedException e) {
            return false;
        }
    }

    private ConnectContext buildAuthorizerContext() {
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(userIdentity);
        context.setCurrentRoleIds(currentRoleIds);
        return context;
    }
}
