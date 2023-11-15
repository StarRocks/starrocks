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

package com.starrocks.catalog.system.sys;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeLocksReq;
import com.starrocks.thrift.TFeLocksRes;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.thrift.TException;

import java.util.Collection;

public class FeLocks {

    public static final String NAME = "fe_locks";

    public static SystemTable create() {
        return new SystemTable(SystemId.FE_LOCKS_ID, NAME,
                Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("lock_type", ScalarType.createVarcharType(64))
                        .column("lock_object", ScalarType.createVarcharType(64))
                        .column("lock_mode", ScalarType.createVarcharType(64))
                        .column("thread_info", ScalarType.createVarcharType(64))
                        .column("granted", ScalarType.createVarchar(64))
                        .column("wait_start", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .column("waiter_list", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.SYS_FE_LOCKS);
    }

    public static TFeLocksRes listLocks(TFeLocksReq request) throws TException {
        TAuthInfo auth = request.getAuth_info();
        UserIdentity currentUser;
        if (auth.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(auth.getCurrent_user_ident());
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(auth.getUser(), auth.getUser_ip());
        }

        // authorize
        try {
            Authorizer.checkSystemAction(currentUser, null, PrivilegeType.SELECT);
        } catch (AccessDeniedException e) {
            throw new TException(e.getMessage());
        }

        TFeLocksRes response = new TFeLocksRes();
        Collection<Database> dbs = GlobalStateMgr.getCurrentState().getFullNameToDb().values();
        for (Database db : CollectionUtils.emptyIfNull(dbs)) {
            // TODO
        }
        return response;
    }
}
