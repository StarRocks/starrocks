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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.gson.JsonObject;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.common.util.concurrent.lock.LockHolder;
import com.starrocks.common.util.concurrent.lock.LockInfo;
import com.starrocks.common.util.concurrent.lock.LockManager;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeLocksItem;
import com.starrocks.thrift.TFeLocksReq;
import com.starrocks.thrift.TFeLocksRes;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.apache.thrift.TException;

import java.util.List;
import java.util.stream.Collectors;

public class SysFeLocks {
    public static final String NAME = "fe_locks";

    public static SystemTable create() {
        return new SystemTable(SystemId.FE_LOCKS_ID, NAME,
                Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("lock_type", TypeFactory.createVarcharType(64))
                        .column("lock_object", TypeFactory.createVarcharType(64))
                        .column("lock_mode", TypeFactory.createVarcharType(64))
                        .column("start_time", DateType.DATETIME)
                        .column("hold_time_ms", IntegerType.BIGINT)
                        .column("thread_info", TypeFactory.createVarcharType(64))
                        .column("granted", BooleanType.BOOLEAN)
                        .column("waiter_list", TypeFactory.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.SYS_FE_LOCKS);
    }

    @VisibleForTesting
    public static TFeLocksRes listLocks(TFeLocksReq request, boolean authenticate) throws TException {
        TAuthInfo auth = request.getAuth_info();
        ConnectContext context = new ConnectContext();
        UserIdentityUtils.setAuthInfoFromThrift(context, auth);

        // authorize
        try {
            if (authenticate) {
                Authorizer.checkSystemAction(context, PrivilegeType.OPERATE);
            }
        } catch (AccessDeniedException e) {
            throw new TException(e.getMessage(), e);
        }

        TFeLocksRes response = new TFeLocksRes();
        long currentTime = System.currentTimeMillis();
        LockManager lockManager = GlobalStateMgr.getCurrentState().getLockManager();
        List<LockInfo> lockInfos = lockManager.dumpLockManager();

        for (LockInfo lockInfo : lockInfos) {
            for (LockHolder owner : lockInfo.getOwners()) {
                TFeLocksItem lockItem = new TFeLocksItem();

                lockItem.setLock_type("");
                lockItem.setLock_object(String.valueOf(lockInfo.getRid()));
                lockItem.setLock_mode(owner.getLockType().toString());
                lockItem.setStart_time(owner.getLocker().getLockRequestTimeMs());
                lockItem.setHold_time_ms(currentTime - owner.getLockAcquireTimeMs());

                JsonObject ownerInfo = new JsonObject();
                ownerInfo.addProperty("threadId", owner.getLocker().getThreadId());
                ownerInfo.addProperty("threadName", owner.getLocker().getThreadName());
                lockItem.setThread_info(ownerInfo.toString());

                List<String> waiters = lockInfo.getWaiters().stream().map(LockHolder::getLocker)
                        .map(Locker::toString).collect(Collectors.toList());
                lockItem.setWaiter_list(Joiner.on(",").join(waiters));
                response.addToItems(lockItem);
            }
        }
        return response;
    }
}
