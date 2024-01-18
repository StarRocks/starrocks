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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.starrocks.authz.authorization.AccessDeniedException;
import com.starrocks.authz.authorization.PrivilegeType;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeLocksItem;
import com.starrocks.thrift.TFeLocksReq;
import com.starrocks.thrift.TFeLocksRes;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.thrift.TException;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class SysFeLocks {

    public static final String NAME = "fe_locks";

    public static SystemTable create() {
        return new SystemTable(SystemId.FE_LOCKS_ID, NAME,
                Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("lock_type", ScalarType.createVarcharType(64))
                        .column("lock_object", ScalarType.createVarcharType(64))
                        .column("lock_mode", ScalarType.createVarcharType(64))
                        .column("start_time", ScalarType.createType(PrimitiveType.DATETIME))
                        .column("hold_time_ms", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("thread_info", ScalarType.createVarcharType(64))
                        .column("granted", ScalarType.createType(PrimitiveType.BOOLEAN))
                        .column("waiter_list", ScalarType.createVarcharType(SystemTable.NAME_CHAR_LEN))
                        .build(),
                TSchemaTableType.SYS_FE_LOCKS);
    }

    @VisibleForTesting
    public static TFeLocksRes listLocks(TFeLocksReq request, boolean authenticate) throws TException {
        TAuthInfo auth = request.getAuth_info();
        UserIdentity currentUser;
        if (auth.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(auth.getCurrent_user_ident());
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(auth.getUser(), auth.getUser_ip());
        }

        // authorize
        try {
            if (authenticate) {
                Authorizer.checkSystemAction(currentUser, null, PrivilegeType.OPERATE);
            }
        } catch (AccessDeniedException e) {
            throw new TException(e.getMessage(), e);
        }

        TFeLocksRes response = new TFeLocksRes();
        Collection<Database> dbs = GlobalStateMgr.getCurrentState().getFullNameToDb().values();
        for (Database db : CollectionUtils.emptyIfNull(dbs)) {
            TFeLocksItem item = resolveLockInfo(db);
            response.addToItems(item);
        }
        return response;
    }

    @VisibleForTesting
    protected static TFeLocksItem resolveLockInfo(Database db) {
        var lock = db.getLock();
        TFeLocksItem lockItem = new TFeLocksItem();
        lockItem.setLock_type("DATABASE");
        lockItem.setLock_object(db.getFullName());

        Thread owner = lock.getOwner();
        List<Long> sharedLockThreadIds = lock.getSharedLockThreadIds();
        long currentTime = System.currentTimeMillis();

        if (owner != null) {
            lockItem.setLock_mode("EXCLUSIVE");
            lockItem.setGranted(true);
            JsonObject ownerInfo = new JsonObject();
            ownerInfo.addProperty("threadId", owner.getId());
            ownerInfo.addProperty("threadName", owner.getName());
            lockItem.setThread_info(ownerInfo.toString());

            // wait start
            long lockStartTime = lock.getExclusiveLockTime();
            lockItem.setStart_time(lockStartTime);
            lockItem.setHold_time_ms(currentTime - lockStartTime);
        } else if (CollectionUtils.isNotEmpty(sharedLockThreadIds)) {
            lockItem.setLock_mode("SHARED");
            lockItem.setGranted(true);

            // lock start
            long lockStart = ListUtils.emptyIfNull(sharedLockThreadIds).stream()
                    .map(lock::getSharedLockTime)
                    .filter(x -> x > 0)
                    .min(Comparator.naturalOrder()).orElse(0L);
            lockItem.setStart_time(lockStart);
            lockItem.setHold_time_ms(currentTime - lockStart);

            // thread info
            JsonArray sharedLockInfo = new JsonArray();
            for (long threadId : ListUtils.emptyIfNull(sharedLockThreadIds)) {
                JsonObject lockInfo = new JsonObject();
                lockInfo.addProperty("threadId", threadId);
                Thread thread = ThreadUtils.findThreadById(threadId);
                if (thread != null) {
                    lockInfo.addProperty("threadName", thread.getName());
                }
                sharedLockInfo.add(lockInfo);
            }
            lockItem.setThread_info(sharedLockInfo.toString());

        } else {
            lockItem.setGranted(false);
        }

        // waiters
        Collection<Thread> waiters = lock.getQueuedThreads();
        JsonArray waiterIds = new JsonArray();
        for (Thread th : CollectionUtils.emptyIfNull(waiters)) {
            if (th != null) {
                JsonObject waiter = new JsonObject();
                waiter.addProperty("threadId", th.getId());
                waiter.addProperty("threadName", th.getName());
                waiterIds.add(waiter);
            }
        }
        lockItem.setWaiter_list(waiterIds.toString());

        return lockItem;
    }
}
