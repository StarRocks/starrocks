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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/DbsProcDir.java

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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.common.concurrent.locks.LockType;
import com.starrocks.common.concurrent.locks.Locker;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.structure.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.common.util.ListComparator;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.server.GlobalStateMgr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * SHOW PROC /dbs/
 * show all dbs' info
 */
public class DbsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbId").add("DbName").add("TableNum").add("Quota")
            .add("LastConsistencyCheckTime").add("ReplicaQuota")
            .build();

    private final GlobalStateMgr globalStateMgr;

    public DbsProcDir(GlobalStateMgr globalStateMgr) {
        this.globalStateMgr = globalStateMgr;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String dbIdOrName) throws AnalysisException {
        if (globalStateMgr == null) {
            throw new AnalysisException("globalStateMgr is null");
        }
        if (Strings.isNullOrEmpty(dbIdOrName)) {
            throw new AnalysisException("database id or name is null or empty");
        }

        Database db;
        try {
            db = globalStateMgr.getDb(Long.parseLong(dbIdOrName));
        } catch (NumberFormatException e) {
            db = globalStateMgr.getDb(dbIdOrName);
        }

        if (db == null) {
            throw new AnalysisException("Unknown database id or name \"" + dbIdOrName + "\"");
        }

        return new TablesProcDir(db);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(globalStateMgr);
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<String> dbNames = globalStateMgr.getDbNames();
        if (dbNames == null || dbNames.isEmpty()) {
            // empty
            return result;
        }

        // get info
        List<List<Comparable>> dbInfos = new ArrayList<List<Comparable>>();
        for (String dbName : dbNames) {
            Database db = globalStateMgr.getDb(dbName);
            if (db == null) {
                continue;
            }
            List<Comparable> dbInfo = new ArrayList<Comparable>();
            Locker locker = new Locker();
            locker.lockDatabase(db, LockType.READ);
            try {
                int tableNum = db.getTables().size();
                dbInfo.add(db.getId());
                dbInfo.add(dbName);
                dbInfo.add(tableNum);

                long dataQuota = db.getDataQuota();
                Pair<Double, String> quotaUnitPair = DebugUtil.getByteUint(dataQuota);
                String readableQuota = DebugUtil.DECIMAL_FORMAT_SCALE_3.format(quotaUnitPair.first) + " "
                        + quotaUnitPair.second;
                dbInfo.add(readableQuota);

                dbInfo.add(TimeUtils.longToTimeString(db.getLastCheckTime()));

                long replicaQuota = db.getReplicaQuota();
                dbInfo.add(replicaQuota);

            } finally {
                locker.unLockDatabase(db, LockType.READ);
            }
            dbInfos.add(dbInfo);
        }

        // order by dbId, asc
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(dbInfos, comparator);

        // set result
        for (List<Comparable> info : dbInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }
        return result;
    }
}
