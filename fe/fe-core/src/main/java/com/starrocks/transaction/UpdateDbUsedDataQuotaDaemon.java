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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/transaction/UpdateDbUsedDataQuotaDaemon.java

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

package com.starrocks.transaction;

import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class UpdateDbUsedDataQuotaDaemon extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(UpdateDbUsedDataQuotaDaemon.class);

    public UpdateDbUsedDataQuotaDaemon() {
        super("UpdateDbUsedDataQuota", Config.db_used_data_quota_update_interval_secs * 1000L);
    }

    @Override
    protected void runAfterCatalogReady() {
        updateAllDatabaseUsedDataQuota();
    }

    private void updateAllDatabaseUsedDataQuota() {
        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        List<Long> dbIdList = globalStateMgr.getLocalMetastore().getDbIds();
        GlobalTransactionMgr globalTransactionMgr = globalStateMgr.getGlobalTransactionMgr();
        for (Long dbId : dbIdList) {
            Database db = globalStateMgr.getDb(dbId);
            if (db == null) {
                LOG.warn("Database [" + dbId + "] doese not exist, skip to update database used data quota");
                continue;
            }
            if (db.isSystemDatabase()) {
                continue;
            }
            try {
                long usedDataQuotaBytes = db.getUsedDataQuotaWithLock();
                globalTransactionMgr.updateDatabaseUsedQuotaData(dbId, usedDataQuotaBytes);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Update database[{}] used data quota bytes : {}.", db.getOriginName(), usedDataQuotaBytes);
                }
            } catch (AnalysisException e) {
                LOG.warn("Update database[" + db.getOriginName() + "] used data quota failed", e);
            }
        }
    }
}
