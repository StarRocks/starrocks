// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/InsertLoadJob.java

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

package com.starrocks.load.loadv2;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.starrocks.catalog.AuthorizationInfo;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.FailMsg;
import com.starrocks.load.FailMsg.CancelType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * The class is performed to record the finished info of insert load job.
 * It is created after txn is visible which belongs to insert load job.
 * The state of insert load job is always finished, so it will never be scheduled by JobScheduler.
 */
public class InsertLoadJob extends LoadJob {

    private long tableId;

    // only for log replay
    public InsertLoadJob() {
        super();
        this.jobType = EtlJobType.INSERT;
    }

    public InsertLoadJob(String label, long dbId, long tableId, long createTimestamp, String failMsg,
                         String trackingUrl) throws MetaNotFoundException {
        super(dbId, label);
        this.tableId = tableId;
        this.createTimestamp = createTimestamp;
        this.loadStartTimestamp = createTimestamp;
        this.finishTimestamp = System.currentTimeMillis();
        if (Strings.isNullOrEmpty(failMsg)) {
            this.state = JobState.FINISHED;
            this.progress = 100;
        } else {
            this.state = JobState.CANCELLED;
            this.failMsg = new FailMsg(CancelType.LOAD_RUN_FAIL, failMsg);
            this.progress = 0;
        }
        this.jobType = EtlJobType.INSERT;
        this.timeoutSecond = Config.insert_load_default_timeout_second;
        this.authorizationInfo = gatherAuthInfo();
        this.loadingStatus.setTrackingUrl(trackingUrl);
    }

    public AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        return new AuthorizationInfo(database.getFullName(), getTableNames());
    }

    @Override
    public Set<String> getTableNamesForShow() {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            return Sets.newHashSet(String.valueOf(tableId));
        }
        // The database will not be locked in here.
        // The getTable is a thread-safe method called without read lock of database
        Table table = database.getTable(tableId);
        if (table == null) {
            return Sets.newHashSet(String.valueOf(tableId));
        }
        return Sets.newHashSet(table.getName());
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        Table table = database.getTable(tableId);
        if (table == null) {
            throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
        }
        return Sets.newHashSet(table.getName());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(tableId);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        tableId = in.readLong();
    }
}
