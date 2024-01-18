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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/TransProcDir.java

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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;

import java.util.List;

public class TransProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TransactionId")
            .add("Label")
            .add("Coordinator")
            .add("TransactionStatus")
            .add("LoadJobSourceType")
            .add("PrepareTime")
            .add("CommitTime")
            .add("PublishTime")
            .add("FinishTime")
            .add("Reason")
            .add("ErrorReplicasCount")
            .add("ListenerId")
            .add("TimeoutMs")
            .add("ErrMsg")
            .build();

    public static final int MAX_SHOW_ENTRIES = 2000;

    private long dbId;
    private String state;

    public TransProcDir(long dbId, String state) {
        this.dbId = dbId;
        this.state = state;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        List<List<String>> infos = transactionMgr.getDbTransInfo(dbId, state.equals("running"), MAX_SHOW_ENTRIES);
        result.setRows(infos);
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String tidStr) throws AnalysisException {
        if (tidStr == null || Strings.isNullOrEmpty(tidStr)) {
            throw new AnalysisException("Table id is null");
        }

        long tid = -1L;
        try {
            tid = Long.valueOf(tidStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid transaction id format: " + tid);
        }
        return new TransTablesProcDir(dbId, tid);
    }
}
