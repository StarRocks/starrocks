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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/TransTablesProcDir.java

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
import com.starrocks.common.util.ListComparator;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.transaction.GlobalTransactionMgr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransTablesProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TableId")
            .add("CommittedPartitionIds")
            .build();

    private long dbId;
    private long txnId;

    public TransTablesProcDir(long dbId, long txnId) {
        this.dbId = dbId;
        this.txnId = txnId;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        // get info
        GlobalTransactionMgr transactionMgr = GlobalStateMgr.getCurrentGlobalTransactionMgr();
        List<List<Comparable>> tableInfos = transactionMgr.getTableTransInfo(dbId, txnId);
        // sort by table id
        ListComparator<List<Comparable>> comparator = new ListComparator<List<Comparable>>(0);
        Collections.sort(tableInfos, comparator);

        // set result
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        for (List<Comparable> info : tableInfos) {
            List<String> row = new ArrayList<String>(info.size());
            for (Comparable comparable : info) {
                row.add(comparable.toString());
            }
            result.addRow(row);
        }

        return result;
    }

    @Override
    public ProcNodeInterface lookup(String tableIdStr) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableIdStr)) {
            throw new AnalysisException("TableIdStr is null");
        }

        long tableId = -1L;
        try {
            tableId = Long.valueOf(tableIdStr);
        } catch (NumberFormatException e) {
            throw new AnalysisException("Invalid table id format: " + tableIdStr);
        }

        return new TransPartitionProcNode(dbId, txnId, tableId);
    }
}
