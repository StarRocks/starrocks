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
import com.google.common.collect.ImmutableList;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LimitElement;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.OrderByPair;

import java.util.HashMap;
import java.util.List;

public class OptimizeProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("TableName").add("CreateTime").add("FinishTime")
            .add("Operation")
            .add("TransactionId").add("State").add("Msg").add("Progress").add("Timeout")
            .build();

    private SchemaChangeHandler schemaChangeHandler;
    private Database db;

    public OptimizeProcDir(SchemaChangeHandler schemaChangeHandler, Database db) {
        this.schemaChangeHandler = schemaChangeHandler;
        this.db = db;
    }

    public List<List<Comparable>> getOptimizeJobInfos() {
        return schemaChangeHandler.getOptimizeJobInfosByDb(db);
    }

    public ProcResult fetchResultByFilter(HashMap<String, Expr> filter, List<OrderByPair> orderByPairs,
                                          LimitElement limitElement) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(schemaChangeHandler);

        return ProcUtils.applyFilterOrderLimit(TITLE_NAMES, getOptimizeJobInfos(),
                filter, orderByPairs, limitElement);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(schemaChangeHandler);

        return ProcUtils.toProcResult(TITLE_NAMES, getOptimizeJobInfos());
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        return ProcUtils.analyzeColumn(TITLE_NAMES, columnName);
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String jobIdStr) throws AnalysisException {
        throw new AnalysisException("Not support");
    }
}
