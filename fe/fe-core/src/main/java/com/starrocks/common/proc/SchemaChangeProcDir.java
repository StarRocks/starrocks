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
import com.starrocks.alter.AlterJobV2;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.alter.SchemaChangeJobV2;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.OrderByPair;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.LimitElement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SchemaChangeProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES;
    static {
        ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>()
                .add("JobId").add("TableName").add("CreateTime").add("FinishTime")
                .add("IndexName").add("IndexId").add("OriginIndexId").add("SchemaVersion")
                .add("TransactionId").add("State").add("Msg").add("Progress").add("Timeout");
        if (RunMode.getCurrentRunMode() == RunMode.SHARED_DATA) {
            builder.add("Warehouse");
        }
        TITLE_NAMES = builder.build();
    }

    private SchemaChangeHandler schemaChangeHandler;
    private Database db;

    public SchemaChangeProcDir(SchemaChangeHandler schemaChangeHandler, Database db) {
        this.schemaChangeHandler = schemaChangeHandler;
        this.db = db;
    }

    public ProcResult fetchResultByFilter(HashMap<String, Expr> filter, List<OrderByPair> orderByPairs,
                                          LimitElement limitElement) throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(schemaChangeHandler);

        return ProcUtils.applyFilterOrderLimit(TITLE_NAMES, schemaChangeHandler.getAlterJobInfosByDb(db),
                filter, orderByPairs, limitElement);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(db);
        Preconditions.checkNotNull(schemaChangeHandler);

        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<List<Comparable>> schemaChangeJobInfos = schemaChangeHandler.getAlterJobInfosByDb(db);
        for (List<Comparable> infoStr : schemaChangeJobInfos) {
            List<String> oneInfo = new ArrayList<String>(TITLE_NAMES.size());
            for (Comparable element : infoStr) {
                oneInfo.add(element.toString());
            }
            result.addRow(oneInfo);
        }
        return result;
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
        if (Strings.isNullOrEmpty(jobIdStr)) {
            throw new AnalysisException("Job id is null");
        }

        long jobId = -1L;
        try {
            jobId = Long.valueOf(jobIdStr);
        } catch (Exception e) {
            throw new AnalysisException("Job id is invalid");
        }

        Preconditions.checkState(jobId != -1L);
        AlterJobV2 job = schemaChangeHandler.getUnfinishedAlterJobV2ByJobId(jobId);
        if (job == null) {
            return null;
        }

        return new SchemaChangeJobProcNode((SchemaChangeJobV2) job);
    }
}
