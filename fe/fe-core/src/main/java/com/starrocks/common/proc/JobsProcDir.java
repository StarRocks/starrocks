// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/proc/JobsProcDir.java

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
import com.google.common.collect.Lists;
import com.starrocks.alter.MaterializedViewHandler;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.common.AnalysisException;
import com.starrocks.load.ExportJob;
import com.starrocks.load.ExportMgr;
import com.starrocks.load.loadv2.LoadManager;

/*
 * SHOW PROC '/jobs/dbId/'
 * show job type
 */
public class JobsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobType").add("Pending").add("Running").add("Finished")
            .add("Cancelled").add("Total")
            .build();

    private static final String LOAD = "load";
    private static final String DELETE = "delete";
    private static final String ROLLUP = "rollup";
    private static final String SCHEMA_CHANGE = "schema_change";
    private static final String EXPORT = "export";

    private Catalog catalog;
    private Database db;

    public JobsProcDir(Catalog catalog, Database db) {
        this.catalog = catalog;
        this.db = db;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String jobTypeName) throws AnalysisException {
        if (Strings.isNullOrEmpty(jobTypeName) || catalog == null) {
            throw new AnalysisException("Job type name is null");
        }

        if (jobTypeName.equals(LOAD)) {
            return new LoadProcDir(db);
        } else if (jobTypeName.equals(DELETE)) {
            return new DeleteInfoProcDir(catalog.getDeleteHandler(), catalog.getLoadInstance(), db.getId());
        } else if (jobTypeName.equals(ROLLUP)) {
            return new RollupProcDir(catalog.getRollupHandler(), db);
        } else if (jobTypeName.equals(SCHEMA_CHANGE)) {
            return new SchemaChangeProcDir(catalog.getSchemaChangeHandler(), db);
        } else if (jobTypeName.equals(EXPORT)) {
            return new ExportProcNode(catalog.getExportMgr(), db);
        } else {
            throw new AnalysisException("Invalid job type: " + jobTypeName);
        }
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        Preconditions.checkNotNull(catalog);

        BaseProcResult result = new BaseProcResult();

        result.setNames(TITLE_NAMES);

        long dbId = db.getId();
        // load
        LoadManager loadManager = Catalog.getCurrentCatalog().getLoadManager();
        Long pendingNum = loadManager.getLoadJobNum(com.starrocks.load.loadv2.JobState.PENDING, dbId);
        Long runningNum = loadManager.getLoadJobNum(com.starrocks.load.loadv2.JobState.LOADING, dbId);
        Long finishedNum = loadManager.getLoadJobNum(com.starrocks.load.loadv2.JobState.FINISHED, dbId);
        Long cancelledNum = loadManager.getLoadJobNum(com.starrocks.load.loadv2.JobState.CANCELLED, dbId);
        Long totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(LOAD, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        // delete
        MaterializedViewHandler materializedViewHandler = Catalog.getCurrentCatalog().getRollupHandler();
        pendingNum = materializedViewHandler.getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.PENDING, dbId);
        runningNum =
                materializedViewHandler.getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.WAITING_TXN, dbId)
                        + materializedViewHandler
                        .getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.RUNNING, dbId);
        finishedNum =
                materializedViewHandler.getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.FINISHED, dbId);
        cancelledNum =
                materializedViewHandler.getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(ROLLUP, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        // schema change
        SchemaChangeHandler schemaChangeHandler = Catalog.getCurrentCatalog().getSchemaChangeHandler();
        pendingNum = schemaChangeHandler.getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.PENDING, dbId);
        runningNum = schemaChangeHandler.getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.WAITING_TXN, dbId)
                + schemaChangeHandler.getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.RUNNING, dbId);
        finishedNum = schemaChangeHandler.getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.FINISHED, dbId);
        cancelledNum = schemaChangeHandler.getAlterJobV2Num(com.starrocks.alter.AlterJobV2.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(SCHEMA_CHANGE, pendingNum.toString(), runningNum.toString(),
                finishedNum.toString(), cancelledNum.toString(), totalNum.toString()));

        // export
        ExportMgr exportMgr = Catalog.getCurrentCatalog().getExportMgr();
        pendingNum = exportMgr.getJobNum(ExportJob.JobState.PENDING, dbId);
        runningNum = exportMgr.getJobNum(ExportJob.JobState.EXPORTING, dbId);
        finishedNum = exportMgr.getJobNum(ExportJob.JobState.FINISHED, dbId);
        cancelledNum = exportMgr.getJobNum(ExportJob.JobState.CANCELLED, dbId);
        totalNum = pendingNum + runningNum + finishedNum + cancelledNum;
        result.addRow(Lists.newArrayList(EXPORT, pendingNum.toString(), runningNum.toString(), finishedNum.toString(),
                cancelledNum.toString(), totalNum.toString()));

        return result;
    }
}
