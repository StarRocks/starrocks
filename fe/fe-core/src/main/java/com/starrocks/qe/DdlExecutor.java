// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/qe/DdlExecutor.java

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

package com.starrocks.qe;

import com.starrocks.analysis.AdminCancelRepairTableStmt;
import com.starrocks.analysis.AdminCheckTabletsStmt;
import com.starrocks.analysis.AdminRepairTableStmt;
import com.starrocks.analysis.AdminSetConfigStmt;
import com.starrocks.analysis.AdminSetReplicaStatusStmt;
import com.starrocks.analysis.AlterResourceStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.CancelExportStmt;
import com.starrocks.analysis.CreateFileStmt;
import com.starrocks.analysis.CreateResourceStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropFileStmt;
import com.starrocks.analysis.DropResourceStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.analysis.InstallPluginStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.DropAnalyzeJobStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;

public class DdlExecutor {
    public static ShowResultSet execute(GlobalStateMgr globalStateMgr, DdlStmt ddlStmt) throws Exception {
        if (ddlStmt instanceof AdminRepairTableStmt) {
            globalStateMgr.getTabletChecker().repairTable((AdminRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCancelRepairTableStmt) {
            globalStateMgr.getTabletChecker().cancelRepairTable((AdminCancelRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetConfigStmt) {
            globalStateMgr.setConfig((AdminSetConfigStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateFileStmt) {
            globalStateMgr.getSmallFileMgr().createFile((CreateFileStmt) ddlStmt);
        } else if (ddlStmt instanceof DropFileStmt) {
            globalStateMgr.getSmallFileMgr().dropFile((DropFileStmt) ddlStmt);
        } else if (ddlStmt instanceof InstallPluginStmt) {
            globalStateMgr.installPlugin((InstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof UninstallPluginStmt) {
            globalStateMgr.uninstallPlugin((UninstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCheckTabletsStmt) {
            globalStateMgr.checkTablets((AdminCheckTabletsStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetReplicaStatusStmt) {
            globalStateMgr.setReplicaStatus((AdminSetReplicaStatusStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateResourceStmt) {
            globalStateMgr.getResourceMgr().createResource((CreateResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof DropResourceStmt) {
            globalStateMgr.getResourceMgr().dropResource((DropResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterResourceStmt) {
            globalStateMgr.getResourceMgr().alterResource((AlterResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelExportStmt) {
            globalStateMgr.getExportMgr().cancelExportJob((CancelExportStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateAnalyzeJobStmt) {
            globalStateMgr.getAnalyzeManager().addAnalyzeJob(((CreateAnalyzeJobStmt) ddlStmt).toAnalyzeJob());
        } else if (ddlStmt instanceof DropAnalyzeJobStmt) {
            globalStateMgr.getAnalyzeManager().removeAnalyzeJob(((DropAnalyzeJobStmt) ddlStmt).getId());
        } else if (ddlStmt instanceof RefreshTableStmt) {
            globalStateMgr.refreshExternalTable((RefreshTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateWorkGroupStmt) {
            globalStateMgr.getWorkGroupMgr().createWorkGroup((CreateWorkGroupStmt) ddlStmt);
        } else if (ddlStmt instanceof DropWorkGroupStmt) {
            globalStateMgr.getWorkGroupMgr().dropWorkGroup((DropWorkGroupStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterWorkGroupStmt) {
            globalStateMgr.getWorkGroupMgr().alterWorkGroup((AlterWorkGroupStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateCatalogStmt) {
            globalStateMgr.getCatalogMgr().createCatalog((CreateCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof DropCatalogStmt) {
            globalStateMgr.getCatalogMgr().dropCatalog((DropCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof SubmitTaskStmt) {
            return globalStateMgr.getTaskManager().handleSubmitTaskStmt((SubmitTaskStmt) ddlStmt);
        } else {
            throw new DdlException("Unknown statement.");
        }
        return null;
    }
}
