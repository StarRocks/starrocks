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
import com.starrocks.analysis.AlterDatabaseQuotaStmt;
import com.starrocks.analysis.AlterDatabaseRename;
import com.starrocks.analysis.AlterResourceStmt;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterUserStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.CancelAlterSystemStmt;
import com.starrocks.analysis.CancelBackupStmt;
import com.starrocks.analysis.CancelExportStmt;
import com.starrocks.analysis.CreateFileStmt;
import com.starrocks.analysis.CreateRepositoryStmt;
import com.starrocks.analysis.CreateResourceStmt;
import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropFileStmt;
import com.starrocks.analysis.DropRepositoryStmt;
import com.starrocks.analysis.DropResourceStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.analysis.InstallPluginStmt;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverPartitionStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.RevokeStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.SyncStmt;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.CreateAnalyzeJobStmt;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.DropAnalyzeJobStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.GrantImpersonateStmt;
import com.starrocks.sql.ast.GrantRoleStmt;
import com.starrocks.sql.ast.RefreshTableStmt;
import com.starrocks.sql.ast.RevokeImpersonateStmt;
import com.starrocks.sql.ast.RevokeRoleStmt;
import com.starrocks.sql.ast.SubmitTaskStmt;

public class DdlExecutor {
    public static ShowResultSet execute(GlobalStateMgr globalStateMgr, DdlStmt ddlStmt) throws Exception {
        if (ddlStmt instanceof CreateUserStmt) {
            CreateUserStmt stmt = (CreateUserStmt) ddlStmt;
            globalStateMgr.getAuth().createUser(stmt);
        } else if (ddlStmt instanceof AlterUserStmt) {
            AlterUserStmt stmt = (AlterUserStmt) ddlStmt;
            globalStateMgr.getAuth().alterUser(stmt);
        } else if (ddlStmt instanceof DropUserStmt) {
            DropUserStmt stmt = (DropUserStmt) ddlStmt;
            globalStateMgr.getAuth().dropUser(stmt);
        } else if (ddlStmt instanceof RevokeRoleStmt) {
            RevokeRoleStmt stmt = (RevokeRoleStmt) ddlStmt;
            globalStateMgr.getAuth().revokeRole(stmt);
        } else if (ddlStmt instanceof GrantRoleStmt) {
            GrantRoleStmt stmt = (GrantRoleStmt) ddlStmt;
            globalStateMgr.getAuth().grantRole(stmt);
        } else if (ddlStmt instanceof GrantStmt) {
            GrantStmt stmt = (GrantStmt) ddlStmt;
            globalStateMgr.getAuth().grant(stmt);
        } else if (ddlStmt instanceof GrantImpersonateStmt) {
            GrantImpersonateStmt stmt = (GrantImpersonateStmt) ddlStmt;
            globalStateMgr.getAuth().grantImpersonate(stmt);
        } else if (ddlStmt instanceof RevokeStmt) {
            RevokeStmt stmt = (RevokeStmt) ddlStmt;
            globalStateMgr.getAuth().revoke(stmt);
        } else if (ddlStmt instanceof RevokeImpersonateStmt) {
            RevokeImpersonateStmt stmt = (RevokeImpersonateStmt) ddlStmt;
            globalStateMgr.getAuth().revokeImpersonate(stmt);
        } else if (ddlStmt instanceof CreateRoleStmt) {
            globalStateMgr.getAuth().createRole((CreateRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRoleStmt) {
            globalStateMgr.getAuth().dropRole((DropRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof SetUserPropertyStmt) {
            globalStateMgr.getAuth().updateUserProperty((SetUserPropertyStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterSystemStmt) {
            AlterSystemStmt stmt = (AlterSystemStmt) ddlStmt;
            return globalStateMgr.alterCluster(stmt);
        } else if (ddlStmt instanceof CancelAlterSystemStmt) {
            CancelAlterSystemStmt stmt = (CancelAlterSystemStmt) ddlStmt;
            globalStateMgr.cancelAlterCluster(stmt);
        } else if (ddlStmt instanceof AlterDatabaseQuotaStmt) {
            globalStateMgr.alterDatabaseQuota((AlterDatabaseQuotaStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterDatabaseRename) {
            globalStateMgr.renameDatabase((AlterDatabaseRename) ddlStmt);
        } else if (ddlStmt instanceof RecoverDbStmt) {
            globalStateMgr.recoverDatabase((RecoverDbStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverTableStmt) {
            globalStateMgr.recoverTable((RecoverTableStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverPartitionStmt) {
            globalStateMgr.recoverPartition((RecoverPartitionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateViewStmt) {
            globalStateMgr.createView((CreateViewStmt) ddlStmt);
        } else if (ddlStmt instanceof BackupStmt) {
            globalStateMgr.backup((BackupStmt) ddlStmt);
        } else if (ddlStmt instanceof RestoreStmt) {
            globalStateMgr.restore((RestoreStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelBackupStmt) {
            globalStateMgr.cancelBackup((CancelBackupStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateRepositoryStmt) {
            globalStateMgr.getBackupHandler().createRepository((CreateRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRepositoryStmt) {
            globalStateMgr.getBackupHandler().dropRepository((DropRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof SyncStmt) {
            return null;
        } else if (ddlStmt instanceof TruncateTableStmt) {
            globalStateMgr.truncateTable((TruncateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminRepairTableStmt) {
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
