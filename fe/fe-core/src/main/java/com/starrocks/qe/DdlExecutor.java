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
import com.starrocks.analysis.AlterRoutineLoadStmt;
import com.starrocks.analysis.AlterSystemStmt;
import com.starrocks.analysis.AlterTableStmt;
import com.starrocks.analysis.AlterUserStmt;
import com.starrocks.analysis.AlterViewStmt;
import com.starrocks.analysis.AlterWorkGroupStmt;
import com.starrocks.analysis.BackupStmt;
import com.starrocks.analysis.CancelAlterSystemStmt;
import com.starrocks.analysis.CancelAlterTableStmt;
import com.starrocks.analysis.CancelBackupStmt;
import com.starrocks.analysis.CancelExportStmt;
import com.starrocks.analysis.CancelLoadStmt;
import com.starrocks.analysis.CreateAnalyzeJobStmt;
import com.starrocks.analysis.CreateDbStmt;
import com.starrocks.analysis.CreateFileStmt;
import com.starrocks.analysis.CreateFunctionStmt;
import com.starrocks.analysis.CreateMaterializedViewStmt;
import com.starrocks.analysis.CreateRepositoryStmt;
import com.starrocks.analysis.CreateResourceStmt;
import com.starrocks.analysis.CreateRoleStmt;
import com.starrocks.analysis.CreateRoutineLoadStmt;
import com.starrocks.analysis.CreateTableLikeStmt;
import com.starrocks.analysis.CreateTableStmt;
import com.starrocks.analysis.CreateUserStmt;
import com.starrocks.analysis.CreateViewStmt;
import com.starrocks.analysis.CreateWorkGroupStmt;
import com.starrocks.analysis.DdlStmt;
import com.starrocks.analysis.DropAnalyzeJobStmt;
import com.starrocks.analysis.DropDbStmt;
import com.starrocks.analysis.DropFileStmt;
import com.starrocks.analysis.DropFunctionStmt;
import com.starrocks.analysis.DropMaterializedViewStmt;
import com.starrocks.analysis.DropRepositoryStmt;
import com.starrocks.analysis.DropResourceStmt;
import com.starrocks.analysis.DropRoleStmt;
import com.starrocks.analysis.DropTableStmt;
import com.starrocks.analysis.DropUserStmt;
import com.starrocks.analysis.DropWorkGroupStmt;
import com.starrocks.analysis.GrantStmt;
import com.starrocks.analysis.InstallPluginStmt;
import com.starrocks.analysis.LoadStmt;
import com.starrocks.analysis.PauseRoutineLoadStmt;
import com.starrocks.analysis.RecoverDbStmt;
import com.starrocks.analysis.RecoverPartitionStmt;
import com.starrocks.analysis.RecoverTableStmt;
import com.starrocks.analysis.RefreshExternalTableStmt;
import com.starrocks.analysis.RestoreStmt;
import com.starrocks.analysis.ResumeRoutineLoadStmt;
import com.starrocks.analysis.RevokeStmt;
import com.starrocks.analysis.SetUserPropertyStmt;
import com.starrocks.analysis.StopRoutineLoadStmt;
import com.starrocks.analysis.SyncStmt;
import com.starrocks.analysis.TruncateTableStmt;
import com.starrocks.analysis.UninstallPluginStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.load.EtlJobType;

public class DdlExecutor {
    public static void execute(Catalog catalog, DdlStmt ddlStmt) throws Exception {
        if (ddlStmt instanceof CreateDbStmt) {
            catalog.createDb((CreateDbStmt) ddlStmt);
        } else if (ddlStmt instanceof DropDbStmt) {
            catalog.dropDb((DropDbStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateFunctionStmt) {
            catalog.createFunction((CreateFunctionStmt) ddlStmt);
        } else if (ddlStmt instanceof DropFunctionStmt) {
            catalog.dropFunction((DropFunctionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableStmt) {
            catalog.createTable((CreateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableLikeStmt) {
            catalog.createTableLike((CreateTableLikeStmt) ddlStmt);
        } else if (ddlStmt instanceof DropTableStmt) {
            catalog.dropTable((DropTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateMaterializedViewStmt) {
            catalog.createMaterializedView((CreateMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof DropMaterializedViewStmt) {
            catalog.dropMaterializedView((DropMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterTableStmt) {
            catalog.alterTable((AlterTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterViewStmt) {
            catalog.alterView((AlterViewStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelAlterTableStmt) {
            catalog.cancelAlter((CancelAlterTableStmt) ddlStmt);
        } else if (ddlStmt instanceof LoadStmt) {
            LoadStmt loadStmt = (LoadStmt) ddlStmt;
            EtlJobType jobType = loadStmt.getEtlJobType();
            if (jobType == EtlJobType.UNKNOWN) {
                throw new DdlException("Unknown load job type");
            }
            if (jobType == EtlJobType.HADOOP && Config.disable_hadoop_load) {
                throw new DdlException("Load job by hadoop cluster is disabled."
                        + " Try using broker load. See 'help broker load;'");
            }

            catalog.getLoadManager().createLoadJobFromStmt(loadStmt);
        } else if (ddlStmt instanceof CancelLoadStmt) {
            catalog.getLoadManager().cancelLoadJob((CancelLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateRoutineLoadStmt) {
            catalog.getRoutineLoadManager().createRoutineLoadJob((CreateRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof PauseRoutineLoadStmt) {
            catalog.getRoutineLoadManager().pauseRoutineLoadJob((PauseRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof ResumeRoutineLoadStmt) {
            catalog.getRoutineLoadManager().resumeRoutineLoadJob((ResumeRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof StopRoutineLoadStmt) {
            catalog.getRoutineLoadManager().stopRoutineLoadJob((StopRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterRoutineLoadStmt) {
            catalog.getRoutineLoadManager().alterRoutineLoadJob((AlterRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateUserStmt) {
            CreateUserStmt stmt = (CreateUserStmt) ddlStmt;
            catalog.getAuth().createUser(stmt);
        } else if (ddlStmt instanceof AlterUserStmt) {
            AlterUserStmt stmt = (AlterUserStmt) ddlStmt;
            catalog.getAuth().alterUser(stmt);
        } else if (ddlStmt instanceof DropUserStmt) {
            DropUserStmt stmt = (DropUserStmt) ddlStmt;
            catalog.getAuth().dropUser(stmt);
        } else if (ddlStmt instanceof GrantStmt) {
            GrantStmt stmt = (GrantStmt) ddlStmt;
            catalog.getAuth().grant(stmt);
        } else if (ddlStmt instanceof RevokeStmt) {
            RevokeStmt stmt = (RevokeStmt) ddlStmt;
            catalog.getAuth().revoke(stmt);
        } else if (ddlStmt instanceof CreateRoleStmt) {
            catalog.getAuth().createRole((CreateRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRoleStmt) {
            catalog.getAuth().dropRole((DropRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof SetUserPropertyStmt) {
            catalog.getAuth().updateUserProperty((SetUserPropertyStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterSystemStmt) {
            AlterSystemStmt stmt = (AlterSystemStmt) ddlStmt;
            catalog.alterCluster(stmt);
        } else if (ddlStmt instanceof CancelAlterSystemStmt) {
            CancelAlterSystemStmt stmt = (CancelAlterSystemStmt) ddlStmt;
            catalog.cancelAlterCluster(stmt);
        } else if (ddlStmt instanceof AlterDatabaseQuotaStmt) {
            catalog.alterDatabaseQuota((AlterDatabaseQuotaStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterDatabaseRename) {
            catalog.renameDatabase((AlterDatabaseRename) ddlStmt);
        } else if (ddlStmt instanceof RecoverDbStmt) {
            catalog.recoverDatabase((RecoverDbStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverTableStmt) {
            catalog.recoverTable((RecoverTableStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverPartitionStmt) {
            catalog.recoverPartition((RecoverPartitionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateViewStmt) {
            catalog.createView((CreateViewStmt) ddlStmt);
        } else if (ddlStmt instanceof BackupStmt) {
            catalog.backup((BackupStmt) ddlStmt);
        } else if (ddlStmt instanceof RestoreStmt) {
            catalog.restore((RestoreStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelBackupStmt) {
            catalog.cancelBackup((CancelBackupStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateRepositoryStmt) {
            catalog.getBackupHandler().createRepository((CreateRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRepositoryStmt) {
            catalog.getBackupHandler().dropRepository((DropRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof SyncStmt) {
            return;
        } else if (ddlStmt instanceof TruncateTableStmt) {
            catalog.truncateTable((TruncateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminRepairTableStmt) {
            catalog.getTabletChecker().repairTable((AdminRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCancelRepairTableStmt) {
            catalog.getTabletChecker().cancelRepairTable((AdminCancelRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetConfigStmt) {
            catalog.setConfig((AdminSetConfigStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateFileStmt) {
            catalog.getSmallFileMgr().createFile((CreateFileStmt) ddlStmt);
        } else if (ddlStmt instanceof DropFileStmt) {
            catalog.getSmallFileMgr().dropFile((DropFileStmt) ddlStmt);
        } else if (ddlStmt instanceof InstallPluginStmt) {
            catalog.installPlugin((InstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof UninstallPluginStmt) {
            catalog.uninstallPlugin((UninstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCheckTabletsStmt) {
            catalog.checkTablets((AdminCheckTabletsStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetReplicaStatusStmt) {
            catalog.setReplicaStatus((AdminSetReplicaStatusStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateResourceStmt) {
            catalog.getResourceMgr().createResource((CreateResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof DropResourceStmt) {
            catalog.getResourceMgr().dropResource((DropResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterResourceStmt) {
            catalog.getResourceMgr().alterResource((AlterResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelExportStmt) {
            catalog.getExportMgr().cancelExportJob((CancelExportStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateAnalyzeJobStmt) {
            catalog.getAnalyzeManager().addAnalyzeJob(((CreateAnalyzeJobStmt) ddlStmt).toAnalyzeJob());
        } else if (ddlStmt instanceof DropAnalyzeJobStmt) {
            catalog.getAnalyzeManager().removeAnalyzeJob(((DropAnalyzeJobStmt) ddlStmt).getId());
        } else if (ddlStmt instanceof RefreshExternalTableStmt) {
            catalog.refreshExternalTable((RefreshExternalTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateWorkGroupStmt) {
            catalog.getWorkGroupMgr().createWorkGroup((CreateWorkGroupStmt) ddlStmt);
        } else if (ddlStmt instanceof DropWorkGroupStmt) {
            catalog.getWorkGroupMgr().dropWorkGroup((DropWorkGroupStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterWorkGroupStmt) {
            catalog.getWorkGroupMgr().alterWorkGroup((AlterWorkGroupStmt) ddlStmt);
        } else {
            throw new DdlException("Unknown statement.");
        }
    }
}
