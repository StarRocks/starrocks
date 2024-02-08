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

package com.starrocks.backup.mv;

import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.backup.AbstractJob;
import com.starrocks.backup.BackupJobInfo;
import com.starrocks.backup.BackupMeta;
import com.starrocks.backup.RestoreJob;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * MvRestoreContext is used in Restore job to track materialized view's associated infos. eg, we can use it to track
 * remote base table which is defined in the materialized view's `baseTableInfos` and track the materialized view by
 * remote MvId.
 */
public class MvRestoreContext {
    private static final Logger LOG = LogManager.getLogger(MvRestoreContext.class);

    // <remote db name, remote table name> -> BackupTableInfo and associated infos.
    // Because materialized view only keeps remote db/table names in the BaseTable info, and materialized view and its
    // base tables may not in the same backup/restore. We keep this to track remote backup table info by the remote db
    // and table name, so can update materialized view's base table info in restoring the materialized view.
    // NOTE: we only record the newest restore of the base table to avoid caching too much history metas.
    // NOTE: we won't cache all the
    protected Map<TableName, MvBaseTableBackupInfo> mvBaseTableToBackupTableInfo = Maps.newConcurrentMap();

    // <MvId> -> <db name, table name>
    // MvId only contains remote db id and remote table id, but remote db/table id are changed after restore, so use this
    // to track the new db name/table name after restore.
    protected Map<MvId, MvBackupInfo> mvIdToTableNameMap = Maps.newConcurrentMap();

    public MvRestoreContext() {

    }

    public Map<TableName, MvBaseTableBackupInfo> getMvBaseTableToBackupTableInfo() {
        return mvBaseTableToBackupTableInfo;
    }

    public Map<MvId, MvBackupInfo> getMvIdToTableNameMap() {
        return mvIdToTableNameMap;
    }

    public void addIntoMvBaseTableBackupInfoIfNeeded(Table remoteTbl,
                                                     BackupJobInfo jobInfo,
                                                     BackupJobInfo.BackupTableInfo backupTableInfo) {
        if (remoteTbl == null) {
            return;
        }

        // put it into mvId->table name map
        if (remoteTbl.isMaterializedView()) {
            MvId mvId = new MvId(jobInfo.dbId, remoteTbl.getId());
            mvIdToTableNameMap.put(mvId, new MvBackupInfo(jobInfo.dbName, remoteTbl.getName()));
        }

        // put it into mvBaseTableToBackupTableInfo
        if (remoteTbl.getRelatedMaterializedViews() == null || remoteTbl.getRelatedMaterializedViews().isEmpty()) {
            return;
        }
        String localDbName = jobInfo.dbName;
        String localTableName = jobInfo.getAliasByOriginNameIfSet(remoteTbl.getName());
        MvBaseTableBackupInfo mvBaseTableBackupInfo =
                new MvBaseTableBackupInfo(backupTableInfo, localDbName, localTableName, remoteTbl.getId(), jobInfo.backupTime);
        mvBaseTableToBackupTableInfo.put(new TableName(jobInfo.dbName, remoteTbl.getName()), mvBaseTableBackupInfo);
    }

    public void addIntoMvBaseTableBackupInfo(AbstractJob job) {
        if (job == null || !(job instanceof RestoreJob)) {
            return;
        }
        RestoreJob restoreJob = (RestoreJob) job;
        BackupJobInfo jobInfo = restoreJob.getJobInfo();
        BackupMeta backupMeta = restoreJob.getBackupMeta();
        if (backupMeta == null) {
            return;
        }
        for (BackupJobInfo.BackupTableInfo tblInfo : jobInfo.tables.values()) {
            Table remoteTbl = backupMeta.getTable(tblInfo.name);
            addIntoMvBaseTableBackupInfoIfNeeded(remoteTbl, jobInfo, tblInfo);
        }
    }

    public void discardExpiredBackupTableInfo(AbstractJob job) {
        if (!(job instanceof RestoreJob)) {
            return;
        }
        RestoreJob restoreJob = (RestoreJob) job;
        BackupJobInfo backupJobInfo = restoreJob.getJobInfo();
        // discard expired restore db and name backup infos.
        for (Map.Entry<String, BackupJobInfo.BackupTableInfo> e : backupJobInfo.tables.entrySet()) {
            TableName dbTblName = new TableName(backupJobInfo.dbName, e.getKey());
            if (mvBaseTableToBackupTableInfo.containsKey(dbTblName)) {
                MvBaseTableBackupInfo cachedBackupTableInfo =
                        mvBaseTableToBackupTableInfo.get(dbTblName);
                if (cachedBackupTableInfo.getBackupTime() == backupJobInfo.backupTime) {
                    LOG.warn("discard expired mvBaseTableToBackupTableInfo, baseTable:{}", dbTblName);
                    mvBaseTableToBackupTableInfo.remove(dbTblName);
                }
            }

            BackupMeta backupMeta = restoreJob.getBackupMeta();
            Table remoteTable = backupMeta.getTable(e.getKey());
            if (remoteTable == null || !remoteTable.isMaterializedView()) {
                continue;
            }
            MvId mvId = new MvId(backupJobInfo.dbId, remoteTable.getId());
            if (mvIdToTableNameMap.containsKey(mvId)) {
                LOG.warn("discard expired mvIdToTableNameMap, mvId:{}", mvId);
                mvIdToTableNameMap.remove(mvId);
            }
        }
    }
}
