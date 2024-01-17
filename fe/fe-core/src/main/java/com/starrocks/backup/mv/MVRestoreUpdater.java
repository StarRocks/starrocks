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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.backup.BackupJobInfo;
import com.starrocks.backup.Status;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.privilege.PrivilegeBuiltinConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.parser.SqlParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class MVRestoreUpdater {
    private static final Logger LOG = LogManager.getLogger(MVRestoreUpdater.class);

    /**
     * Check materialized view's defined query can be analyzed successfully or not.
     * @return Pair<Status, Boolean> : status means can be analyzed successfully or not, and boolean means whether to
     *  update the old defined query or not. if it's true, mv need renew defined queries from `newDefineQueries`.
     */
    public static Pair<Status, Boolean> checkMvDefinedQuery(MaterializedView mv,
                                                            Map<TableName, TableName> remoteToLocalTableName,
                                                            Pair<String, String> newDefineQueries) {
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

        String mvName = mv.getName();
        String viewDefineSql = mv.getViewDefineSql();
        try {
            analyzeMvDefineQuery(context, viewDefineSql);
        } catch (SemanticException e) {
            String errorMsg = String.format("Can not active materialized view [%s] and try to renew it" +
                    " because analyze materialized view define sql: \n\n%s" +
                    "\n\nCause an error: %s", mvName, viewDefineSql, e.getDetailMsg());
            LOG.warn(errorMsg);

            // only update defined queries when defined query can be analyzed successfully.
            if (renewMvBaseTableNames(mv, remoteToLocalTableName, context, newDefineQueries)) {
                return Pair.create(Status.OK, true);
            }

            return Pair.create(new Status(Status.ErrCode.COMMON_ERROR, errorMsg), false);
        }
        return Pair.create(Status.OK, false);
    }

    private static QueryStatement analyzeMvDefineQuery(ConnectContext connectContext,
                                                       String defineSql) {
        // Try to parse and analyze the creation sql
        List<StatementBase> statementBaseList = SqlParser.parse(defineSql, connectContext.getSessionVariable());
        StatementBase createStmt = statementBaseList.get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(createStmt, connectContext);
        return (QueryStatement) createStmt;
    }

    @VisibleForTesting
    public static boolean renewMvBaseTableNames(MaterializedView mv,
                                                Map<TableName, TableName> remoteToLocalTableName,
                                                ConnectContext connectContext,
                                                Pair<String, String> newDefineQueries) {
        if (remoteToLocalTableName.size() != mv.getBaseTableInfos().size()) {
            return false;
        }

        // try new db/tbl name to replace old defined query and check again.
        String viewDefineSql = mv.getViewDefineSql();
        String simpleDefineSql = mv.getSimpleDefineSql();
        String oldViewDefineSql = viewDefineSql;
        String oldSimpleDefineSql = simpleDefineSql;
        String mvName = mv.getName();

        String newViewDefineDql = viewDefineSql;
        String newSimpleDefineSql = simpleDefineSql;
        for (Map.Entry<TableName, TableName> entry : remoteToLocalTableName.entrySet()) {
            TableName oldTableName = entry.getKey();
            TableName newTableName = entry.getValue();
            String oldDbTableName = String.format("`%s`.`%s`", oldTableName.getDb(), oldTableName.getTbl());
            String newDbTableName = String.format("`%s`.`%s`", newTableName.getDb(), newTableName.getTbl());

            newViewDefineDql = newViewDefineDql.replaceAll(oldDbTableName, newDbTableName);
            newSimpleDefineSql = newSimpleDefineSql.replaceAll(oldDbTableName, newDbTableName);
        }

        try {
            analyzeMvDefineQuery(connectContext, newViewDefineDql);

            // only renew defined view sql when analyze the new query success.
            LOG.info("Renew materialized view' defined sql from {} to {}", mvName, oldViewDefineSql, newViewDefineDql);
            LOG.info("Renew materialized view' simple defined sql from {} to {}", mvName, oldSimpleDefineSql, newSimpleDefineSql);
            newDefineQueries.first = newViewDefineDql;
            newDefineQueries.second = newSimpleDefineSql;
            return true;
        } catch (Exception e) {
            String errorMsg = String.format("Can not active materialized view [%s]" +
                    " because analyze materialized view define sql failed: \n\n%s" +
                    "\n\nCause an error: %s", mvName, newViewDefineDql, e.getMessage());
            LOG.warn(errorMsg);
        }
        return false;
    }

    public static boolean isCurrentRemoteMvId(MaterializedView mv,
                                              MvId mvId, Map<MvId, MvBackupInfo> mvIdTableNameMap) {
        if (!mvIdTableNameMap.containsKey(mvId)) {
            return false;
        }
        MvBackupInfo mvBackupInfo = mvIdTableNameMap.get(mvId);
        if (mvBackupInfo == null) {
            return false;
        }
        MvId localMvId = mvBackupInfo.getLocalMvId();
        if (localMvId == null) {
            return false;
        }
        // find the local mvId is same the current materialized view
        return localMvId.getId() == mv.getId() && localMvId.getDbId() == mv.getDbId();
    }

    public static Optional<MvId> restoreBaseTable(MaterializedView mv,
                                                  Database db, Table baseTable,
                                                  MvRestoreContext mvRestoreContext) {
        Map<MvId, MvBackupInfo> mvIdTableNameMap = mvRestoreContext.getMvIdToTableNameMap();
        Set<MvId> mvIds = baseTable.getRelatedMaterializedViews();
        Optional<MvId> oldMvId = mvIds.stream()
                .filter(mvId -> isCurrentRemoteMvId(mv, mvId, mvIdTableNameMap))
                .findFirst();
        if (!oldMvId.isPresent()) {
            LOG.warn(String.format("Cannot find base table info used by the table %s.%s in the materialized view %s, " +
                            "mvIds:%s", db.getFullName(), baseTable.getName(), mv.getName(),
                    mvIds.stream().map(x -> x.toString()).collect(Collectors.joining(","))));
        } else {
            LOG.info("Remove remote base table info {} used by the table {}.{} in the materialized view {}",
                    oldMvId.get(), db.getFullName(), baseTable.getName(), mv.getName());
            mvIds.remove(oldMvId.get());
        }

        // update base table's related mv's info
        mvIds.add(new MvId(db.getId(), mv.getId()));
        return oldMvId;
    }

    public static void restoreBaseTableVersionMap(
            Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap,
            Table localBaseTable, MvBaseTableBackupInfo mvBaseTableBackupInfo) {
        if (baseTableVisibleVersionMap == null) {
            return;
        }
        BackupJobInfo.BackupTableInfo backupTableInfo = mvBaseTableBackupInfo.getBackupTableInfo();
        long remoteBaseTableId = mvBaseTableBackupInfo.getRemoteTableId();
        // update version map if possible
        if (!baseTableVisibleVersionMap.containsKey(remoteBaseTableId)) {
            return;
        }

        Map<String, MaterializedView.BasePartitionInfo> versionMap = baseTableVisibleVersionMap.get(remoteBaseTableId);
        for (Map.Entry<String, BackupJobInfo.BackupPartitionInfo> e : backupTableInfo.partitions.entrySet()) {
            String partName = e.getKey();
            // update base partition info by the new partition id
            if (!versionMap.containsKey(partName)) {
                continue;
            }
            MaterializedView.BasePartitionInfo oldBasePartitionInfo = versionMap.get(partName);
            long oldPartId = e.getValue().id;
            Preconditions.checkArgument(oldPartId == oldBasePartitionInfo.getId());
            long newPartId = localBaseTable.getPartition(partName).getId();
            MaterializedView.BasePartitionInfo newBasePartitionInfo = new MaterializedView.BasePartitionInfo(newPartId,
                    oldBasePartitionInfo.getVersion(), oldBasePartitionInfo.getLastRefreshTime());
            versionMap.put(partName, newBasePartitionInfo);
        }
        baseTableVisibleVersionMap.put(localBaseTable.getId(), versionMap);
    }

    public static boolean restoreBaseTableInfoIfNoRestored(MaterializedView mv,
                                                           BaseTableInfo baseTableInfo,
                                                           List<BaseTableInfo> newBaseTableInfos) {

        String remoteDbName = baseTableInfo.getDbName();
        String remoteTableName = baseTableInfo.getTableName();
        Database baseTableDb = GlobalStateMgr.getCurrentState().getDb(remoteDbName);
        if (baseTableDb == null) {
            LOG.warn(String.format("Materialized view %s can not find old base table's db name:%s.%s",
                    mv.getName(), remoteDbName, remoteTableName));
            return false;
        }
        Table baseTable = baseTableDb.getTable(remoteTableName);
        if (baseTable == null) {
            LOG.warn(String.format("Materialized view %s can not find old base table:%s.%s",
                    mv.getName(), remoteDbName, remoteTableName));
            return false;
        }
        // use baseTable's db instead of mv's db to construct baseTableInfo.
        BaseTableInfo newBaseTableInfo = new BaseTableInfo(baseTableDb.getId(), baseTableDb.getFullName(),
                baseTable.getName(), baseTable.getId());
        newBaseTableInfos.add(newBaseTableInfo);
        return true;
    }

    public static Pair<Boolean, Optional<MvId>> restoreBaseTableInfoIfRestored(
            Database restoreDb,
            MvRestoreContext mvRestoreContext,
            MaterializedView mv,
            MvBaseTableBackupInfo mvBaseTableBackupInfo,
            BaseTableInfo baseTableInfo,
            Map<TableName, TableName> remoteToLocalTableName,
            List<BaseTableInfo> newBaseTableInfos) {
        String remoteDbName = baseTableInfo.getDbName();
        String remoteTableName = baseTableInfo.getTableName();
        TableName remoteDbTblName = new TableName(remoteDbName, remoteTableName);
        if (mvBaseTableBackupInfo == null) {
            LOG.warn("Materialized view {} can not find old base table name:{} because " +
                            "mvBaseTableBackupInfo is null",
                    mv.getName(), remoteTableName);
            return Pair.create(false, Optional.empty());
        }

        String localTableName = mvBaseTableBackupInfo.getLocalTableName();
        Table localTable = restoreDb.getTable(localTableName);
        remoteToLocalTableName.put(remoteDbTblName, new TableName(restoreDb.getFullName(), localTableName));
        if (localTable == null) {
            LOG.warn("Materialized view {} can not find the base table {}, old base table name:{}",
                    mv.getName(), localTableName, remoteTableName);
            return Pair.create(false, Optional.empty());
        }

        // restore materialized view's associated base table's mvIds.
        Optional<MvId> oldMvIdOpt = restoreBaseTable(mv, restoreDb, localTable, mvRestoreContext);
        // restore materialized view's version map if base table is also backed up and restore.
        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableVisibleVersionMap();
        restoreBaseTableVersionMap(baseTableVisibleVersionMap, localTable, mvBaseTableBackupInfo);

        // update base table info since materialized view's db or base table info may be changed.
        BaseTableInfo newBaseTableInfo = new BaseTableInfo(restoreDb.getId(), restoreDb.getFullName(), localTableName,
                localTable.getId());
        newBaseTableInfos.add(newBaseTableInfo);
        return Pair.create(true, oldMvIdOpt);
    }
}
