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
package com.starrocks.sql.automv.util;

import com.google.common.collect.Sets;
import com.starrocks.analysis.TableName;
import com.starrocks.authentication.AuthenticationMgr;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.sql.automv.qe.CustomizedQueryExecutor;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class MetaUtil {
    private static final Logger LOG = LogManager.getLogger(MetaUtil.class);

    public static Result<Optional<Catalog>> getCatalog(String catalogName) {
        if (CatalogMgr.isInternalCatalog(catalogName)) {
            return Result.wrap(() -> Optional.empty());
        }
        return Result.wrap(() -> {
            Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
            if (catalog == null) {
                throw new RuntimeException(String.format("Catalog %s is absent", catalogName));
            }
            return Optional.of(catalog);
        }).ifError(err -> LOG.error(err.getMessage(), err));
    }

    public static Result<Database> getDatabase(TableName tableName) {
        String catalogName = CatalogMgr.isInternalCatalog(tableName.getCatalog()) ?
                InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME :
                tableName.getCatalog();
        String dbName = Objects.requireNonNull(tableName.getDb());
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();

        return Result.wrap(() -> {
            Database db = metadataMgr.getDb(catalogName, dbName);
            if (db == null) {
                throw new RuntimeException(String.format("Database %s.%s is absent", catalogName, dbName));
            }
            return db;
        }).ifError(err -> LOG.error(err.getMessage(), err));
    }

    public static <T> Result<T> criticalRegion(Database db, List<Long> tableIdList, LockType lockType,
                                               Result.ThrowableSupplier<T> codeBlock) {
        Locker locker = new Locker();
        locker.lockTablesWithIntensiveDbLock(db.getId(), tableIdList, lockType);
        try {
            return Result.wrap(codeBlock);
        } finally {
            locker.unLockTablesWithIntensiveDbLock(db.getId(), tableIdList, lockType);
        }
    }

    public static <T> Result<T> criticalRegion(Database db, Table table, LockType lockType,
                                               Result.ThrowableSupplier<T> codeBlock) {
        return criticalRegion(db, Arrays.asList(table.getId()), lockType, codeBlock);
    }

    public static ConnectContext createConnectContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        ctx.setQualifiedUser(AuthenticationMgr.ROOT_USER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));
        ctx.setSessionVariable(new SessionVariable());
        ctx.setThreadLocalInfo();
        return ctx;
    }

    public static void dropMV(String fqMvName) {
        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        Optional<ConnectContext> savedConnectContext = Optional.ofNullable(ConnectContext.get());
        ConnectContext connectContext = savedConnectContext.orElseGet(MetaUtil::createConnectContext);
        Result.wrap(() -> {
            executor.exec(connectContext, "DROP MATERIALIZED VIEW IF EXISTS " + fqMvName);
        }).ifError(err -> LOG.error("Fail to drop materialized view {}", fqMvName, err));
        if (!savedConnectContext.isPresent()) {
            connectContext.cleanup();
        }
    }

    public static void dropDb(String db) {
        CustomizedQueryExecutor executor = new CustomizedQueryExecutor();
        Optional<ConnectContext> savedConnectContext = Optional.ofNullable(ConnectContext.get());
        ConnectContext connectContext = savedConnectContext.orElseGet(MetaUtil::createConnectContext);
        Result.wrap(() -> {
            executor.exec(connectContext, "DROP DATABASE IF EXISTS " + db);
        }).ifError(err -> LOG.error("Fail to drop database {}", db, err));
        if (!savedConnectContext.isPresent()) {
            connectContext.cleanup();
        }
    }

    public static boolean hasData(TableName tableName) {
        return checkTable(tableName, false,
                (db, tbl) -> MetaUtil.criticalRegion(db, tbl, LockType.READ, () ->
                                tbl.getPartitions().stream().anyMatch(Partition::hasData))
                        .unwrap().orElse(false));
    }

    public static boolean exists(TableName tableName) {
        return checkTable(tableName, false, (db, tbl) -> true);
    }

    public static <T> T checkTable(TableName tableName, T defaultValue, BiFunction<Database, Table, T> checker) {
        return MetaUtil.getDatabase(tableName)
                .unwrap()
                .map(db -> GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .mayGetTable(db.getFullName(), tableName.getTbl())
                        .map(tbl -> checker.apply(db, tbl))
                        .orElse(defaultValue))
                .orElse(defaultValue);
    }

    public static List<MaterializedViewPlus> listLegacyMVs(String catalogName, String dbName) {
        catalogName = Optional.ofNullable(catalogName).orElse(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, dbName);
        if (db == null) {
            LOG.error("Database '{}.{}' is absent", catalogName, dbName);
            return Collections.emptyList();
        }
        return db.getMaterializedViews()
                .stream()
                .map(mv -> MaterializedViewPlus.of(mv,
                        new TableName(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName, mv.getName())))
                .collect(Collectors.toList());
    }

    public static Optional<String> getResourceName(Table table) {
        if (table.isHiveTable()) {
            HiveTable hiveTable = (HiveTable) table;
            return Optional.ofNullable(hiveTable.getResourceName());
        } else if (table.isOlapTable()) {
            OlapTable olapTable = (OlapTable) table;
            String resource = olapTable.getTableProperty().getProperties().get("resource");
            return Optional.ofNullable(resource);
        } else {
            return Optional.empty();
        }
    }
}
