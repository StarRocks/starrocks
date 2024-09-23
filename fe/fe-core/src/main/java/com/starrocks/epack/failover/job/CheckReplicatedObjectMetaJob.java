// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.failover.ReplicatedObjectMeta;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class CheckReplicatedObjectMetaJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(CheckReplicatedObjectMetaJob.class);

    public CheckReplicatedObjectMetaJob(FailoverGroup failoverGroup) {
        super(failoverGroup);
    }

    @Override
    public void execute() {
        checkReplicatedCatalogs();
        checkReplicatedDatabases();
        checkReplicatedTables();
    }

    private void checkReplicatedCatalogs() {
        for (ReplicatedObjectMeta.CatalogMeta catalogMeta : failoverGroup.getObjectMeta().getCatalogMetas().values()) {
            if (!catalogMeta.isInternalCatalog()) {
                LOG.warn("Ignore remote external catalog {}", catalogMeta.getCatalog().getName());
                continue;
            }

            if (failoverGroup.getExcludeMgr().isExcludeCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
                LOG.warn("Ignore remote exclude catalog {}", InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
                continue;
            }

            Set<String> databaseNames = Sets.newHashSet();
            for (Database remoteDatabase : catalogMeta.getDatabases().values()) {
                if (failoverGroup.getExcludeMgr().isExcludeDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        remoteDatabase.getFullName())) {
                    LOG.warn("Ignore remote exclude database {}", remoteDatabase.getFullName());
                    continue;
                }

                CheckReplicatedDatabaseJob job = new CheckReplicatedDatabaseJob(failoverGroup,
                        remoteDatabase, false);
                job.start();

                databaseNames.add(remoteDatabase.getFullName());
            }

            failoverGroup.getIncludeMgr().addIncludeCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID);

            if (!Config.failover_group_allow_drop_extra_table) {
                continue;
            }

            // Drop extra databases in catalog
            for (Database localDatabase : failoverGroup.getIncludeMgr().getIncludeCatalogs().get(null).values()) {
                if (failoverGroup.getExcludeMgr().isExcludeDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        localDatabase.getFullName())) {
                    continue;
                }
                if (!databaseNames.contains(localDatabase.getFullName())) {
                    DropReplicatedDatabaseJob job = new DropReplicatedDatabaseJob(failoverGroup, null, null,
                            localDatabase, false, false);
                    job.start();
                }
            }
        }

        // Remove deleted catalog
        if (failoverGroup.getObjectMeta().getCatalogMetas().isEmpty()
                && !failoverGroup.getIncludeMgr().getIncludeCatalogs().isEmpty()) {
            failoverGroup.getIncludeMgr().removeIncludeCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID);
        }
    }

    private void checkReplicatedDatabases() {
        Set<String> databaseNames = Sets.newHashSet();
        for (ReplicatedObjectMeta.DatabaseMeta databaseMeta : failoverGroup.getObjectMeta().getDatabaseMetas()
                .values()) {
            if (!databaseMeta.isInternalCatalog()) {
                LOG.warn("Ignore remote external catalog {}", databaseMeta.getCatalog().getName());
                continue;
            }

            if (failoverGroup.getExcludeMgr().isExcludeDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    databaseMeta.getDatabase().getFullName())) {
                LOG.warn("Ignore remote exclude database {}", databaseMeta.getDatabase().getFullName());
                continue;
            }

            CheckReplicatedDatabaseJob job = new CheckReplicatedDatabaseJob(failoverGroup,
                    databaseMeta.getDatabase(), true);
            job.start();

            databaseNames.add(databaseMeta.getDatabase().getFullName());
        }

        if (!Config.failover_group_allow_drop_extra_table) {
            return;
        }

        // Drop extra databases
        for (Database localDatabase : failoverGroup.getIncludeMgr().getIncludeDatabases().values()) {
            if (failoverGroup.getExcludeMgr().isExcludeDatabase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    localDatabase.getFullName())) {
                continue;
            }
            if (!databaseNames.contains(localDatabase.getFullName())) {
                DropReplicatedDatabaseJob job = new DropReplicatedDatabaseJob(failoverGroup, null, null,
                        localDatabase, true, false);
                job.start();
            }
        }
    }

    private void checkReplicatedTables() {
        Map<Database, List<Table>> databaseToTables = Maps.newHashMap();
        Map<String, Set<String>> databaseToTableNames = Maps.newHashMap();
        for (ReplicatedObjectMeta.TableMeta tableMeta : failoverGroup.getObjectMeta().getTableMetas().values()) {
            if (!tableMeta.isInternalCatalog()) {
                LOG.warn("Ignore remote external catalog {}", tableMeta.getCatalog().getName());
                continue;
            }

            if (failoverGroup.getExcludeMgr().isExcludeTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                    tableMeta.getDatabase().getFullName(), tableMeta.getTable().getName())) {
                LOG.warn("Ignore remote exclude table {}.{}", tableMeta.getDatabase().getFullName(),
                        tableMeta.getTable().getName());
                continue;
            }

            databaseToTables.computeIfAbsent(tableMeta.getDatabase(), key -> Lists.newArrayList())
                    .add(tableMeta.getTable());
            databaseToTableNames.computeIfAbsent(tableMeta.getDatabase().getFullName(), key -> Sets.newHashSet())
                    .add(tableMeta.getTable().getName());
        }

        for (Map.Entry<Database, List<Table>> entry : databaseToTables.entrySet()) {
            CheckReplicatedDatabaseJob job = new CheckReplicatedDatabaseJob(failoverGroup,
                    entry.getKey(), entry.getValue(), false);
            job.start();
        }

        if (!Config.failover_group_allow_drop_extra_table) {
            return;
        }

        // Drop extra tables
        for (Map.Entry<Database, Map<Long, Table>> entry : failoverGroup.getIncludeMgr().getIncludeTables()
                .entrySet()) {
            Set<String> tableNames = databaseToTableNames.get(entry.getKey().getFullName());
            if (tableNames == null) {
                continue;
            }

            for (Table localTable : entry.getValue().values()) {
                if (failoverGroup.getExcludeMgr().isExcludeTable(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                        entry.getKey().getFullName(), localTable.getName())) {
                    continue;
                }
                if (!tableNames.contains(localTable.getName())) {
                    DropReplicatedTableJob job = new DropReplicatedTableJob(failoverGroup, null,
                            null, entry.getKey(), localTable, true, false);
                    job.start();
                }
            }
        }
    }
}
