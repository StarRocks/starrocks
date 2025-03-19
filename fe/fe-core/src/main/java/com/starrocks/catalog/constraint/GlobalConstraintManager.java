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

package com.starrocks.catalog.constraint;

import com.google.common.collect.Maps;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.util.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class GlobalConstraintManager {
    private static final Logger LOG = LogManager.getLogger(GlobalConstraintManager.class);

    // Table with its referred child table foreign constraints
    // eg:
    //  FK: t2.a references t1.a
    //  FK: t3.a references t1.a
    // Then the map will be: t1 -> {t2, t3}, so we can easily find the child tables of t1.
    private final Map<Table, Set<TableWithFKConstraint>> globalTableFKConstraintMap = Maps.newConcurrentMap();

    public void registerConstraint(Table table) {
        // anyway, we should ignore the exception here to avoid the impact of the exception on the main process
        try {
            registerConstraintImpl(table);
        } catch (Exception e) {
            // ignore
            LOG.warn("Failed to register constraint for table: {}", table.getName(), e);
        }
    }

    private void registerConstraintImpl(Table table) {
        if (table == null) {
            return;
        }
        List<ForeignKeyConstraint> foreignKeyConstraints = table.getForeignKeyConstraints();
        if (CollectionUtils.isEmpty(foreignKeyConstraints)) {
            return;
        }

        for (ForeignKeyConstraint fk : foreignKeyConstraints) {
            BaseTableInfo parent = fk.getParentTableInfo();
            if (parent == null) {
                continue;
            }
            MvUtils.getTable(parent).ifPresent(parentTable -> {
                globalTableFKConstraintMap.computeIfAbsent(parentTable, k -> Sets.newConcurrentHashSet())
                        .add(TableWithFKConstraint.of(table, fk));
            });
            LOG.info("Register constraint for parent table: {}, child table: {}, foreign constraint: {}",
                    parent.getTableName(), table.getName(), fk);
        }
    }

    public void unRegisterConstraint(Table table) {
        if (table == null) {
            return;
        }

        try {
            unRegisterConstraintOfParentTable(table);
        } catch (Exception e) {
            // ignore
            LOG.warn("Failed to unregister constraint as parent table: {}", table.getName(), e);
        }

        try {
            unRegisterConstraintOfChildTable(table);
        } catch (Exception e) {
            // ignore
            LOG.warn("Failed to unregister constraint as child table: {}", table.getName(), e);
        }
    }

    private void unRegisterConstraintOfChildTable(Table childTable) {
        // If the table has no child foreign key constraints, check its parent if it is a child table.
        List<ForeignKeyConstraint> parentForeignConstraints = childTable.getForeignKeyConstraints();
        if (CollectionUtils.isEmpty(parentForeignConstraints)) {
            return;
        }
        for (ForeignKeyConstraint foreignKeyConstraint : parentForeignConstraints) {
            BaseTableInfo parentTableInfo = foreignKeyConstraint.getParentTableInfo();
            if (parentTableInfo == null) {
                continue;
            }
            MvUtils.getTable(parentTableInfo).ifPresent(parentTable -> {
                Set<TableWithFKConstraint> parentChildTables = globalTableFKConstraintMap.get(parentTable);
                if (CollectionUtils.isEmpty(parentChildTables)) {
                    return;
                }
                // Remove the fk constraint from the global mapping.
                parentChildTables.removeIf(tableWithFKConstraint ->
                        tableWithFKConstraint.equals(TableWithFKConstraint.of(childTable, foreignKeyConstraint)));
                LOG.info("Unregister constraint for parent table: {}, child table: {}, foreign constraint: {}",
                        parentTable.getName(), childTable.getName(), foreignKeyConstraint);

                if (parentChildTables.isEmpty()) {
                    globalTableFKConstraintMap.remove(parentTable);
                }
            });
        }
    }

    private void unRegisterConstraintOfParentTable(Table parentTable) {
        Set<TableWithFKConstraint> childTables = globalTableFKConstraintMap.get(parentTable);
        if (CollectionUtils.isEmpty(childTables)) {
            return;
        }
        // If the table has child foreign key constraints, remove the fk constraints from the child tables
        // since its parent is dropped.
        LOG.info("Unregister constraint for table: {}", parentTable.getName());
        for (TableWithFKConstraint tableWithFKConstraint : childTables) {
            // remove child's fk constraint since the parent table is dropped
            Table childTable = tableWithFKConstraint.getChildTable();
            if (childTable == null) {
                continue;
            }

            List<ForeignKeyConstraint> newChildForeignKeyConstraints = childTable.getForeignKeyConstraints();
            if (CollectionUtils.isEmpty(newChildForeignKeyConstraints)) {
                continue;
            }
            LOG.info("Unregister constraint for child table: {}, foreign constraints:{}", childTable.getName(),
                    newChildForeignKeyConstraints);
            newChildForeignKeyConstraints.remove(tableWithFKConstraint.getRefConstraint());
            childTable.setForeignKeyConstraints(newChildForeignKeyConstraints);
        }
        globalTableFKConstraintMap.remove(parentTable);
    }

    public Set<TableWithFKConstraint> getRefConstraints(Table table) {
        if (table == null || !globalTableFKConstraintMap.containsKey(table)) {
            return Sets.newHashSet();
        }
        return globalTableFKConstraintMap.get(table);
    }

    public void updateConstraint(Table table, Set<TableWithFKConstraint> relatedConstrains) {
        if (CollectionUtils.isEmpty(relatedConstrains)) {
            globalTableFKConstraintMap.remove(table);
        } else {
            globalTableFKConstraintMap.put(table, relatedConstrains);
        }
    }
}
