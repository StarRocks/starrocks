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

package com.starrocks.statistic.columns;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PredicateColumnsMgr {

    private static final Logger LOG = LogManager.getLogger(PredicateColumnsMgr.class);
    private static final PredicateColumnsMgr INSTANCE = new PredicateColumnsMgr();

    // Why Map? To update the usage
    private final Map<ColumnUsage, ColumnUsage> id2columnUsage = Maps.newConcurrentMap();

    public static PredicateColumnsMgr getInstance() {
        return INSTANCE;
    }

    // ============================ Record predicate columns from query ========================= //
    public void recordScanColumns(Map<ColumnRefOperator, Column> scanColumns, Table table, OptExpression optExpr) {
        for (Column column : scanColumns.values()) {
            addOrUpdateColumnUsage(table, column, ColumnUsage.UseCase.NORMAL);
        }
    }

    public void recordPredicateColumns(ScalarOperator predicate, ColumnRefFactory factory, OptExpression optExpr) {
        if (predicate == null) {
            return;
        }
        List<ColumnRefOperator> refs = Utils.collect(predicate, ColumnRefOperator.class);
        addOrUpdateColumnUsage(refs, factory, ColumnUsage.UseCase.PREDICATE, optExpr);
    }

    public void recordJoinPredicate(List<BinaryPredicateOperator> onPredicates, ColumnRefFactory factory,
                                    OptExpression optExpr) {
        for (BinaryPredicateOperator op : onPredicates) {
            List<ColumnRefOperator> refs = Utils.collect(op, ColumnRefOperator.class);
            addOrUpdateColumnUsage(refs, factory, ColumnUsage.UseCase.JOIN, optExpr);
        }
    }

    public void recordGroupByColumns(Map<ColumnRefOperator, CallOperator> aggregations,
                                     List<ColumnRefOperator> groupBys,
                                     ColumnRefFactory factory, OptExpression optExpr) {
        for (var entry : aggregations.entrySet()) {
            if (entry.getValue().isDistinct()) {
                List<ColumnRefOperator> refs = Utils.collect(entry.getValue(), ColumnRefOperator.class);
                addOrUpdateColumnUsage(refs, factory, ColumnUsage.UseCase.DISTINCT, optExpr);
            }
        }

        addOrUpdateColumnUsage(groupBys, factory, ColumnUsage.UseCase.GROUP_BY, optExpr);
    }

    public void recordWindowPartitionBy(List<ScalarOperator> partitionByList, ColumnRefFactory factory,
                                        OptExpression optExpr) {
        for (var partitionBy : ListUtils.emptyIfNull(partitionByList)) {
            List<ColumnRefOperator> refs = Utils.collect(partitionBy, ColumnRefOperator.class);
            addOrUpdateColumnUsage(refs, factory, ColumnUsage.UseCase.GROUP_BY, optExpr);
        }
    }

    private void addOrUpdateColumnUsage(List<ColumnRefOperator> refs, ColumnRefFactory factory,
                                        ColumnUsage.UseCase useCase, OptExpression optExpr) {
        for (ColumnRefOperator ref : ListUtils.emptyIfNull(refs)) {
            try {
                var tables = Utils.resolveColumnRefRecursive(ref, factory, optExpr);
                for (var column : ListUtils.emptyIfNull(tables)) {
                    addOrUpdateColumnUsage(column.first, column.second, useCase);
                }
            } catch (Exception e) {
                LOG.warn("failed to resolve column ref {} from expr {}", ref, optExpr);
            }
        }
    }

    private void addOrUpdateColumnUsage(Table table, Column column, ColumnUsage.UseCase useCase) {
        // only support OLAP table right now
        if (!table.isNativeTableOrMaterializedView()) {
            return;
        }
        Optional<ColumnUsage> mayUsage = ColumnUsage.build(column, table, useCase);
        if (mayUsage.isEmpty()) {
            return;
        }
        if (Database.isSystemOrInternalDatabase(mayUsage.get().getTableName().getDb())) {
            return;
        }
        ColumnUsage usage = mayUsage.get();
        ColumnUsage oldValue = id2columnUsage.computeIfAbsent(usage, k -> usage);
        oldValue.useNow(useCase);
    }

    //==================================== Query ============================================ //
    public List<ColumnUsage> query(TableName tableName) {
        return queryByUseCase(tableName, ColumnUsage.UseCase.all());
    }

    public List<ColumnUsage> queryPredicateColumns(TableName tableName) {
        return queryByUseCase(tableName, ColumnUsage.UseCase.getPredicateColumnUseCase());
    }

    private List<ColumnUsage> queryByUseCase(TableName tableName, EnumSet<ColumnUsage.UseCase> useCases) {
        TableNamePredicate predicate = new TableNamePredicate(tableName);
        Predicate<ColumnUsage> useCasePredicate = (c) -> !SetUtils.intersection(c.getUseCases(), useCases).isEmpty();
        Predicate<ColumnUsage> pred = useCasePredicate.and(x -> predicate.test(x.getTableName()));
        if (FeConstants.runningUnitTest) {
            return id2columnUsage.values().stream().filter(pred).collect(Collectors.toList());
        } else {
            return getStorage().queryGlobalState(tableName, useCases).stream().filter(pred)
                    .collect(Collectors.toList());
        }
    }

    //==================================== Maintenance ============================================ //
    public void persist() {
        getStorage().persist(id2columnUsage.values());
    }

    public void vacuum() {
        long ttlHour = Config.statistic_predicate_columns_ttl_hours;
        LocalDateTime ttlTime = TimeUtils.getSystemNow().minusHours(ttlHour);
        Predicate<ColumnUsage> outdated = x -> x.getLastUsed().isBefore(ttlTime);

        long before = id2columnUsage.size();
        if (id2columnUsage.values().removeIf(outdated)) {
            long after = id2columnUsage.size();
            LOG.info("removed {} objects from predicate columns because of ttl {}", before - after,
                    Config.statistic_predicate_columns_ttl_hours);
        }

        // If the process crashed before vacuum the storage, the storage may be different from in-memory state,
        // but it doesn't matter. Because we will remove them finally.
        getStorage().vacuum(ttlTime);
    }

    public void restore() {
        List<ColumnUsage> state = getStorage().restore();
        for (ColumnUsage usage : ListUtils.emptyIfNull(state)) {
            id2columnUsage.merge(usage, usage, ColumnUsage::merge);
        }
    }

    private PredicateColumnsStorage getStorage() {
        return PredicateColumnsStorage.getInstance();
    }

    @VisibleForTesting
    public void reset() {
        id2columnUsage.clear();
    }

    public void startDaemon() {
        DaemonThread.getInstance().start();
    }

    static class DaemonThread extends FrontendDaemon {

        private static final DaemonThread INSTANCE = new DaemonThread();

        public DaemonThread() {
            super("PredicateColumnsDaemonThread", Config.statistic_predicate_columns_persist_interval_sec * 1000L);
        }

        public static DaemonThread getInstance() {
            return INSTANCE;
        }

        @Override
        protected void runAfterCatalogReady() {
            setInterval(Config.statistic_predicate_columns_persist_interval_sec * 1000L);

            PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
            PredicateColumnsStorage storage = PredicateColumnsStorage.getInstance();

            if (!storage.isSystemTableReady()) {
                LOG.warn("system table of predicate_columns is still not ready");
                return;
            }

            if (!storage.isRestored()) {
                mgr.restore();
                storage.finishRestore();
            } else {
                mgr.vacuum();
                mgr.persist();
            }
        }
    }

}
