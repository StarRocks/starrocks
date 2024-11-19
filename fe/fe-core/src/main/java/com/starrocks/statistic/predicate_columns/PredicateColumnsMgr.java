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

package com.starrocks.statistic.predicate_columns;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
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
import org.apache.commons.lang3.StringUtils;

import java.time.LocalDateTime;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class PredicateColumnsMgr {

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
            var tables = Utils.resolveColumnRefRecursive(ref, factory, optExpr);
            for (var column : ListUtils.emptyIfNull(tables)) {
                addOrUpdateColumnUsage(column.first, column.second, useCase);
            }
        }
    }

    private void addOrUpdateColumnUsage(Table table, Column column, ColumnUsage.UseCase useCase) {
        Optional<ColumnUsage> mayUsage = ColumnUsage.build(column, table, useCase);
        if (mayUsage.isEmpty()) {
            return;
        }
        ColumnUsage usage = mayUsage.get();
        ColumnUsage oldValue = id2columnUsage.computeIfAbsent(usage, k -> usage);
        oldValue.useNow(useCase);
    }

    //==================================== Query ============================================ //
    public List<ColumnUsage> query(TableName tableName) {
        // FIXME: use Storage.queryGlobalState interface
        TablePredicate predicate = new TablePredicate(tableName);
        return id2columnUsage.values().stream().filter(predicate).collect(Collectors.toList());
    }

    public List<ColumnUsage> queryPredicateColumns(TableName tableName) {
        // FIXME: use Storage.queryGlobalState interface
        return queryByUseCase(tableName, ColumnUsage.UseCase.getPredicateColumnUseCase());
    }

    public List<ColumnUsage> queryByUseCase(TableName tableName, EnumSet<ColumnUsage.UseCase> useCases) {
        // FIXME: use Storage.queryGlobalState interface
        TablePredicate predicate = new TablePredicate(tableName);
        Predicate<ColumnUsage> useCasePredicate = (c) -> !SetUtils.intersection(c.getUseCases(), useCases).isEmpty();
        Predicate<ColumnUsage> pred = predicate.and(useCasePredicate);
        return id2columnUsage.values().stream().filter(pred).collect(Collectors.toList());
    }

    //==================================== Maintenance ============================================ //
    public void persist() {
        getStorage().persist(id2columnUsage.values());
    }

    public void vacuum() {
        long ttlHour = Config.statistic_predicate_columns_ttl_hours;
        LocalDateTime ttlTime = TimeUtils.getSystemNow().minusHours(ttlHour);
        Predicate<ColumnUsage> outdated = x -> x.getLastUsed().isBefore(ttlTime);

        id2columnUsage.values().removeIf(outdated);
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

    /**
     * The predicate to identify a table
     */
    static class TablePredicate implements Predicate<ColumnUsage> {

        private final TableName tableName;

        public TablePredicate(TableName tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean test(ColumnUsage columnUsage) {
            if (StringUtils.isNotEmpty(tableName.getCatalog())) {
                if (!columnUsage.getTableName().getCatalog().equalsIgnoreCase(tableName.getCatalog())) {
                    return false;
                }
            }
            if (StringUtils.isNotEmpty(tableName.getDb())) {
                if (!columnUsage.getTableName().getDb().equalsIgnoreCase(tableName.getDb())) {
                    return false;
                }
            }
            if (StringUtils.isNotEmpty(tableName.getTbl())) {
                if (!columnUsage.getTableName().getTbl().equalsIgnoreCase(tableName.getTbl())) {
                    return false;
                }
            }

            return true;
        }
    }

    static class DaemonThread extends FrontendDaemon {

        private static final DaemonThread INSTANCE = new DaemonThread();

        public DaemonThread() {
            super("PredicateColumnsDaemonThread");
        }

        public static DaemonThread getInstance() {
            return INSTANCE;
        }

        public void start() {

        }

        @Override
        protected void runAfterCatalogReady() {
            setInterval(Config.statistic_predicate_columns_persist_interval_sec * 1000L);

            PredicateColumnsMgr mgr = PredicateColumnsMgr.getInstance();
            PredicateColumnsStorage storage = PredicateColumnsStorage.getInstance();

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
