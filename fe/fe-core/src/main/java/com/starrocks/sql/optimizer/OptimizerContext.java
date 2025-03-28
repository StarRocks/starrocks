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

package com.starrocks.sql.optimizer;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;
import com.starrocks.sql.optimizer.transformer.MVTransformerContext;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class OptimizerContext {
    // ============================ Query ============================
    private StatementBase statement;
    private ConnectContext connectContext;
    private ColumnRefFactory columnRefFactory;
    private Set<OlapTable> queryTables;
    private long updateTableId = -1;

    private OptimizerOptions optimizerOptions;

    // ============================ Optimizer ============================
    private Memo memo;
    private final RuleSet ruleSet;
    private TaskScheduler taskScheduler;

    private final CTEContext cteContext;
    private TaskContext currentTaskContext;
    private final QueryMaterializationContext queryMaterializationContext = new QueryMaterializationContext();

    private MVTransformerContext mvTransformerContext;

    // uniquePartitionIdGenerator for external catalog
    private long uniquePartitionIdGenerator = 0L;
    private final Stopwatch optimizerTimer = Stopwatch.createStarted();
    private final Map<RuleType, Stopwatch> ruleWatchMap = Maps.newHashMap();

    // ============================ Task Variables ============================
    // The options for join predicate pushdown rule
    private boolean enableJoinEquivalenceDerive = true;
    private boolean enableJoinPredicatePushDown = true;

    // QueryMaterializationContext is different from MaterializationContext that it keeps the context during the query
    // lifecycle instead of per materialized view.

    private boolean isObtainedFromInternalStatistics = false;
    private boolean inMemoPhase = false;

    // Is not null predicate can be derived from inner join or semi join,
    // which should be kept to be used to convert outer join into inner join.
    private final List<IsNullPredicateOperator> pushdownNotNullPredicates = Lists.newArrayList();

    OptimizerContext(ConnectContext context) {
        this.connectContext = context;
        this.ruleSet = new RuleSet();
        this.cteContext = new CTEContext();
        cteContext.reset();
        this.cteContext.setEnableCTE(getSessionVariable().isCboCteReuse());
        this.cteContext.setInlineCTERatio(getSessionVariable().getCboCTERuseRatio());
        this.cteContext.setMaxCTELimit(getSessionVariable().getCboCTEMaxLimit());

        this.optimizerOptions = OptimizerOptions.defaultOpt();
    }

    // ============================ Query ============================
    public StatementBase getStatement() {
        return statement;
    }

    public void setStatement(StatementBase statement) {
        this.statement = statement;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public void setConnectContext(ConnectContext connectContext) {
        this.connectContext = connectContext;
    }

    public UUID getQueryId() {
        return connectContext.getQueryId();
    }

    public final SessionVariable getSessionVariable() {
        return connectContext.getSessionVariable();
    }

    public DumpInfo getDumpInfo() {
        return connectContext.getDumpInfo();
    }

    public void setColumnRefFactory(ColumnRefFactory columnRefFactory) {
        this.columnRefFactory = columnRefFactory;
    }

    public ColumnRefFactory getColumnRefFactory() {
        return columnRefFactory;
    }

    public Set<OlapTable> getQueryTables() {
        return queryTables;
    }

    public void setQueryTables(Set<OlapTable> queryTables) {
        this.queryTables = queryTables;
    }

    public void setUpdateTableId(long updateTableId) {
        this.updateTableId = updateTableId;
    }

    public long getUpdateTableId() {
        return updateTableId;
    }

    public OptimizerOptions getOptimizerOptions() {
        return optimizerOptions;
    }

    public void setOptimizerOptions(OptimizerOptions optimizerOptions) {
        this.optimizerOptions = optimizerOptions;
    }

    // ============================ Optimizer ============================
    public Memo getMemo() {
        return memo;
    }

    public void setMemo(Memo memo) {
        this.memo = memo;
    }

    public RuleSet getRuleSet() {
        return ruleSet;
    }

    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    public void setTaskScheduler(TaskScheduler taskScheduler) {
        this.taskScheduler = taskScheduler;
    }

    public CTEContext getCteContext() {
        return cteContext;
    }

    public void setTaskContext(TaskContext context) {
        this.currentTaskContext = context;
    }

    public TaskContext getTaskContext() {
        return currentTaskContext;
    }

    public QueryMaterializationContext getQueryMaterializationContext() {
        return queryMaterializationContext;
    }

    public MVTransformerContext getMvTransformerContext() {
        return mvTransformerContext;
    }

    public void setMvTransformerContext(MVTransformerContext mvTransformerContext) {
        this.mvTransformerContext = mvTransformerContext;
    }

    // ============================ Task Variables ============================
    public boolean isEnableJoinEquivalenceDerive() {
        return enableJoinEquivalenceDerive;
    }

    public void setEnableJoinEquivalenceDerive(boolean enableJoinEquivalenceDerive) {
        this.enableJoinEquivalenceDerive = enableJoinEquivalenceDerive;
    }

    public boolean isEnableJoinPredicatePushDown() {
        return enableJoinPredicatePushDown;
    }

    public void setEnableJoinPredicatePushDown(boolean enableJoinPredicatePushDown) {
        this.enableJoinPredicatePushDown = enableJoinPredicatePushDown;
    }

    public boolean isObtainedFromInternalStatistics() {
        return isObtainedFromInternalStatistics;
    }

    public void setObtainedFromInternalStatistics(boolean obtainedFromInternalStatistics) {
        isObtainedFromInternalStatistics = obtainedFromInternalStatistics;
    }

    public void setInMemoPhase(boolean inMemoPhase) {
        this.inMemoPhase = inMemoPhase;
    }

    public boolean isInMemoPhase() {
        return this.inMemoPhase;
    }

    /**
     * Get all valid candidate materialized views for the query:
     * - The materialized view is valid to rewrite by rule(SPJG)
     * - The materialized view's refresh-ness is valid to rewrite.
     */
    public List<MaterializationContext> getCandidateMvs() {
        return queryMaterializationContext.getValidCandidateMVs();
    }

    public Stopwatch getStopwatch(RuleType ruleType) {
        return ruleWatchMap.computeIfAbsent(ruleType, (k) -> Stopwatch.createStarted());
    }

    public boolean ruleExhausted(RuleType ruleType) {
        Stopwatch watch = getStopwatch(ruleType);
        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        long timeLimit = Math.min(getSessionVariable().getOptimizerMaterializedViewTimeLimitMillis(),
                getSessionVariable().getOptimizerExecuteTimeout());
        return elapsed > timeLimit;
    }

    public List<IsNullPredicateOperator> getPushdownNotNullPredicates() {
        return pushdownNotNullPredicates;
    }

    public void addPushdownNotNullPredicates(IsNullPredicateOperator notNullPredicate) {
        pushdownNotNullPredicates.add(notNullPredicate);
    }

    // Should clear pushdownNotNullPredicates after each call of PUSH_DOWN_PREDICATE rule set
    public void clearNotNullPredicates() {
        pushdownNotNullPredicates.clear();
    }

    public long getNextUniquePartitionId() {
        return uniquePartitionIdGenerator++;
    }

    /**
     * Throw exception if reach optimizer timeout
     */
    public void checkTimeout() {
        long timeout = getSessionVariable().getOptimizerExecuteTimeout();
        long now = optimizerTimer.elapsed(TimeUnit.MILLISECONDS);
        if (timeout > 0 && now > timeout) {
            throw new StarRocksPlannerException("StarRocks planner use long time " + now +
                    " ms in " + (inMemoPhase ? "memo" : "logical") + " phase, This probably because " +
                    "1. FE Full GC, " +
                    "2. Hive external table fetch metadata took a long time, " +
                    "3. The SQL is very complex. " +
                    "You could " +
                    "1. adjust FE JVM config, " +
                    "2. try query again, " +
                    "3. enlarge new_planner_optimize_timeout session variable",
                    ErrorType.INTERNAL_ERROR);
        }
    }
}
