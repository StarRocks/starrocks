// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.task.SeriallyTaskScheduler;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;

import java.util.List;

public class OptimizerContext {
    private final Memo memo;
    private final RuleSet ruleSet;
    private final GlobalStateMgr globalStateMgr;
    private final TaskScheduler taskScheduler;
    private final ColumnRefFactory columnRefFactory;
    private SessionVariable sessionVariable;
    private DumpInfo dumpInfo;
    private CTEContext cteContext;
    private TaskContext currentTaskContext;
    private OptimizerTraceInfo traceInfo;
    private OptimizerConfig optimizerConfig;
    private List<MaterializationContext> candidateMvs;

    // Is not null predicate can be derived from inner join or semi join,
    // which should be kept to be used to convert outer join into inner join.
    private List<IsNullPredicateOperator> pushdownNotNullPredicates = Lists.newArrayList();

    @VisibleForTesting
    public OptimizerContext(Memo memo, ColumnRefFactory columnRefFactory) {
        this.memo = memo;
        this.ruleSet = new RuleSet();
        this.globalStateMgr = GlobalStateMgr.getCurrentState();
        this.taskScheduler = SeriallyTaskScheduler.create();
        this.columnRefFactory = columnRefFactory;
        this.sessionVariable = VariableMgr.newSessionVariable();
        this.optimizerConfig = new OptimizerConfig();
        this.candidateMvs = Lists.newArrayList();
    }

    @VisibleForTesting
    public OptimizerContext(Memo memo, ColumnRefFactory columnRefFactory, ConnectContext connectContext) {
        this(memo, columnRefFactory, connectContext, OptimizerConfig.defaultConfig());
    }

    public OptimizerContext(Memo memo, ColumnRefFactory columnRefFactory, ConnectContext connectContext,
                            OptimizerConfig optimizerConfig) {
        this.memo = memo;
        this.ruleSet = new RuleSet();
        this.globalStateMgr = GlobalStateMgr.getCurrentState();
        this.taskScheduler = SeriallyTaskScheduler.create();
        this.columnRefFactory = columnRefFactory;
        this.sessionVariable = connectContext.getSessionVariable();
        this.dumpInfo = connectContext.getDumpInfo();
        this.cteContext = new CTEContext();
        cteContext.reset();
        this.cteContext.setEnableCTE(sessionVariable.isCboCteReuse());
        this.cteContext.setInlineCTERatio(sessionVariable.getCboCTERuseRatio());
        this.cteContext.setMaxCTELimit(sessionVariable.getCboCTEMaxLimit());
        this.optimizerConfig = optimizerConfig;
        this.candidateMvs = Lists.newArrayList();
    }

    public Memo getMemo() {
        return memo;
    }

    public RuleSet getRuleSet() {
        return ruleSet;
    }

    public GlobalStateMgr getCatalog() {
        return globalStateMgr;
    }

    public TaskScheduler getTaskScheduler() {
        return taskScheduler;
    }

    public ColumnRefFactory getColumnRefFactory() {
        return columnRefFactory;
    }

    public final SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public void setSessionVariable(SessionVariable sessionVariable) {
        this.sessionVariable = sessionVariable;
    }

    public DumpInfo getDumpInfo() {
        return dumpInfo;
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

    public void setTraceInfo(OptimizerTraceInfo traceInfo) {
        this.traceInfo = traceInfo;
    }

    public OptimizerTraceInfo getTraceInfo() {
        return traceInfo;
    }

    public OptimizerConfig getOptimizerConfig() {
        return optimizerConfig;
    }

    public List<MaterializationContext> getCandidateMvs() {
        return candidateMvs;
    }

    public void addCandidateMvs(MaterializationContext candidateMv) {
        this.candidateMvs.add(candidateMv);
    }
<<<<<<< HEAD
=======

    public void setEnableLeftRightJoinEquivalenceDerive(boolean enableLeftRightJoinEquivalenceDerive) {
        this.enableLeftRightJoinEquivalenceDerive = enableLeftRightJoinEquivalenceDerive;
    }

    public boolean isEnableLeftRightJoinEquivalenceDerive() {
        return enableLeftRightJoinEquivalenceDerive;
    }

    public void setUpdateTableId(long updateTableId) {
        this.updateTableId = updateTableId;
    }

    public long getUpdateTableId() {
        return updateTableId;
    }

    public long optimizerElapsedMs() {
        return optimizerTimer.elapsed(TimeUnit.MILLISECONDS);
    }

    public boolean ruleExhausted(RuleType ruleType) {
        Stopwatch watch = ruleWatchMap.computeIfAbsent(ruleType, (k) -> Stopwatch.createStarted());
        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        long timeLimit = Math.min(sessionVariable.getOptimizerMaterializedViewTimeLimitMillis(),
                sessionVariable.getOptimizerExecuteTimeout());
        return elapsed > timeLimit;
    }

    public boolean isObtainedFromInternalStatistics() {
        return isObtainedFromInternalStatistics;
    }

    public void setObtainedFromInternalStatistics(boolean obtainedFromInternalStatistics) {
        isObtainedFromInternalStatistics = obtainedFromInternalStatistics;
    }

    /**
     * Whether reach optimizer timeout
     */
    public boolean reachTimeout() {
        long timeout = getSessionVariable().getOptimizerExecuteTimeout();
        return optimizerElapsedMs() > timeout;
    }

    public Set<OlapTable> getQueryTables() {
        return queryTables;
    }

    public void setQueryTables(Set<OlapTable> queryTables) {
        this.queryTables = queryTables;
    }

    /**
     * Throw exception if reach optimizer timeout
     */
    public void checkTimeout() {
        if (!reachTimeout()) {
            return;
        }
        Memo memo = getMemo();
        Group group = memo == null ? null : memo.getRootGroup();
        throw new StarRocksPlannerException("StarRocks planner use long time " + optimizerElapsedMs() +
                " ms in " + (group == null ? "logical" : "memo") + " phase, This probably because " +
                "1. FE Full GC, " +
                "2. Hive external table fetch metadata took a long time, " +
                "3. The SQL is very complex. " +
                "You could " +
                "1. adjust FE JVM config, " +
                "2. try query again, " +
                "3. enlarge new_planner_optimize_timeout session variable",
                ErrorType.INTERNAL_ERROR);
    }

    public void setQueryMaterializationContext(QueryMaterializationContext queryMaterializationContext) {
        this.queryMaterializationContext = queryMaterializationContext;
    }

    public QueryMaterializationContext getQueryMaterializationContext() {
        return queryMaterializationContext;
    }

    public boolean isShortCircuit() {
        return isShortCircuit;
    }

    public void setShortCircuit(boolean shortCircuit) {
        isShortCircuit = shortCircuit;
    }

    public void clear() {
        if (this.queryMaterializationContext != null) {
            this.queryMaterializationContext.clear();
        }
    }

    public void setInMemoPhase(boolean inMemoPhase) {
        this.inMemoPhase = inMemoPhase;
    }

    public boolean isInMemoPhase() {
        return this.inMemoPhase;
    }

    public List<IsNullPredicateOperator> getPushdownNotNullPredicates() {
        return pushdownNotNullPredicates;
    }

    public void addPushdownNotNullPredicates(IsNullPredicateOperator notNullPredicate) {
        pushdownNotNullPredicates.add(notNullPredicate);
    }

    // Should clear pushdownNotNullPredicates after each call of PUSH_DOWN_PREDICATE rule set
    public void reset() {
        pushdownNotNullPredicates.clear();
    }
>>>>>>> 37b8aa5a55 ([BugFix] fix left outer join to inner join bug and string not equal rewrite bug (#39331))
}
