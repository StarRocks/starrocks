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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.OlapTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.dump.DumpInfo;
import com.starrocks.sql.optimizer.rule.RuleSet;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.task.SeriallyTaskScheduler;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.sql.optimizer.task.TaskScheduler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

    private Set<OlapTable>  queryTables;

    private long updateTableId = -1;
    private boolean enableLeftRightJoinEquivalenceDerive = true;
    private final Stopwatch optimizerTimer = Stopwatch.createStarted();
    private final Map<RuleType, Stopwatch> ruleWatchMap = Maps.newHashMap();

<<<<<<< HEAD
=======
    // used by view based mv rewrite
    // query's logical plan with view
    private OptExpression logicalTreeWithView;
    // collect LogicalViewScanOperators
    private List<LogicalViewScanOperator> viewScans;

    private boolean isShortCircuit = false;
    // QueryMaterializationContext is different from MaterializationContext that it keeps the context during the query
    // lifecycle instead of per materialized view.
    // TODO: refactor materialized view's variables/contexts into this.
    private QueryMaterializationContext queryMaterializationContext;

>>>>>>> 0c5a5ccbe9 ([BugFix] Optimize partition compensate strategy for performance(Part1) (backport #36559) (#38555))
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
        return traceInfo.getStopwatch().elapsed(TimeUnit.MILLISECONDS);
    }

    public boolean ruleExhausted(RuleType ruleType) {
        Stopwatch watch = ruleWatchMap.computeIfAbsent(ruleType, (k) -> Stopwatch.createStarted());
        long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
        long timeLimit = Math.min(sessionVariable.getOptimizerMaterializedViewTimeLimitMillis(),
                sessionVariable.getOptimizerExecuteTimeout());
        return elapsed > timeLimit;
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
<<<<<<< HEAD
=======

    public OptExpression getLogicalTreeWithView() {
        return logicalTreeWithView;
    }

    public void setLogicalTreeWithView(OptExpression logicalTreeWithView) {
        this.logicalTreeWithView = logicalTreeWithView;
    }

    public void setViewScans(List<LogicalViewScanOperator> viewScans) {
        this.viewScans = viewScans;
    }

    public List<LogicalViewScanOperator> getViewScans() {
        return viewScans;
    }

    public boolean isShortCircuit() {
        return isShortCircuit;
    }

    public void setShortCircuit(boolean shortCircuit) {
        isShortCircuit = shortCircuit;
    }

    public void setQueryMaterializationContext(QueryMaterializationContext queryMaterializationContext) {
        this.queryMaterializationContext = queryMaterializationContext;
    }

    public QueryMaterializationContext getQueryMaterializationContext() {
        return queryMaterializationContext;
    }

    public void clear() {
        if (this.queryMaterializationContext != null) {
            this.queryMaterializationContext.clear();
        }
    }
>>>>>>> 0c5a5ccbe9 ([BugFix] Optimize partition compensate strategy for performance(Part1) (backport #36559) (#38555))
}
