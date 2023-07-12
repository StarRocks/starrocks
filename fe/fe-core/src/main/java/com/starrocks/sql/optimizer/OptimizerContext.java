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
import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.dump.DumpInfo;
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

    private long updateTableId = -1;
    private boolean enableLeftRightJoinEquivalenceDerive = true;

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
}
