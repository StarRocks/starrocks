// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.VariableMgr;
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
    private final Catalog catalog;
    private final List<TaskContext> taskContext;
    private final TaskScheduler taskScheduler;
    private final ColumnRefFactory columnRefFactory;
    private SessionVariable sessionVariable;
    private DumpInfo dumpInfo;
    private CTEContext cteContext;

    public OptimizerContext(Memo memo, ColumnRefFactory columnRefFactory) {
        this.memo = memo;
        this.ruleSet = new RuleSet();
        this.catalog = Catalog.getCurrentCatalog();
        this.taskContext = Lists.newArrayList();
        this.taskScheduler = SeriallyTaskScheduler.create();
        this.columnRefFactory = columnRefFactory;
        this.sessionVariable = VariableMgr.newSessionVariable();
    }

    public OptimizerContext(Memo memo, ColumnRefFactory columnRefFactory, ConnectContext connectContext) {
        this.memo = memo;
        this.ruleSet = new RuleSet();
        this.catalog = Catalog.getCurrentCatalog();
        this.taskContext = Lists.newArrayList();
        this.taskScheduler = SeriallyTaskScheduler.create();
        this.columnRefFactory = columnRefFactory;
        this.sessionVariable = connectContext.getSessionVariable();
        this.dumpInfo = connectContext.getDumpInfo();
        this.cteContext = new CTEContext();
        cteContext.reset();
        this.cteContext.setEnableCTE(sessionVariable.isCboCteReuse());
    }

    public Memo getMemo() {
        return memo;
    }

    public RuleSet getRuleSet() {
        return ruleSet;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public List<TaskContext> getTaskContext() {
        return taskContext;
    }

    public void addTaskContext(TaskContext context) {
        taskContext.add(context);
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
}
