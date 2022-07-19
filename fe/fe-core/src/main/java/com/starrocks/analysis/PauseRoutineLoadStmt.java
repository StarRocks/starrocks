// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;

/*
  Pause routine load by name

  syntax:
      PAUSE ROUTINE LOAD [database.]name
 */
public class PauseRoutineLoadStmt extends DdlStmt {

    private LabelName labelName;

    public PauseRoutineLoadStmt(LabelName labelName) {
        this.labelName = labelName;
    }

    public String getName() {
        return labelName.getLabelName();
    }

    public String getDbFullName() {
        return labelName.getDbName();
    }

    public void setLabelName(LabelName labelName) {
        this.labelName = labelName;
    }

    @Deprecated
    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        labelName.analyze(analyzer);
    }

    @Override
    public String toSql() {
        String dbName = labelName.getDbName();
        String routineName = labelName.getLabelName();
        dbName = ClusterNamespace.getNameFromFullName(dbName);
        return "PAUSE ROUTINE LOAD FOR " + dbName + "." + routineName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPauseRoutineLoadStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
