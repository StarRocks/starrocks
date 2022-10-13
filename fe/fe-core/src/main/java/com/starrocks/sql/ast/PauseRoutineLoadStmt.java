// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.LabelName;

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

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPauseRoutineLoadStatement(this, context);
    }
}
