// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.LabelName;

/*
  Resume routine load job by name

  syntax:
      RESUME ROUTINE LOAD [database.]name
 */
public class ResumeRoutineLoadStmt extends DdlStmt {

    private LabelName labelName;

    public ResumeRoutineLoadStmt(LabelName labelName) {
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
        return visitor.visitResumeRoutineLoadStatement(this, context);
    }
}
