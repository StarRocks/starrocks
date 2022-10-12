// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.UserException;
import com.starrocks.sql.ast.AstVisitor;

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
        return "RESUME ROUTINE LOAD FOR " + dbName + "." + routineName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitResumeRoutineLoadStatement(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
