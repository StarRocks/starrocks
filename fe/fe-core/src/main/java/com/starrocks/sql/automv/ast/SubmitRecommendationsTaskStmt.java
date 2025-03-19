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
package com.starrocks.sql.automv.ast;

import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

public class SubmitRecommendationsTaskStmt extends DdlStmt {
    private String taskName;
    private ShowRecommendationsStmt stmt;

    public SubmitRecommendationsTaskStmt(String taskName, ShowRecommendationsStmt stmt) {
        super(NodePosition.ZERO);
        this.taskName = taskName;
        this.stmt = stmt;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public ShowRecommendationsStmt getStmt() {
        return stmt;
    }

    public void setStmt(ShowRecommendationsStmt stmt) {
        this.stmt = stmt;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        if (visitor instanceof AstVisitorEPack) {
            return ((AstVisitorEPack<R, C>) visitor).visitSubmitRecommendationsTaskStmt(this, context);
        } else {
            return null;
        }
    }
}
