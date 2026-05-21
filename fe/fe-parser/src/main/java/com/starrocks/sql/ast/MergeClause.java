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

package com.starrocks.sql.ast;

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public abstract class MergeClause implements ParseNode {
    private final Expr condition; // optional AND condition
    private final NodePosition pos;

    protected MergeClause(Expr condition, NodePosition pos) {
        this.condition = condition;
        this.pos = pos;
    }

    public Expr getCondition() {
        return condition;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    public abstract boolean isMatched();

    // WHEN MATCHED THEN UPDATE SET ...
    public static class MergeUpdateClause extends MergeClause {
        private final List<ColumnAssignment> assignments;

        public MergeUpdateClause(Expr condition, List<ColumnAssignment> assignments, NodePosition pos) {
            super(condition, pos);
            this.assignments = assignments;
        }

        public List<ColumnAssignment> getAssignments() {
            return assignments;
        }

        @Override
        public boolean isMatched() {
            return true;
        }
    }

    // WHEN MATCHED THEN DELETE
    public static class MergeDeleteClause extends MergeClause {
        public MergeDeleteClause(Expr condition, NodePosition pos) {
            super(condition, pos);
        }

        @Override
        public boolean isMatched() {
            return true;
        }
    }

    // WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
    public static class MergeInsertClause extends MergeClause {
        private final List<String> targetColumnNames; // optional, null means all columns
        private final List<Expr> valueExpressions;

        public MergeInsertClause(Expr condition, List<String> targetColumnNames,
                                 List<Expr> valueExpressions, NodePosition pos) {
            super(condition, pos);
            this.targetColumnNames = targetColumnNames;
            this.valueExpressions = valueExpressions;
        }

        public List<String> getTargetColumnNames() {
            return targetColumnNames;
        }

        public List<Expr> getValueExpressions() {
            return valueExpressions;
        }

        @Override
        public boolean isMatched() {
            return false;
        }
    }
}
