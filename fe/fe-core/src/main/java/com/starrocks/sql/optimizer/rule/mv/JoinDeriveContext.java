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

package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.List;

public class JoinDeriveContext {
    private JoinOperator queryJoinType;
    private JoinOperator mvJoinType;
    // join columns for left and right join tables
    private List<List<ColumnRefOperator>> joinColumns;

    public JoinDeriveContext(
            JoinOperator queryJoinType,
            JoinOperator mvJoinType,
            List<List<ColumnRefOperator>> joinColumns) {
        this.queryJoinType = queryJoinType;
        this.mvJoinType = mvJoinType;
        this.joinColumns = joinColumns;
    }

    public JoinOperator getQueryJoinType() {
        return queryJoinType;
    }

    public JoinOperator getMvJoinType() {
        return mvJoinType;
    }

    public List<ColumnRefOperator> getLeftJoinColumns() {
        return joinColumns.get(0);
    }

    public List<ColumnRefOperator> getRightJoinColumns() {
        return joinColumns.get(1);
    }
}
