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

import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class DropSnapshotStmt extends DdlStmt {
    private final String repoName;
    private final Expr where;
    private String snapshotName;
    private String timestampOperator; // "<=", ">="
    private String timestamp;
    private List<String> snapshotNames;

    public DropSnapshotStmt(String repoName, Expr where) {
        this(repoName, where, NodePosition.ZERO);
    }

    public DropSnapshotStmt(String repoName, Expr where, NodePosition pos) {
        super(pos);
        this.repoName = repoName;
        this.where = where;
        this.snapshotNames = Lists.newArrayList();
    }

    public String getRepoName() {
        return repoName;
    }

    public Expr getWhere() {
        return where;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public String getTimestampOperator() {
        return timestampOperator;
    }

    public void setTimestampOperator(String timestampOperator) {
        this.timestampOperator = timestampOperator;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public List<String> getSnapshotNames() {
        return snapshotNames;
    }

    public void addSnapshotName(String snapshotName) {
        this.snapshotNames.add(snapshotName);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropSnapshotStatement(this, context);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP SNAPSHOT ON ").append(repoName);
        if (where != null) {
            sb.append(" WHERE ").append(where.toSql());
        }
        return sb.toString();
    }
}
