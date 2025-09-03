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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class ShowSnapshotStmt extends ShowStmt {
    public static final ImmutableList<String> SNAPSHOT_ALL = new ImmutableList.Builder<String>()
            .add("Snapshot").add("Timestamp").add("Status")
            .build();
    public static final ImmutableList<String> SNAPSHOT_DETAIL = new ImmutableList.Builder<String>()
            .add("Snapshot").add("Timestamp").add("Database").add("Details").add("Status")
            .build();

    private final String repoName;
    private final Expr where;
    private String snapshotName;
    private String timestamp;
    private List<String> snapshotNames;

    public ShowSnapshotStmt(String repoName, Expr where) {
        this(repoName, where, NodePosition.ZERO);
    }

    public ShowSnapshotStmt(String repoName, Expr where, NodePosition pos) {
        super(pos);
        this.repoName = repoName;
        this.where = where;
        this.snapshotNames = Lists.newArrayList();
    }

    public List<String> getSnapshotNames() {
        return this.snapshotNames;
    }

    public String getRepoName() {
        return repoName;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public Expr getWhere() {
        return where;
    }

    public void setSnapshotName(String snapshotName) {
        this.snapshotName = snapshotName;
    }

    public void addSnapshotName(String snapshotName) {
        this.snapshotNames.add(snapshotName);
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowSnapshotStatement(this, context);
    }
}

