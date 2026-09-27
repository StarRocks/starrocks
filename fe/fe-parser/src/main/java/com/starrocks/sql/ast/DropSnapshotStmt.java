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

import com.starrocks.sql.parser.NodePosition;

public class DropSnapshotStmt extends DdlStmt {

    private final String snapshotName;
    private final String repoName;
    // With FORCE the snapshot is dropped without checking which cluster wrote it.
    private final boolean forceDrop;

    public DropSnapshotStmt(String snapshotName, String repoName, boolean forceDrop) {
        this(snapshotName, repoName, forceDrop, NodePosition.ZERO);
    }

    public DropSnapshotStmt(String snapshotName, String repoName, boolean forceDrop, NodePosition pos) {
        super(pos);
        this.snapshotName = snapshotName;
        this.repoName = repoName;
        this.forceDrop = forceDrop;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getRepoName() {
        return repoName;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropSnapshotStatement(this, context);
    }
}
