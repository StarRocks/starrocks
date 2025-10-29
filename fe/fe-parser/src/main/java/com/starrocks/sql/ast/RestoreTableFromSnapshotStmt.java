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

import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;
import java.util.Objects;

public class RestoreTableFromSnapshotStmt extends DdlStmt {
    private final TableRef sourceTable;
    private final String snapshotName;
    private final TableRef targetTable;
    private final Map<String, String> properties;

    public RestoreTableFromSnapshotStmt(TableRef sourceTable, String snapshotName,
                                        TableRef targetTable, Map<String, String> properties) {
        this(sourceTable, snapshotName, targetTable, properties, NodePosition.ZERO);
    }

    public RestoreTableFromSnapshotStmt(TableRef sourceTable, String snapshotName,
                                        TableRef targetTable, Map<String, String> properties,
                                        NodePosition pos) {
        super(pos);
        this.sourceTable = Objects.requireNonNull(sourceTable, "sourceTable");
        this.snapshotName = Objects.requireNonNull(snapshotName, "snapshotName");
        this.targetTable = Objects.requireNonNull(targetTable, "targetTable");
        this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    }

    public TableRef getSourceTable() {
        return sourceTable;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public TableRef getTargetTable() {
        return targetTable;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRestoreTableFromSnapshotStatement(this, context);
    }
}
