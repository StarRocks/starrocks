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

// Minimal carrier so the equality-delete -> position-delete conversion procedure can drive its
// pre-built ExecPlan through StmtExecutor.handleDMLStmt. Deliberately not a DeleteStmt: that path
// has delete-specific invariants (shouldHandledByDeleteHandler, isIcebergMetadataDelete) and a
// mandatory WHERE clause that conversion does not want. handleDMLStmt routes the commit by the
// plan's sink type (IcebergDeleteSink), so this carrier only needs to name the target table and
// fall through to the generic Iceberg-sink branch.
public class ConvertEqualityDeletesStmt extends DmlStmt {
    private final TableRef tableRef;

    public ConvertEqualityDeletesStmt(TableRef tableRef) {
        super(NodePosition.ZERO);
        this.tableRef = tableRef;
    }

    @Override
    public TableRef getTableRef() {
        return tableRef;
    }

    // Dispatch to the generic statement visitor. The carrier is never analyzed, but visitors that walk
    // a statement (e.g. AnalyzerUtils.collectAllDatabase via isStatisticsJob) still call accept, and the
    // default ParseNode.accept throws. Generic dispatch keeps those walks no-op for this carrier.
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitStatement(this, context);
    }
}
