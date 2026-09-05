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

package com.starrocks.sql.analyzer.mv;

import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;

/**
 * Context for {@link MVBaseTablePartitionHandler#checkPartitionColumn}.
 */
public class MVPartitionCheckContext {
    private final CreateMaterializedViewStatement statement;
    private final Expr partitionByExpr;
    private final SlotRef slotRef;
    private final Table table;

    public MVPartitionCheckContext(CreateMaterializedViewStatement statement,
                                  Expr partitionByExpr,
                                  SlotRef slotRef,
                                  Table table) {
        this.statement = statement;
        this.partitionByExpr = partitionByExpr;
        this.slotRef = slotRef;
        this.table = table;
    }

    public CreateMaterializedViewStatement getStatement() {
        return statement;
    }

    public Expr getPartitionByExpr() {
        return partitionByExpr;
    }

    public SlotRef getSlotRef() {
        return slotRef;
    }

    public Table getTable() {
        return table;
    }
}
