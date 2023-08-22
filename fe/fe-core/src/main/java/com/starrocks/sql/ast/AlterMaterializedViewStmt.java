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

import com.starrocks.analysis.TableName;
import com.starrocks.sql.parser.NodePosition;

<<<<<<< HEAD
import java.util.List;
import java.util.Set;

=======
>>>>>>> 1ab67549d4 (Refactor alter materialized view statement executor to eliminate code coupling (#29619))
/**
 * 1.Support for modifying the way of refresh and the cycle of asynchronous refresh;
 * 2.Support for modifying the name of a materialized view;
 * 3.SYNC is not supported and ASYNC is not allow changed to SYNC
 */
public class AlterMaterializedViewStmt extends DdlStmt {
    private final TableName mvName;
<<<<<<< HEAD
    private final String newMvName;
    private final RefreshSchemeDesc refreshSchemeDesc;
    private final ModifyTablePropertiesClause modifyTablePropertiesClause;
    private final String status;
    private final SwapTableClause swapTable;
    private List<AlterClause> ops;
=======
    private final AlterTableClause alterTableClause;
>>>>>>> 1ab67549d4 (Refactor alter materialized view statement executor to eliminate code coupling (#29619))

    public AlterMaterializedViewStmt(TableName mvName, AlterTableClause alterTableClause, NodePosition pos) {
        super(pos);
        this.mvName = mvName;
        this.alterTableClause = alterTableClause;
    }

    public TableName getMvName() {
        return mvName;
    }

    public AlterTableClause getAlterTableClause() {
        return alterTableClause;
    }

    public List<AlterClause> getOps() {
        return ops;
    }

    public void setOps(List<AlterClause> ops) {
        this.ops = ops;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterMaterializedViewStatement(this, context);
    }
}
