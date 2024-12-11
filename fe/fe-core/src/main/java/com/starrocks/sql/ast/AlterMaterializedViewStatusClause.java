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

import com.google.common.collect.Sets;
import com.starrocks.alter.AlterOpType;
import com.starrocks.sql.parser.NodePosition;

import java.util.Set;

public class AlterMaterializedViewStatusClause extends AlterTableClause {
    public static final String ACTIVE = "active";
    public static final String INACTIVE = "inactive";
    public static final Set<String> SUPPORTED_MV_STATUS = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

    static {
        SUPPORTED_MV_STATUS.add(ACTIVE);
        SUPPORTED_MV_STATUS.add(INACTIVE);
    }

    private final String status;

    public AlterMaterializedViewStatusClause(String status, NodePosition pos) {
        super(AlterOpType.ALTER_MV_STATUS, pos);
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterMaterializedViewStatusClause(this, context);
    }
}
