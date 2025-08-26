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

package com.starrocks.sql.ast.spm;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

public class ControlBaselinePlanStmt extends DdlStmt {
    private final boolean isEnable;
    private final List<Long> baseLineId;

    public ControlBaselinePlanStmt(boolean isEnable, List<Long> baseLineId, NodePosition pos) {
        super(pos);
        this.isEnable = isEnable;
        this.baseLineId = baseLineId;
    }

    public boolean isEnable() {
        return isEnable;
    }

    public List<Long> getBaseLineId() {
        return baseLineId;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitControlBaselinePlanStatement(this, context);
    }
}