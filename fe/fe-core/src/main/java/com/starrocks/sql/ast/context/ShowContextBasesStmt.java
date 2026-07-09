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

package com.starrocks.sql.ast.context;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.parser.NodePosition;

public class ShowContextBasesStmt extends ShowStmt {

    private final String likePattern;

    public ShowContextBasesStmt(String likePattern, NodePosition pos) {
        super(pos);
        this.likePattern = likePattern;
    }

    public String getLikePattern() {
        return likePattern;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitShowContextBasesStatement(this, context);
    }

    @Override
    public String toSql() {
        return likePattern == null ? "SHOW CONTEXTBASES"
                : "SHOW CONTEXTBASES LIKE '" + likePattern.replace("'", "''") + "'";
    }
}
