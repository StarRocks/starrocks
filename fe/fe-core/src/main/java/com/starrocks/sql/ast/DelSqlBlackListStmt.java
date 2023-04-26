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

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;

// use for delete sql's blacklist by ids.
// indexs is the ids of regular expression's sql
public class DelSqlBlackListStmt extends StatementBase {

    private final List<Long> indexs;

    public List<Long> getIndexs() {
        return indexs;
    }

    public DelSqlBlackListStmt(List<Long> indexs) {
        this(indexs, NodePosition.ZERO);
    }

    public DelSqlBlackListStmt(List<Long> indexs, NodePosition pos) {
        super(pos);
        this.indexs = indexs;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDelSqlBlackListStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}

