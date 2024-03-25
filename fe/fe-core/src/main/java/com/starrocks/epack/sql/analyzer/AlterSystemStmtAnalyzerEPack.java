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
package com.starrocks.epack.sql.analyzer;

import com.starrocks.epack.sql.ast.AddBackendClauseEPack;
import com.starrocks.epack.sql.ast.AddComputeNodeClauseEPack;
import com.starrocks.epack.sql.ast.AstVisitorEPack;
import com.starrocks.epack.sql.ast.DropBackendClauseEPack;
import com.starrocks.epack.sql.ast.DropComputeNodeClauseEPack;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AlterSystemStmtAnalyzer;

public class AlterSystemStmtAnalyzerEPack extends AlterSystemStmtAnalyzer implements AstVisitorEPack<Void, ConnectContext> {
    @Override
    public Void visitAddBackendClause(AddBackendClauseEPack clause, ConnectContext context) {
        return visit(clause.getAddBackendClause(), context);
    }

    @Override
    public Void visitAddComputeNodeClause(AddComputeNodeClauseEPack clause, ConnectContext context) {
        return visit(clause.getAddComputeNodeClause(), context);
    }

    @Override
    public Void visitDropBackendClause(DropBackendClauseEPack clause, ConnectContext context) {
        return visit(clause.getDropBackendClause(), context);
    }

    @Override
    public Void visitDropComputeNodeClause(DropComputeNodeClauseEPack clause, ConnectContext context) {
        return visit(clause.getDropComputeNodeClause(), context);
    }
}
