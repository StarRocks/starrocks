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
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

/**
 * {@code CONTEXT UPSERT INTO {collection} ENTITY (...) [EDGES (...)] [OPTIONS (...)]}.
 * {@code entityArgs} carries the named-argument payload (entity_key, entity_type, title, preview, content, ...).
 * {@code edges} is the compatibility input for explicit refs — internally treated as reference rows, not graph edges.
 */
public class ContextUpsertStmt extends DdlStmt {

    private final ContextCollectionName collection;
    private final Map<String, Expr> entityArgs;
    private final List<Expr> edges;
    private final Map<String, Expr> options;

    public ContextUpsertStmt(ContextCollectionName collection,
                             Map<String, Expr> entityArgs,
                             List<Expr> edges,
                             Map<String, Expr> options,
                             NodePosition pos) {
        super(pos);
        this.collection = collection;
        this.entityArgs = entityArgs;
        this.edges = edges;
        this.options = options;
    }

    public ContextCollectionName getCollection() {
        return collection;
    }

    public Map<String, Expr> getEntityArgs() {
        return entityArgs;
    }

    public List<Expr> getEdges() {
        return edges;
    }

    public Map<String, Expr> getOptions() {
        return options;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitContextUpsertStatement(this, context);
    }

    @Override
    public String toSql() {
        return ContextStmtFormatter.contextUpsert(collection, entityArgs, edges, options);
    }
}
