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

package com.starrocks.sql.common;

import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;

public class DebugRelationTracer implements AstVisitor<String, String> {
    @Override
    public String visit(ParseNode node) {
        return node == null ? "null" : node.toSql();
    }

    @Override
    public String visit(ParseNode node, String indent) {
        return node == null ? "null" : node.accept(this, indent);
    }

    @Override
    public String visitQueryStatement(QueryStatement statement, String indent) {
        return "QueryStatement{\n" + indent + "  queryRelation=" +
                visit(statement.getQueryRelation(), indent + "  ") +
                "\n" +
                indent + "}";
    }

    @Override
    public String visitSelect(SelectRelation node, String indent) {
        return "SelectRelation{\n" + indent + "  selectList=" + node.getSelectList() + "\n" +
                indent + "  fromRelation=" +
                visit(node.getRelation(), indent + "  ") + "\n" +
                indent + "  predicate=" +
                visit(node.getPredicate()) + "\n" +
                indent + "  groupByClause=" +
                visit(node.getGroupByClause()) + "\n" +
                indent + "  having=" +
                visit(node.getHaving()) + "\n" +
                indent + "  sortClause=" +
                node.getOrderBy() + "\n" +
                indent + "  limit=" +
                visit(node.getLimit()) + "\n" +
                indent + "}";
    }

    @Override
    public String visitJoin(JoinRelation node, String indent) {
        return "JoinRelation{" + "joinType=" + node.getJoinOp() +
                ", left=" + visit(node.getLeft(), indent) +
                ", right=" + visit(node.getRight(), indent) +
                ", onPredicate=" + node.getOnPredicate() +
                "}";
    }

    @Override
    public String visitSubqueryRelation(SubqueryRelation node, String indent) {
        return "SubqueryRelation{\n" + indent + "  alias=" +
                (node.getAlias() == null ? "anonymous" : node.getAlias()) + "\n" +
                indent + "  query=" +
                visit(node.getQueryStatement(), indent + "  ") + "\n" +
                indent + "}";
    }

    @Override
    public String visitUnion(UnionRelation node, String indent) {
        StringBuilder sb = new StringBuilder("UnionRelation{\n");
        sb.append(indent).append("relations=\n");
        for (QueryRelation relation : node.getRelations()) {
            sb.append(indent).append("  ").append(visit(relation, indent + "  ")).append("\n");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String visitTable(TableRelation node, String indent) {
        return node.toString();
    }
}
