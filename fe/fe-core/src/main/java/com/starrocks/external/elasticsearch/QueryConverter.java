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


package com.starrocks.external.elasticsearch;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.BoolLiteral;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FloatLiteral;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.thrift.TExprOpcode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryConverter extends AstVisitor<QueryBuilders.QueryBuilder, Void> {

    // expr sets which can not be pushed down to Elasticsearch, SR BE should process
    List<Expr> localConjuncts = new ArrayList<>();

    // expr sets which can be pushed down to Elasticsearch, SR BE should not process
    List<Expr> remoteConjuncts = new ArrayList<>();

    public List<Expr> localConjuncts() {
        return localConjuncts;
    }

    public List<Expr> remoteConjuncts() {
        return remoteConjuncts;
    }

    // used for test
    public QueryBuilders.QueryBuilder convert(Expr conjunct) {
        return visit(conjunct);
    }

    public QueryBuilders.QueryBuilder convert(List<Expr> conjuncts) {
        if (conjuncts == null || conjuncts.size() == 0) {
            return QueryBuilders.matchAllQuery();
        }
        QueryBuilders.BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (Expr conjunct : conjuncts) {
            // maybe use filter is better, but at this time we keep consistent with BE's logic
            try {
                boolQueryBuilder.must(visit(conjunct));
                remoteConjuncts.add(conjunct.clone());
            } catch (Exception e) {
                localConjuncts.add(conjunct.clone());
            }
        }
        if (remoteConjuncts.size() == 0) {
            return QueryBuilders.matchAllQuery();
        }
        return boolQueryBuilder;
    }

    @Override
    public QueryBuilders.QueryBuilder visitCompoundPredicate(CompoundPredicate node, Void context) {
        switch (node.getOp()) {
            case AND: {
                QueryBuilders.QueryBuilder left = node.getChild(0).accept(this, context);
                QueryBuilders.QueryBuilder right = node.getChild(1).accept(this, context);
                if (left != null && right != null) {
                    return QueryBuilders.boolQuery().must(left).must(right);
                }
                throw new StarRocksESException("process compound and failure ");
            }
            case OR: {
                QueryBuilders.QueryBuilder left = node.getChild(0).accept(this, context);
                QueryBuilders.QueryBuilder right = node.getChild(1).accept(this, context);
                if (left != null && right != null) {
                    return QueryBuilders.boolQuery().should(left).should(right);
                }
                throw new StarRocksESException("process compound or failure ");
            }
            case NOT: {
                QueryBuilders.QueryBuilder child = node.getChild(0).accept(this, context);
                if (child != null) {
                    return QueryBuilders.boolQuery().mustNot(child);
                }
                throw new StarRocksESException("process compound not failure ");
            }
            default:
                throw new StarRocksESException("currently only support compound and/or/not");
        }
    }

    @Override
    public QueryBuilders.QueryBuilder visitBinaryPredicate(BinaryPredicate node, Void context) {
        String column = null;
        Object value = null;
        // process BinaryPredicate
        if (node.getChild(0) instanceof SlotRef || node.getChild(0) instanceof CastExpr) {
            column = getColumnName(exprWithoutCast(node.getChild(0)));
            value = valueFor(node.getChild(1));
        } else if (node.getChild(1) instanceof SlotRef || node.getChild(1) instanceof CastExpr) {
            column = getColumnName(exprWithoutCast(node.getChild(1)));
            value = valueFor(node.getChild(0));
        }
        TExprOpcode opCode = node.getOpcode();
        switch (opCode) {
            case EQ:
                return QueryBuilders.termQuery(column, value);
            case NE:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(column, value));
            case GE:
                return QueryBuilders.rangeQuery(column).gte(value);
            case GT:
                return QueryBuilders.rangeQuery(column).gt(value);
            case LE:
                return QueryBuilders.rangeQuery(column).lte(value);
            case LT:
                return QueryBuilders.rangeQuery(column).lt(value);
            default:
                throw new StarRocksESException("can not support " + opCode + " in BinaryPredicate");
        }
    }

    @Override
    public QueryBuilders.QueryBuilder visitIsNullPredicate(IsNullPredicate node, Void context) {
        String column = getColumnName(exprWithoutCast(node.getChild(0)));
        if (node.isNotNull()) {
            return QueryBuilders.existsQuery(column);
        }
        return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(column));
    }

    @Override
    public QueryBuilders.QueryBuilder visitLikePredicate(LikePredicate node, Void context) {
        String column = null;
        String value = null;
        // process BinaryPredicate
        if (node.getChild(0) instanceof SlotRef || node.getChild(0) instanceof CastExpr) {
            column = getColumnName(exprWithoutCast(node.getChild(0)));
            value = (String) valueFor(node.getChild(1));
        } else if (node.getChild(1) instanceof SlotRef || node.getChild(1) instanceof CastExpr) {
            column = getColumnName(exprWithoutCast(node.getChild(1)));
            value = (String) valueFor(node.getChild(0));
        }
        char[] chars = value.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == '_' || chars[i] == '%') {
                if (i == 0 || chars[i - 1] != '\\') {
                    chars[i] = (chars[i] == '_') ? '?' : '*';
                }
            }
        }
        return QueryBuilders.wildcardQuery(column, new String(chars));
    }

    @Override
    public QueryBuilders.QueryBuilder visitInPredicate(InPredicate node, Void context) {
        String column = ((SlotRef) exprWithoutCast(node.getChild(0))).getDesc().getColumn().getName();
        List<Object> values = node.getListChildren()
                .stream()
                .map(QueryConverter::valueFor)
                .collect(Collectors.toList());
        if (node.isNotIn()) {
            return QueryBuilders.boolQuery().mustNot(QueryBuilders.termsQuery(column, values));
        }
        return QueryBuilders.termsQuery(column, values);
    }

    @Override
    public QueryBuilders.QueryBuilder visitFunctionCall(FunctionCallExpr node, Void context) {
        if ("esquery".equals(node.getFnName().getFunction())) {
            String stringValue = ((StringLiteral) node.getChild(1)).getStringValue();
            return new QueryBuilders.RawQueryBuilder(stringValue);
        } else {
            throw new StarRocksESException("can not support function { " + node.getFnName() + " }");
        }
    }

    private static String getColumnName(Expr expr) {
        if (expr == null) {
            throw new StarRocksESException("internal exception: expr is null");
        }
        String columnName = new QueryConverter.ExtractColumnName().visit(expr, null);
        if (columnName == null || columnName.isEmpty()) {
            throw new StarRocksESException("column's name must be not null or empty");
        }
        return columnName;
    }

    private static class ExtractColumnName extends AstVisitor<String, Void> {
        @Override
        public String visitCastExpr(CastExpr node, Void context) {
            return node.getChild(0).accept(this, null);
        }

        @Override
        public String visitSlot(SlotRef node, Void context) {
            return node.getColumn().getName();
        }
    }

    private static Object valueFor(Expr expr) {
        if (!expr.isLiteral()) {
            throw new StarRocksESException("can not get literal value from " + expr);
        }
        if (expr instanceof BoolLiteral) {
            return ((BoolLiteral) expr).getValue();
        } else if (expr instanceof IntLiteral) {
            return ((IntLiteral) expr).getValue();
        } else if (expr instanceof LargeIntLiteral) {
            return ((LargeIntLiteral) expr).getValue();
        } else if (expr instanceof FloatLiteral) {
            return ((FloatLiteral) expr).getValue();
        } else if (expr instanceof DecimalLiteral) {
            return ((DecimalLiteral) expr).getValue();
        }
        return ((LiteralExpr) expr).getStringValue();
    }

    // k1 = 2 (k1 is float), conjunct->get_child(0)->node_type() return CAST_EXPR
    // SR ignore this cast transformation, push down this `Cast Operation` to Elasticsearch
    static Expr exprWithoutCast(Expr expr) {
        if (expr instanceof CastExpr) {
            return exprWithoutCast(expr.getChild(0));
        }
        return expr;
    }

}
