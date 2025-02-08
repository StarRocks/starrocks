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

package com.starrocks.sql.spm;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.SlotRef;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.sql.analyzer.AstToSQLBuilder;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Why not use SqlDigestBuilder?
 * SPM need special spm function to serialize/deserialize digest
 */
public class SPMAst2SQLBuilder {
    private boolean enableHints = false;

    private boolean enableDigest = false;

    private final Builder builder = new Builder();

    // for SQLPlanHash
    private final Set<Long> tables = Sets.newHashSet();

    private int aggCount = 0;

    private int joinCount = 0;

    public SPMAst2SQLBuilder(boolean enableHints, boolean enableDigest) {
        this.enableHints = enableHints;
        this.enableDigest = enableDigest;
    }

    public String build(QueryRelation statement) {
        return statement.accept(builder, null);
    }

    public String build(QueryStatement statement) {
        return statement.accept(builder, null);
    }

    public long buildHash() {
        // low 32 bits: 6bit(agg)-6bit(join)-6bit(tableSize)-14bit(tableId)
        // high 32 bits: reserved
        long hash = 0;
        hash |= aggCount;
        hash <<= 6;
        hash |= joinCount;
        hash <<= 6;
        hash |= tables.size();
        hash <<= 14;
        long tableHash = tables.stream().sorted().reduce(1L, (a, b) -> 31 * a + b);
        tableHash &= (1L << 15) - 1;
        hash |= tableHash;
        return hash;
    }

    // need deserialize to SQL, so extends from AstToSQLBuilder, not AstToStringBuilder/SqlDigestBuilder
    private class Builder extends AstToSQLBuilder.AST2SQLBuilderVisitor {
        protected Builder() {
            super(false, false, true);
        }

        @Override
        protected String printWithParentheses(ParseNode node) {
            if (node instanceof SlotRef || node instanceof LiteralExpr || SPMFunctions.isSPMFunctions((Expr) node)) {
                return visit(node);
            } else {
                return "(" + visit(node) + ")";
            }
        }

        @Override
        public String visitTable(TableRelation node, Void outerScope) {
            tables.add(node.getTable().getId());
            return super.visitTable(node, outerScope);
        }

        @Override
        public String visitJoin(JoinRelation relation, Void context) {
            joinCount++;
            String join;
            if (enableHints) {
                join = super.visitJoin(relation, context);
            } else {
                String hints = relation.getJoinHint();
                relation.setJoinHint(null);
                join = super.visitJoin(relation, context);
                relation.setJoinHint(hints);
            }
            return join;
        }

        @Override
        public String visitSelect(SelectRelation stmt, Void context) {
            if (stmt.hasGroupByClause()) {
                aggCount++;
            }

            return super.visitSelect(stmt, context);
        }

        @Override
        protected List<String> visitSelectItemList(SelectRelation stmt) {
            List<String> selectListString = new ArrayList<>();
            for (SelectListItem item : stmt.getSelectList().getItems()) {
                if (item.isStar()) {
                    if (item.getTblName() != null) {
                        selectListString.add(item.getTblName() + ".*");
                    } else {
                        selectListString.add("*");
                    }
                } else if (item.getExpr() != null) {
                    Expr expr = item.getExpr();
                    String str = visit(expr);
                    if (StringUtils.isNotEmpty(item.getAlias())) {
                        str += " AS " + ParseUtil.backquote(item.getAlias());
                    }
                    selectListString.add(str);
                }
            }
            return selectListString;
        }

        @Override
        public String visitInPredicate(InPredicate node, Void context) {
            if (enableDigest && node.getChildren().stream().allMatch(SPMFunctions::isSPMFunctions)) {
                Preconditions.checkState(node.getChildren().size() == 2);
                StringBuilder strBuilder = new StringBuilder();
                String notStr = (node.isNotIn()) ? "NOT " : "";
                strBuilder.append(printWithParentheses(node.getChild(0))).append(" ").append(notStr).append("IN ");
                strBuilder.append("(?)");
                return strBuilder.toString();
            }
            return super.visitInPredicate(node, context);
        }

        @Override
        public String visitFunctionCall(FunctionCallExpr node, Void context) {
            if (SPMFunctions.isSPMFunctions(node)) {
                if (enableDigest) {
                    return "?";
                }
                List<String> children = node.getChildren().stream().map(this::visit).toList();
                return SPMFunctions.toSQL(node.getFnName().getFunction(), children);
            }
            return super.visitFunctionCall(node, context);
        }

        @Override
        public String visitLiteral(LiteralExpr expr, Void context) {
            if (enableDigest) {
                return "?";
            }
            return super.visitLiteral(expr, context);
        }

        @Override
        public String visitLimitElement(LimitElement node, Void context) {
            if (!enableDigest) {
                return visitLimitElement(node, context);
            }
            if (node.hasLimit()) {
                StringBuilder sb = new StringBuilder(" LIMIT ");
                if (node.hasOffset()) {
                    sb.append(" ?, ");
                }
                sb.append(" ? ");
                return sb.toString();
            }
            return "";
        }
    }
}