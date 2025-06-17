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

import com.google.common.base.Joiner;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.ValuesRelation;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Used to build sql digest(string without any dynamic parameters in it)
 */
public class SqlDigestBuilder {

    private static final int MASSIVE_COMPOUND_LIMIT = 16;

    public static String build(StatementBase statement) {
        return new SqlDigestBuilderVisitor().visit(statement);
    }

    private static class SqlDigestBuilderVisitor extends AstToStringBuilder.AST2StringBuilderVisitor {

        @Override
        public String visitInPredicate(InPredicate node, Void context) {
            if (!node.isConstantValues()) {
                return super.visitInPredicate(node, context);
            } else {
                StringBuilder strBuilder = new StringBuilder();
                String notStr = (node.isNotIn()) ? "NOT " : "";
                strBuilder.append(printWithParentheses(node.getChild(0))).append(" ").append(notStr).append("IN ");
                strBuilder.append("(?)");
                return strBuilder.toString();
            }
        }

        @Override
        public String visitCompoundPredicate(CompoundPredicate node, Void context) {
            List<Expr> flatten = AnalyzerUtils.flattenPredicate(node);
            if (flatten.size() >= MASSIVE_COMPOUND_LIMIT) {
                // Only record de-duplicated slots if there are too many compounds
                List<SlotRef> exprs = node.collectAllSlotRefs(true);
                String sortedSlots = exprs.stream()
                        .filter(SlotRef::isColumnRef)
                        .map(SlotRef::toSqlImpl)
                        .sorted()
                        .collect(Collectors.joining(","));
                return "$massive_compounds[" + sortedSlots + "]$";
            } else {
                // TODO: it will introduce a little bit overhead in top-down visiting, in which the
                //  flattenPredicate is duplicated revoked. it's better to eliminate this overhead
                return super.visitCompoundPredicate(node, context);
            }
        }

        @Override
        public String visitValues(ValuesRelation node, Void scope) {
            if (node.isNullValues()) {
                return "VALUES(NULL)";
            }

            StringBuilder sqlBuilder = new StringBuilder("VALUES");
            if (!node.getRows().isEmpty()) {
                StringBuilder rowBuilder = new StringBuilder();
                rowBuilder.append("(");
                List<String> rowStrings =
                        node.getRows().get(0).stream().map(this::visit).collect(Collectors.toList());
                rowBuilder.append(Joiner.on(", ").join(rowStrings));
                rowBuilder.append(")");
                sqlBuilder.append(rowBuilder.toString());
            }
            return sqlBuilder.toString();
        }

        @Override
        protected void visitInsertLabel(String label, StringBuilder sb) {
            if (StringUtils.isNotEmpty(label)) {
                sb.append("WITH LABEL ? ");
            }
        }

        @Override
        public String visitLiteral(LiteralExpr expr, Void context) {
            return "?";
        }

        @Override
        public String visitLimitElement(LimitElement node, Void context) {
            if (node.getLimit() == -1) {
                return "";
            }
            StringBuilder sb = new StringBuilder(" LIMIT ");
            if (node.getOffset() != 0) {
                sb.append(" ?, ");
            }
            sb.append(" ? ");
            return sb.toString();
        }
    }
}
