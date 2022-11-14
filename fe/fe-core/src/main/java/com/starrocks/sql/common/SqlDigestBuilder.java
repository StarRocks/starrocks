// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.common;

import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.sql.analyzer.AST2SQL;
import com.starrocks.sql.ast.StatementBase;

//Used to build sql digests
public class SqlDigestBuilder {
    public static String build(StatementBase statement) {
        return new SqlDigestBuilderVisitor().visit(statement);
    }

    private static class SqlDigestBuilderVisitor extends AST2SQL.SQLBuilder {
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
