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

import com.starrocks.analysis.LimitElement;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.StatementBase;

//Used to build sql digests
public class SqlDigestBuilder {
    public static String build(StatementBase statement) {
        return new SqlDigestBuilderVisitor().visit(statement);
    }

    private static class SqlDigestBuilderVisitor extends AstToStringBuilder.AST2StringBuilderVisitor {
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
