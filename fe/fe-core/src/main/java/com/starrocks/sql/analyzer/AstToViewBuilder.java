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

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.ast.MapExpr;

import java.util.stream.Collectors;

public class AstToViewBuilder {
    public static String toSQL(ParseNode statement) {
        return new AstToViewBuilder.AST2ViewBuilderVisitor(false, false).visit(statement);
    }

    public static class AST2ViewBuilderVisitor extends AstToSQLBuilder.AST2SQLBuilderVisitor {
        public AST2ViewBuilderVisitor(boolean simple, boolean withoutTbl) {
            super(simple, withoutTbl);
        }

        @Override
        public String visitArrayExpr(ArrayExpr node, Void context) {
            StringBuilder sb = new StringBuilder();
            Type type = AnalyzerUtils.replaceNullType2Boolean(node.getType());
            sb.append(type.toString());
            sb.append('[');
            sb.append(node.getChildren().stream().map(this::visit).collect(Collectors.joining(", ")));
            sb.append(']');
            return sb.toString();
        }

        @Override
        public String visitMapExpr(MapExpr node, Void context) {
            StringBuilder sb = new StringBuilder();
            Type type = AnalyzerUtils.replaceNullType2Boolean(node.getType());
            sb.append(type.toString());
            sb.append("{");
            for (int i = 0; i < node.getChildren().size(); i = i + 2) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(visit(node.getChild(i)) + ":" + visit(node.getChild(i + 1)));
            }
            sb.append("}");
            return sb.toString();
        }
    }
}
