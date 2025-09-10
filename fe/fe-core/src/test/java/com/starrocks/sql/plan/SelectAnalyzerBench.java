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

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.SelectAnalyzer;
import com.starrocks.sql.ast.expression.Expr;

import java.util.ArrayList;
import java.util.List;

/**
 * Old Deduped size: 1000, time: 501 ms
 * New Deduped size: 1000, time: 18 ms
 */
public class SelectAnalyzerBench {
    // Benchmark test for removeDuplicateField optimization
    // This is a simple micro-benchmark for development/verification purposes.
    // In production, consider using JMH or a proper benchmarking framework.
    public static void main(String[] args) {
        // Mock Field and Expr classes for the benchmark
        class MockExpr extends Expr {
            private final int id;
            MockExpr(int id) {
                super();
                this.id = id;
            }

            @Override
            public Expr clone() {
                return new MockExpr(id);
            }

            @Override public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                MockExpr mockExpr = (MockExpr) o;
                return id == mockExpr.id;
            }
            @Override public int hashCode() { return Integer.hashCode(id); }
        }
        class MockField extends Field {
            private final String name;
            private final MockExpr expr;
            MockField(String name, MockExpr expr) {
                super(name, null, null, null); // Only name and originExpression are used
                this.name = name;
                this.expr = expr;
            }
            @Override public String getName() { return name; }
            @Override public Expr getOriginExpression() { return (Expr) expr; }
        }

        // Generate a large list of fields with duplicates
        int N = 100_000;
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < N; ++i) {
            String name = "col" + (i % 1000); // 1000 unique names
            MockExpr expr = new MockExpr(i % 500); // 500 unique exprs
            fields.add(new MockField(name, expr));
        }

        ConnectContext session = new ConnectContext();

        {
            long start = System.currentTimeMillis();
            List<Field> deduped = removeDuplicateFieldV1(session, fields);
            long end = System.currentTimeMillis();
            System.out.println("Old Deduped size: " + deduped.size() + ", time: " + (end - start) + " ms");
        }
        {
            SelectAnalyzer analyzer = new SelectAnalyzer(session);
            long start = System.currentTimeMillis();
            List<Field> deduped = analyzer.removeDuplicateField(fields);
            long end = System.currentTimeMillis();
            System.out.println("New Deduped size: " + deduped.size() + ", time: " + (end - start) + " ms");
        }
    }

    // The Scope used by order by allows parsing of the same column,
    // such as 'select v1 as v, v1 as v from t0 order by v'
    // but normal parsing does not allow it. So add a de-duplication operation here.
    private static List<Field> removeDuplicateFieldV1(ConnectContext session,
                                                      List<Field> originalFields) {
        List<Field> allFields = Lists.newArrayList();
        for (Field field : originalFields) {
            if (session.getSessionVariable().isEnableStrictOrderBy()) {
                if (field.getName() != null && field.getOriginExpression() != null &&
                        allFields.stream().anyMatch(f -> f.getOriginExpression() != null
                                && f.getName() != null && field.getName().equals(f.getName())
                                && field.getOriginExpression().equals(f.getOriginExpression()))) {
                    continue;
                }
            } else {
                if (field.getName() != null &&
                        allFields.stream().anyMatch(f -> f.getName() != null && field.getName().equals(f.getName()))) {
                    continue;
                }
            }
            allFields.add(field);
        }
        return allFields;
    }
}
