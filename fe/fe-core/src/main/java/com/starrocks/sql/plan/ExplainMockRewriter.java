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

import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Post-processes rendered explain output and mocked SQL for `EXPLAIN COSTS MOCK`,
 * replacing each column reference name with a stable `mock_col_<N>`.
 *
 * Building the mapping from the optimizer's ColumnRefFactory keeps all rendering
 * code paths (PlanNode, expression visitors, AstToSQLBuilder) untouched - the
 * substitution happens once on the final string. Numbering is by ColumnRefOperator
 * id so the same column always maps to the same mock name within a query.
 *
 * Limitations: substitution is whole-word, case-insensitive over the column-name
 * set. Column names that collide with SQL keywords, function names, or other
 * identifiers in the output may be over-replaced; this is acceptable for an
 * opt-in MOCK explain since it errs on the side of hiding more.
 */
public final class ExplainMockRewriter {

    private final Map<String, String> nameToMock;
    private final Pattern pattern;

    public ExplainMockRewriter(ColumnRefFactory factory) {
        Map<String, String> mapping = new LinkedHashMap<>();
        if (factory != null) {
            List<ColumnRefOperator> refs = new ArrayList<>(factory.getColumnRefs());
            refs.sort(Comparator.comparingInt(ColumnRefOperator::getId));
            int seq = 1;
            for (ColumnRefOperator col : refs) {
                String name = col.getName();
                if (name == null || name.isEmpty()) {
                    continue;
                }
                String key = name.toLowerCase();
                if (!mapping.containsKey(key)) {
                    mapping.put(key, "mock_col_" + seq++);
                }
            }
        }
        this.nameToMock = mapping;
        if (mapping.isEmpty()) {
            this.pattern = null;
        } else {
            // Order alternatives by length descending so longer column names take
            // precedence over shorter ones that may share a prefix.
            String alternation = mapping.keySet().stream()
                    .sorted(Comparator.comparingInt(String::length).reversed())
                    .map(Pattern::quote)
                    .collect(Collectors.joining("|"));
            this.pattern = Pattern.compile("(?i)\\b(" + alternation + ")\\b");
        }
    }

    public String rewrite(String input) {
        if (pattern == null || input == null || input.isEmpty()) {
            return input;
        }
        Matcher m = pattern.matcher(input);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String mock = nameToMock.get(m.group(1).toLowerCase());
            m.appendReplacement(sb, Matcher.quoteReplacement(mock == null ? m.group() : mock));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public Map<String, String> getMapping() {
        return nameToMock;
    }
}
