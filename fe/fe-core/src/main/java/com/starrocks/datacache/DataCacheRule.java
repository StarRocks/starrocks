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

package com.starrocks.datacache;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.ast.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataCacheRule {
    private final long id;
    private final QualifiedName target;
    private final Expr predicates;
    private final int priority;
    private final Map<String, String> properties;

    public DataCacheRule(long id, QualifiedName target, Expr predicates, int priority, Map<String, String> properties) {
        this.id = id;
        this.target = target;
        this.predicates = predicates;
        this.priority = priority;
        this.properties = properties;
    }

    public long getId() {
        return id;
    }

    public QualifiedName getTarget() {
        return target;
    }

    public Expr getPredicates() {
        return predicates;
    }

    public int getPriority() {
        return priority;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public List<String> getShowResultSetRows() {
        List<String> result = new ArrayList<>();
        result.add(String.valueOf(id));
        result.add(target.getParts().get(0));
        result.add(target.getParts().get(1));
        result.add(target.getParts().get(2));
        result.add(String.valueOf(priority));
        if (predicates == null) {
            result.add("NULL");
        } else {
            result.add(predicates.toMySql());
        }
        if (properties == null) {
            result.add("NULL");
        } else {
            result.add(printProperties(properties));
        }
        return result;
    }

    private static String printProperties(Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            sb.append(String.format("\"%s\"=\"%s\"", entry.getKey(), entry.getValue()));
            sb.append(", ");
        }
        if (!properties.isEmpty()) {
            // Remove trailing ", "
            sb.delete(sb.length() - 2, sb.length());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        // TODO properties
        return String.format("[id = %d, target = %s, predicates = %s, priority = %d, properties = %s]", id, target,
                predicates == null ? "NULL" : predicates.toMySql(),
                priority, properties == null ? "NULL" : printProperties(properties));
    }
}
