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

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class CreateDataCacheRuleStmt extends DdlStmt {

    private final QualifiedName target;
    private final Expr predicates;
    private final int priority;
    private final Map<String, String> properties;

    public CreateDataCacheRuleStmt(QualifiedName target, Expr predicates,
                                   int priority, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.target = target;
        this.predicates = predicates;
        this.priority = priority;
        this.properties = properties;
    }

    public QualifiedName getTarget() {
        return target;
    }

    public int getPriority() {
        return priority;
    }

    public Expr getPredicates() {
        return predicates;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDataCacheRuleStatement(this, context);
    }
}
