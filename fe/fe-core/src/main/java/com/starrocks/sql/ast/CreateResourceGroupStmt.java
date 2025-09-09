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

import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Map;

// ReesourceGroup create statement format
// create resource group [if not exists] [or replace] <name>
// to
//  (user='foobar1', role='foo1', query_type in ('select'), source_ip='192.168.1.1/24'),
//  (user='foobar2', role='foo2', query_type in ('insert'), source_ip='192.168.2.1/24')
// with ('cpu_core_limit'='n', 'mem_limit'='m%', 'concurrency_limit'='n', 'type' = 'normal');
//
public class CreateResourceGroupStmt extends DdlStmt {
    private final String name;
    private final boolean ifNotExists;
    private final boolean replaceIfExists;
    private final List<List<Predicate>> classifiers;
    private final Map<String, String> properties;

    public CreateResourceGroupStmt(String name, boolean ifNotExists, boolean replaceIfExists,
                                   List<List<Predicate>> classifiers, Map<String, String> properties) {
        this(name, ifNotExists, replaceIfExists, classifiers, properties, NodePosition.ZERO);
    }

    public CreateResourceGroupStmt(String name, boolean ifNotExists, boolean replaceIfExists,
                                   List<List<Predicate>> classifiers, Map<String, String> properties,
                                   NodePosition pos) {
        super(pos);
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.replaceIfExists = replaceIfExists;
        this.classifiers = classifiers;
        this.properties = properties;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isReplaceIfExists() {
        return replaceIfExists;
    }

    public String getName() {
        return name;
    }

    public List<List<Predicate>> getClassifiers() {
        return classifiers;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitCreateResourceGroupStatement(this, context);
    }
}
