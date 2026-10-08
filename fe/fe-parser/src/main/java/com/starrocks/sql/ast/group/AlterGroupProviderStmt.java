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

package com.starrocks.sql.ast.group;

import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.DdlStmt;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

// clause which is used to alter the properties of an existing group provider
public class AlterGroupProviderStmt extends DdlStmt {
    private final String name;
    private final Map<String, String> properties;

    public AlterGroupProviderStmt(String name, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.name = name;
        // Copied, not adopted: AST nodes are immutable after parsing, so neither the caller that
        // built this map nor anyone holding the getter's result may change what analysis validated
        // and execution then journals.
        this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterGroupProviderStatement(this, context);
    }
}
