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

// Alter ResourceGroup specified by name
// 1. Add a new classifier to the ResourceGroup
//  ALTER RESOURCE GROUP <name> ADD (user='<user>', role='<role>', query_type in (...), source_ip='<cidr>')
//
// 2. Drop present classifiers by their ids
//  ALTER RESOURCE GROUP <name> DROP (<id_1>, <id_2>, ...)
//  ALTER RESOURCE GROUP <name> DROP ALL
//
// 3. Modify properties
//  ALTER RESOURCE GROUP <name> WITH ('cpu_core_limit'='n', 'mem_limit'='m%', 'concurrency_limit'='k')
public class AlterResourceGroupStmt extends DdlStmt {
    private final String name;
    private final SubCommand cmd;

    public AlterResourceGroupStmt(String name, SubCommand cmd) {
        this(name, cmd, NodePosition.ZERO);
    }

    public AlterResourceGroupStmt(String name, SubCommand cmd, NodePosition pos) {
        super(pos);
        this.name = name;
        this.cmd = cmd;
    }

    public String getName() {
        return this.name;
    }

    public SubCommand getCmd() {
        return this.cmd;
    }

    public static class SubCommand implements ParseNode {

        protected final NodePosition pos;

        public SubCommand() {
            this(NodePosition.ZERO);
        }

        public SubCommand(NodePosition pos) {
            this.pos = pos;
        }

        @Override
        public NodePosition getPos() {
            return pos;
        }
    }

    public static class AddClassifiers extends SubCommand {
        private List<List<Predicate>> classifiers;

        public AddClassifiers(List<List<Predicate>> classifiers) {
            this(classifiers, NodePosition.ZERO);
        }

        public AddClassifiers(List<List<Predicate>> classifiers, NodePosition pos) {
            super(pos);
            this.classifiers = classifiers;
        }

        public List<List<Predicate>> getClassifiers() {
            return classifiers;
        }
    }

    public static class DropClassifiers extends SubCommand {
        private List<Long> classifierIds;

        public DropClassifiers(List<Long> classifierIds) {
            this(classifierIds, NodePosition.ZERO);
        }

        public DropClassifiers(List<Long> classifierIds, NodePosition pos) {
            super(pos);
            this.classifierIds = classifierIds;
        }

        public List<Long> getClassifierIds() {
            return classifierIds;
        }
    }

    public static class DropAllClassifiers extends SubCommand {

        public DropAllClassifiers() {
            this(NodePosition.ZERO);
        }

        public DropAllClassifiers(NodePosition pos) {
            super(pos);
        }
    }

    public static class AlterProperties extends SubCommand {
        private Map<String, String> properties;

        public AlterProperties(Map<String, String> properties) {
            this(properties, NodePosition.ZERO);
        }

        public AlterProperties(Map<String, String> properties, NodePosition pos) {
            super(pos);
            this.properties = properties;
        }

        public Map<String, String> getProperties() {
            return properties;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitAlterResourceGroupStatement(this, context);
    }
}
