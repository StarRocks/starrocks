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

import com.google.common.base.Preconditions;
import com.starrocks.analysis.ParseNode;
import com.starrocks.analysis.Predicate;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.sql.analyzer.ResourceGroupAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

import java.util.ArrayList;
import java.util.Collections;
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
    private String name;
    private SubCommand cmd;
    private List<ResourceGroupClassifier> newAddedClassifiers = Collections.emptyList();
    private ResourceGroup changedProperties = new ResourceGroup();

    public AlterResourceGroupStmt(String name, SubCommand cmd) {
        this(name, cmd, NodePosition.ZERO);
    }

    public AlterResourceGroupStmt(String name, SubCommand cmd, NodePosition pos) {
        super(pos);
        this.name = name;
        this.cmd = cmd;
    }

    public SubCommand getCmd() {
        return this.cmd;
    }

    public String getName() {
        return this.name;
    }

    public void analyze() {
        if (cmd instanceof AddClassifiers) {
            AddClassifiers addClassifiers = (AddClassifiers) cmd;
            List<ResourceGroupClassifier> classifierList = new ArrayList<>();
            for (List<Predicate> predicates : addClassifiers.classifiers) {
                ResourceGroupClassifier classifier = ResourceGroupAnalyzer.convertPredicateToClassifier(predicates);
                classifierList.add(classifier);
            }
            newAddedClassifiers = classifierList;
        } else if (cmd instanceof AlterProperties) {
            AlterProperties alterProperties = (AlterProperties) cmd;
            ResourceGroupAnalyzer.analyzeProperties(changedProperties, alterProperties.properties);
            if (changedProperties.getResourceGroupType() != null) {
                throw new SemanticException("type of ResourceGroup is immutable");
            }
            if (changedProperties.getCpuWeight() == null &&
                    changedProperties.getExclusiveCpuCores() == null &&
                    changedProperties.getMemLimit() == null &&
                    changedProperties.getConcurrencyLimit() == null &&
                    changedProperties.getMaxCpuCores() == null &&
                    changedProperties.getBigQueryCpuSecondLimit() == null &&
                    changedProperties.getBigQueryMemLimit() == null &&
                    changedProperties.getBigQueryScanRowsLimit() == null &&
                    changedProperties.getSpillMemLimitThreshold() == null) {
                throw new SemanticException("At least one of ('cpu_weight','exclusive_cpu_cores','mem_limit'," +
                        "'max_cpu_cores','concurrency_limit','big_query_mem_limit', 'big_query_scan_rows_limit'," +
                        "'big_query_cpu_second_limit','spill_mem_limit_threshold') " +
                        "should be specified");
            }
        }

        if (ResourceGroup.BUILTIN_WG_NAMES.contains(name) && !(cmd instanceof AlterProperties)) {
            throw new SemanticException(String.format("cannot alter classifiers of builtin resource group [%s]", name));
        }
    }

    public List<ResourceGroupClassifier> getNewAddedClassifiers() {
        return newAddedClassifiers;
    }

    public ResourceGroup getChangedProperties() {
        return changedProperties;
    }

    public List<Long> getClassifiersToDrop() {
        Preconditions.checkArgument(cmd instanceof DropClassifiers);
        DropClassifiers dropClassifiers = (DropClassifiers) cmd;
        return dropClassifiers.classifierIds;
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
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterResourceGroupStatement(this, context);
    }
}
