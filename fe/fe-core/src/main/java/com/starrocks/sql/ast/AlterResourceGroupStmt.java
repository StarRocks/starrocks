// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Predicate;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.sql.analyzer.ResourceGroupAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;

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
            if (changedProperties.getCpuCoreLimit() == null &&
                    changedProperties.getMemLimit() == null &&
                    changedProperties.getConcurrencyLimit() == null &&
                    changedProperties.getBigQueryCpuSecondLimit() == null &&
                    changedProperties.getBigQueryMemLimit() == null &&
                    changedProperties.getBigQueryScanRowsLimit() == null) {
                throw new SemanticException(
                        "At least one of ('cpu_core_limit', 'mem_limit', 'concurrency_limit','big_query_mem_limit', " +
                                "'big_query_scan_rows_limit', 'big_query_cpu_second_limit', should be specified");
            }
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

    public static class SubCommand {

    }

    public static class AddClassifiers extends SubCommand {
        private List<List<Predicate>> classifiers;

        public AddClassifiers(List<List<Predicate>> classifiers) {
            this.classifiers = classifiers;
        }
    }

    public static class DropClassifiers extends SubCommand {
        private List<Long> classifierIds;

        public DropClassifiers(List<Long> classifierIds) {
            this.classifierIds = classifierIds;
        }
    }

    public static class DropAllClassifiers extends SubCommand {
        public DropAllClassifiers() {
        }
    }

    public static class AlterProperties extends SubCommand {
        private Map<String, String> properties;

        public AlterProperties(Map<String, String> properties) {
            this.properties = properties;
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterResourceGroupStatement(this, context);
    }
}
