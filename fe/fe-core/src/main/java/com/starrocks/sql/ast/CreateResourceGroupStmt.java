// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Predicate;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.sql.analyzer.ResourceGroupAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.thrift.TWorkGroupType;

import java.util.ArrayList;
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
    private String name;
    private boolean ifNotExists;
    private boolean replaceIfExists;
    private List<List<Predicate>> classifiers;
    private Map<String, String> properties;
    private ResourceGroup resourceGroup;

    public CreateResourceGroupStmt(String name, boolean ifNotExists, boolean replaceIfExists,
                                   List<List<Predicate>> classifiers, Map<String, String> proeprties) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.replaceIfExists = replaceIfExists;
        this.classifiers = classifiers;
        this.properties = proeprties;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean isReplaceIfExists() {
        return replaceIfExists;
    }

    public void setReplaceIfExists(boolean replaceIfExists) {
        this.replaceIfExists = replaceIfExists;
    }

    public void analyze() throws SemanticException {
        resourceGroup = new ResourceGroup();
        resourceGroup.setName(name);
        List<ResourceGroupClassifier> classifierList = new ArrayList<>();
        for (List<Predicate> predicates : classifiers) {
            ResourceGroupClassifier classifier = ResourceGroupAnalyzer.convertPredicateToClassifier(predicates);
            classifierList.add(classifier);
        }
        resourceGroup.setClassifiers(classifierList);
        ResourceGroupAnalyzer.analyzeProperties(resourceGroup, properties);

        if (resourceGroup.getResourceGroupType() == null) {
            resourceGroup.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        }
        if (resourceGroup.getCpuCoreLimit() == null) {
            throw new SemanticException("property 'cpu_core_limit' is absent");
        }
        if (resourceGroup.getMemLimit() == null) {
            throw new SemanticException("property 'mem_limit' is absent");
        }
    }

    public ResourceGroup getResourceGroup() {
        return resourceGroup;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateResourceGroupStatement(this, context);
    }
}
