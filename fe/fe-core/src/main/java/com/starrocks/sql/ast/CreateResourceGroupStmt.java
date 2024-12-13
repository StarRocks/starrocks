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

<<<<<<< HEAD

=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
package com.starrocks.sql.ast;

import com.starrocks.analysis.Predicate;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.sql.analyzer.ResourceGroupAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TWorkGroupType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

<<<<<<< HEAD
=======
import static com.starrocks.catalog.ResourceGroupMgr.SHORT_QUERY_SET_EXCLUSIVE_CPU_CORES_ERR_MSG;

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
// ReesourceGroup create statement format
// create resource group [if not exists] [or replace] <name>
// to
//  (user='foobar1', role='foo1', query_type in ('select'), source_ip='192.168.1.1/24'),
//  (user='foobar2', role='foo2', query_type in ('insert'), source_ip='192.168.2.1/24')
// with ('cpu_core_limit'='n', 'mem_limit'='m%', 'concurrency_limit'='n', 'type' = 'normal');
//
public class CreateResourceGroupStmt extends DdlStmt {
<<<<<<< HEAD
    private String name;
    private boolean ifNotExists;
    private boolean replaceIfExists;
    private List<List<Predicate>> classifiers;
    private Map<String, String> properties;
    private ResourceGroup resourceGroup;

    public CreateResourceGroupStmt(String name, boolean ifNotExists, boolean replaceIfExists,
                                   List<List<Predicate>> classifiers, Map<String, String> proeprties) {
        this(name, ifNotExists, replaceIfExists, classifiers, proeprties, NodePosition.ZERO);
    }

    public CreateResourceGroupStmt(String name, boolean ifNotExists, boolean replaceIfExists,
                                   List<List<Predicate>> classifiers, Map<String, String> proeprties,
=======
    private final String name;
    private boolean ifNotExists;
    private boolean replaceIfExists;
    private final List<List<Predicate>> classifiers;
    private final Map<String, String> properties;
    private ResourceGroup resourceGroup;

    public CreateResourceGroupStmt(String name, boolean ifNotExists, boolean replaceIfExists,
                                   List<List<Predicate>> classifiers, Map<String, String> properties) {
        this(name, ifNotExists, replaceIfExists, classifiers, properties, NodePosition.ZERO);
    }

    public CreateResourceGroupStmt(String name, boolean ifNotExists, boolean replaceIfExists,
                                   List<List<Predicate>> classifiers, Map<String, String> properties,
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
                                   NodePosition pos) {
        super(pos);
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.replaceIfExists = replaceIfExists;
        this.classifiers = classifiers;
<<<<<<< HEAD
        this.properties = proeprties;
=======
        this.properties = properties;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
<<<<<<< HEAD
        if (resourceGroup.getCpuCoreLimit() == null) {
            throw new SemanticException("property 'cpu_core_limit' is absent");
        }
=======

        if (resourceGroup.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY &&
                (resourceGroup.getExclusiveCpuCores() != null && resourceGroup.getExclusiveCpuCores() > 0)) {
            throw new SemanticException(SHORT_QUERY_SET_EXCLUSIVE_CPU_CORES_ERR_MSG);
        }

        ResourceGroup.validateCpuParameters(resourceGroup.getRawCpuWeight(), resourceGroup.getExclusiveCpuCores());

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
