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

package com.starrocks.catalog;

import com.starrocks.sql.analyzer.ResourceGroupAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.thrift.TWorkGroupType;

import java.util.ArrayList;
import java.util.List;

/**
 * ResourceGroupBuilder is responsible for building ResourceGroup objects and handling ResourceGroup modifications.
 * It separates the construction logic from validation logic in ResourceGroupAnalyzer.
 */
public class ResourceGroupBuilder {

    /**
     * Build a ResourceGroup from CreateResourceGroupStmt
     *
     * @param stmt the CreateResourceGroupStmt containing all parameters
     * @return constructed ResourceGroup object
     * @throws SemanticException if building fails
     */
    public static ResourceGroup buildFromStmt(CreateResourceGroupStmt stmt) {
        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.setName(stmt.getName());

        // Build classifiers from predicates
        List<ResourceGroupClassifier> classifierList = new ArrayList<>();
        for (List<Predicate> predicates : stmt.getClassifiers()) {
            ResourceGroupClassifier classifier = ResourceGroupAnalyzer.convertPredicateToClassifier(predicates);
            classifierList.add(classifier);
        }
        resourceGroup.setClassifiers(classifierList);

        // Apply properties to resource group
        ResourceGroupAnalyzer.analyzeProperties(resourceGroup, stmt.getProperties());

        // Set default type if not specified
        if (resourceGroup.getResourceGroupType() == null) {
            resourceGroup.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        }

        // Validate short query resource group constraints
        if (resourceGroup.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY &&
                (resourceGroup.getExclusiveCpuCores() != null && resourceGroup.getExclusiveCpuCores() > 0)) {
            throw new SemanticException("short query resource group should not set exclusive_cpu_cores");
        }

        // Validate CPU parameters
        ResourceGroup.validateCpuParameters(resourceGroup.getRawCpuWeight(), resourceGroup.getExclusiveCpuCores());

        // Validate required properties
        if (resourceGroup.getMemLimit() == null) {
            throw new SemanticException("property 'mem_limit' is absent");
        }

        return resourceGroup;
    }

    /**
     * Build new classifiers from AlterResourceGroupStmt for ADD operation
     *
     * @param stmt the AlterResourceGroupStmt containing classifier predicates
     * @return List of built ResourceGroupClassifier objects
     * @throws SemanticException if building fails
     */
    public static List<ResourceGroupClassifier> buildAddedClassifiersFromStmt(AlterResourceGroupStmt stmt)
            throws SemanticException {
        if (!(stmt.getCmd() instanceof AlterResourceGroupStmt.AddClassifiers)) {
            return new ArrayList<>();
        }

        AlterResourceGroupStmt.AddClassifiers addClassifiers = (AlterResourceGroupStmt.AddClassifiers) stmt.getCmd();
        List<ResourceGroupClassifier> classifierList = new ArrayList<>();
        for (List<Predicate> predicates : addClassifiers.getClassifiers()) {
            ResourceGroupClassifier classifier = ResourceGroupAnalyzer.convertPredicateToClassifier(predicates);
            classifierList.add(classifier);
        }
        return classifierList;
    }

    /**
     * Build changed properties ResourceGroup from AlterResourceGroupStmt for ALTER PROPERTIES operation
     *
     * @param stmt the AlterResourceGroupStmt containing property changes
     * @return ResourceGroup object with only changed properties set
     * @throws SemanticException if building fails
     */
    public static ResourceGroup buildChangedPropertiesFromStmt(AlterResourceGroupStmt stmt) throws SemanticException {
        ResourceGroup changedProperties = new ResourceGroup();

        if (!(stmt.getCmd() instanceof AlterResourceGroupStmt.AlterProperties)) {
            return changedProperties;
        }

        AlterResourceGroupStmt.AlterProperties alterProperties = (AlterResourceGroupStmt.AlterProperties) stmt.getCmd();
        ResourceGroupAnalyzer.analyzeProperties(changedProperties, alterProperties.getProperties());

        // Validate that type cannot be changed
        if (changedProperties.getResourceGroupType() != null) {
            throw new SemanticException("type of ResourceGroup is immutable");
        }

        // Validate that at least one property is specified
        if (changedProperties.getRawCpuWeight() == null &&
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

        return changedProperties;
    }
}
