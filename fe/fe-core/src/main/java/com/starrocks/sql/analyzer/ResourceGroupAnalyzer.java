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

package com.starrocks.sql.analyzer;

import com.google.common.base.Splitter;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterResourceGroupStmt;
import com.starrocks.sql.ast.CreateResourceGroupStmt;
import com.starrocks.sql.ast.DropResourceGroupStmt;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.Predicate;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.system.BackendResourceStat;
import com.starrocks.thrift.TWorkGroupType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.net.util.SubnetUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ResourceGroupAnalyzer {
    // Classifier format
    // 1. user = foobar
    // 2. role = operator
    // 3. query_type in ('select', 'insert')
    // 4. source_ip = "192.168.1.1/24"
    // 5. databases = "db1,db2,db3"
    public static ResourceGroupClassifier convertPredicateToClassifier(List<Predicate> predicates)
            throws SemanticException {
        ResourceGroupClassifier classifier = new ResourceGroupClassifier();
        for (Predicate pred : predicates) {
            if (pred instanceof BinaryPredicate &&
                    ((BinaryPredicate) pred).getOp().equals(BinaryType.EQ)) {
                BinaryPredicate eqPred = (BinaryPredicate) pred;
                Expr lhs = eqPred.getChild(0);
                Expr rhs = eqPred.getChild(1);
                if (!(lhs instanceof SlotRef) || !(rhs instanceof StringLiteral)) {
                    throw new SemanticException("Illegal classifier '" + eqPred.toSql() + "'");
                }
                String key = ((SlotRef) lhs).getColumnName();
                String value = ((StringLiteral) rhs).getValue();
                if (key.equalsIgnoreCase(ResourceGroup.USER)) {
                    if (!ResourceGroupClassifier.USER_PATTERN.matcher(value).matches()) {
                        throw new SemanticException(
                                String.format("Illegal classifier specifier '%s': '%s'", ResourceGroup.USER,
                                        eqPred.toSql()));
                    }
                    classifier.setUser(value);
                } else if (key.equalsIgnoreCase(ResourceGroup.ROLE)) {
                    if (!ResourceGroupClassifier.USE_ROLE_PATTERN.matcher(value).matches()) {
                        throw new SemanticException(
                                String.format("Illegal classifier specifier '%s': '%s'", ResourceGroup.ROLE,
                                        eqPred.toSql()));
                    }
                    classifier.setRole(value);
                } else if (key.equalsIgnoreCase(ResourceGroup.SOURCE_IP)) {
                    SubnetUtils.SubnetInfo subnetInfo = new SubnetUtils(value).getInfo();
                    classifier.setSourceIp(subnetInfo.getCidrSignature());
                } else if (key.equalsIgnoreCase(ResourceGroup.DATABASES)) {
                    List<String> databases = Splitter.on(",").splitToList(value);
                    if (CollectionUtils.isEmpty(databases)) {
                        throw new SemanticException(
                                String.format("Illegal classifier specifier '%s': '%s'", ResourceGroup.DATABASES, eqPred));
                    }

                    List<Long> databaseIds = new ArrayList<>();
                    for (String name : databases) {
                        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(name);
                        if (db == null) {
                            throw new SemanticException(String.format("Specified database not exists: %s", name));
                        }
                        databaseIds.add(db.getId());
                    }
                    classifier.setDatabases(databaseIds);
                } else if (key.equalsIgnoreCase(ResourceGroup.PLAN_CPU_COST_RANGE)) {
                    ResourceGroupClassifier.CostRange planCpuCostRange = ResourceGroupClassifier.CostRange.fromString(value);
                    if (planCpuCostRange == null) {
                        throw new SemanticException(String.format("Illegal classifier specifier '%s': '%s', and "
                                        + ResourceGroupClassifier.CostRange.FORMAT_STR_RANGE_MESSAGE,
                                ResourceGroup.PLAN_CPU_COST_RANGE, eqPred.toSql()));
                    }
                    classifier.setPlanCpuCostRange(planCpuCostRange);
                } else if (key.equalsIgnoreCase(ResourceGroup.PLAN_MEM_COST_RANGE)) {
                    ResourceGroupClassifier.CostRange planMemCostRange = ResourceGroupClassifier.CostRange.fromString(value);
                    if (planMemCostRange == null) {
                        throw new SemanticException(String.format("Illegal classifier specifier '%s': '%s', and "
                                        + ResourceGroupClassifier.CostRange.FORMAT_STR_RANGE_MESSAGE,
                                ResourceGroup.PLAN_MEM_COST_RANGE, eqPred.toSql()));
                    }
                    classifier.setPlanMemCostRange(planMemCostRange);
                } else {
                    throw new SemanticException(String.format("Unsupported classifier specifier: '%s'", key));
                }
            } else if (pred instanceof InPredicate && !((InPredicate) pred).isNotIn()) {
                InPredicate inPred = (InPredicate) pred;
                Expr lhs = inPred.getChild(0);
                List<Expr> rhs = inPred.getListChildren();
                if (!(lhs instanceof SlotRef) || rhs.stream().anyMatch(e -> !(e instanceof StringLiteral))) {
                    throw new SemanticException(
                            String.format("Illegal classifier specifier: '%s'", inPred.toSql()));
                }
                String key = ((SlotRef) lhs).getColumnName();
                if (!key.equalsIgnoreCase(ResourceGroup.QUERY_TYPE)) {
                    throw new SemanticException(String.format("Unsupported classifier specifier: '%s'", key));
                }

                Set<String> values = rhs.stream().map(e -> ((StringLiteral) e).getValue()).collect(Collectors.toSet());
                for (String queryType : values) {
                    if (!ResourceGroupClassifier.SUPPORTED_QUERY_TYPES.contains(queryType.toUpperCase())) {
                        throw new SemanticException(
                                String.format("Unsupported %s: '%s'", ResourceGroup.QUERY_TYPE, queryType));
                    }
                }
                classifier.setQueryTypes(values.stream()
                        .map(String::toUpperCase).map(ResourceGroupClassifier.QueryType::valueOf)
                        .collect(Collectors.toSet()));
            } else {
                throw new SemanticException(String.format("Illegal classifier specifier: '%s'", pred.toSql()));
            }
        }

        if (classifier.getUser() == null &&
                classifier.getRole() == null &&
                (classifier.getQueryTypes() == null || classifier.getQueryTypes().isEmpty()) &&
                classifier.getSourceIp() == null &&
                classifier.getDatabases() == null &&
                classifier.getPlanCpuCostRange() == null &&
                classifier.getPlanMemCostRange() == null) {
            throw new SemanticException("At least one of ('user', 'role', 'query_type', 'db', 'source_ip', " +
                    "'plan_cpu_cost_range', 'plan_mem_cost_range') should be given");
        }
        return classifier;
    }

    // Property format:
    // ('cpu_weight'='n', 'mem_limit'='m%', 'concurrency_limit'='n', 'type'='normal|default|realtime')
    public static void analyzeProperties(ResourceGroup resourceGroup, Map<String, String> properties)
            throws SemanticException {
        final int avgCoreNum = BackendResourceStat.getInstance().getAvgNumHardwareCoresOfBe();
        for (Map.Entry<String, String> e : properties.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            try {
                if (key.equalsIgnoreCase(ResourceGroup.CPU_CORE_LIMIT) || key.equalsIgnoreCase(ResourceGroup.CPU_WEIGHT)) {
                    int cpuWeight = Integer.parseInt(value);
                    if (cpuWeight > avgCoreNum) {
                        throw new SemanticException(
                                String.format("%s should range from 0 to %d", ResourceGroup.CPU_WEIGHT, avgCoreNum));
                    }
                    resourceGroup.setCpuWeight(cpuWeight);
                    continue;
                }
                if (key.equalsIgnoreCase(ResourceGroup.EXCLUSIVE_CPU_CORES)) {
                    final int exclusiveCpuCores = Integer.parseInt(value);
                    final int minCoreNum = BackendResourceStat.getInstance().getMinNumHardwareCoresOfBe();
                    if (exclusiveCpuCores >= minCoreNum) {
                        throw new SemanticException(String.format(
                                "%s cannot exceed the minimum number of CPU cores available on the backends minus one [%d]",
                                ResourceGroup.EXCLUSIVE_CPU_CORES, minCoreNum - 1));
                    }
                    resourceGroup.setExclusiveCpuCores(exclusiveCpuCores);
                    continue;
                }
                if (key.equalsIgnoreCase(ResourceGroup.MAX_CPU_CORES)) {
                    int maxCpuCores = Integer.parseInt(value);
                    if (maxCpuCores > avgCoreNum) {
                        throw new SemanticException(String.format("max_cpu_cores should range from 0 to %d", avgCoreNum));
                    }
                    resourceGroup.setMaxCpuCores(maxCpuCores);
                    continue;
                }
                if (key.equalsIgnoreCase(ResourceGroup.MEM_LIMIT)) {
                    double memLimit;
                    if (value.endsWith("%")) {
                        value = value.substring(0, value.length() - 1);
                        memLimit = Double.parseDouble(value) / 100;
                    } else {
                        memLimit = Double.parseDouble(value);
                    }
                    if (memLimit <= 0.0 || memLimit > 1.0) {
                        throw new SemanticException("mem_limit should range from 0.00(exclude) to 1.00(include)");
                    }
                    resourceGroup.setMemLimit(memLimit);
                    continue;
                }

                if (key.equalsIgnoreCase(ResourceGroup.MEM_POOL)) {
                    resourceGroup.setMemPool(value);
                    continue;
                }

                if (key.equalsIgnoreCase(ResourceGroup.BIG_QUERY_MEM_LIMIT)) {
                    long bigQueryMemLimit = Long.parseLong(value);
                    if (bigQueryMemLimit < 0) {
                        throw new SemanticException("big_query_mem_limit should greater than 0 or equal to 0");
                    }
                    resourceGroup.setBigQueryMemLimit(bigQueryMemLimit);
                    continue;
                }

                if (key.equalsIgnoreCase(ResourceGroup.BIG_QUERY_SCAN_ROWS_LIMIT)) {
                    long bigQueryScanRowsLimit = Long.parseLong(value);
                    if (bigQueryScanRowsLimit < 0) {
                        throw new SemanticException("big_query_scan_rows_limit should greater than 0 or equal to 0");
                    }
                    resourceGroup.setBigQueryScanRowsLimit(bigQueryScanRowsLimit);
                    continue;
                }

                if (key.equalsIgnoreCase(ResourceGroup.BIG_QUERY_CPU_SECOND_LIMIT)) {
                    long bigQueryCpuSecondLimit = Long.parseLong(value);
                    if (bigQueryCpuSecondLimit < 0 || bigQueryCpuSecondLimit > ResourceGroup.MAX_BIG_QUERY_CPU_SECOND_LIMIT) {
                        throw new SemanticException(
                                String.format("The range of `%s` should be (0, %d]", ResourceGroup.BIG_QUERY_CPU_SECOND_LIMIT,
                                        ResourceGroup.MAX_BIG_QUERY_CPU_SECOND_LIMIT));
                    }
                    resourceGroup.setBigQueryCpuSecondLimit(bigQueryCpuSecondLimit);
                    continue;
                }

                if (key.equalsIgnoreCase(ResourceGroup.CONCURRENCY_LIMIT)) {
                    int concurrencyLimit = Integer.parseInt(value);
                    if (concurrencyLimit < 0) {
                        throw new SemanticException("concurrency_limit should be greater than 0");
                    }
                    resourceGroup.setConcurrencyLimit(concurrencyLimit);
                    continue;
                }

                if (key.equalsIgnoreCase(ResourceGroup.SPILL_MEM_LIMIT_THRESHOLD)) {
                    double spillMemLimitThreshold;
                    if (value.endsWith("%")) {
                        value = value.substring(0, value.length() - 1);
                        spillMemLimitThreshold = Double.parseDouble(value) / 100;
                    } else {
                        spillMemLimitThreshold = Double.parseDouble(value);
                    }
                    if (spillMemLimitThreshold <= 0.0 || spillMemLimitThreshold >= 1.0) {
                        throw new SemanticException("spill_mem_limit_threshold should range from 0.00(exclude) to 1.00(exclude)");
                    }
                    resourceGroup.setSpillMemLimitThreshold(spillMemLimitThreshold);
                    continue;
                }

                if (key.equalsIgnoreCase(ResourceGroup.GROUP_TYPE)) {
                    try {
                        resourceGroup.setResourceGroupType(TWorkGroupType.valueOf("WG_" + value.toUpperCase()));
                        if (resourceGroup.getResourceGroupType() != TWorkGroupType.WG_NORMAL &&
                                resourceGroup.getResourceGroupType() != TWorkGroupType.WG_SHORT_QUERY &&
                                resourceGroup.getResourceGroupType() != TWorkGroupType.WG_MV) {
                            throw new SemanticException("Only support 'normal', 'mv' and 'short_query' type");
                        }
                    } catch (Exception ignored) {
                        throw new SemanticException("Only support 'normal', 'mv' and 'short_query' type");
                    }
                    continue;
                }
            } catch (NumberFormatException exception) {
                throw new SemanticException(String.format("The value type of the property `%s` must be a valid numeric type, " +
                        "but it is set to `%s`", e.getKey(), e.getValue()));
            }

            throw new SemanticException("Unknown property: " + key);
        }
    }

    /**
     * Analyze CreateResourceGroupStmt for validation only
     * The actual ResourceGroup construction is now handled by ResourceGroupBuilder
     */
    public static void analyzeCreateResourceGroupStmt(CreateResourceGroupStmt stmt) throws SemanticException {
        // Validate classifiers by converting them (this validates the syntax and format)
        for (List<Predicate> predicates : stmt.getClassifiers()) {
            convertPredicateToClassifier(predicates);
        }

        // Create a temporary ResourceGroup to validate properties
        ResourceGroup tempResourceGroup = new ResourceGroup();
        analyzeProperties(tempResourceGroup, stmt.getProperties());

        // Validate required properties exist
        if (tempResourceGroup.getMemLimit() == null) {
            throw new SemanticException("property 'mem_limit' is absent");
        }

        // Set default type if not specified for validation
        if (tempResourceGroup.getResourceGroupType() == null) {
            tempResourceGroup.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        }

        // Validate short query resource group constraints
        if (tempResourceGroup.getResourceGroupType() == TWorkGroupType.WG_SHORT_QUERY &&
                (tempResourceGroup.getExclusiveCpuCores() != null && tempResourceGroup.getExclusiveCpuCores() > 0)) {
            throw new SemanticException("short query resource group should not set exclusive_cpu_cores");
        }

        // Validate CPU parameters
        ResourceGroup.validateCpuParameters(tempResourceGroup.getRawCpuWeight(), tempResourceGroup.getExclusiveCpuCores());
    }

    /**
     * Analyze AlterResourceGroupStmt for validation only
     * The actual ResourceGroup modification is now handled by ResourceGroupBuilder
     */
    public static void analyzeAlterResourceGroupStmt(AlterResourceGroupStmt stmt) throws SemanticException {
        String name = stmt.getName();
        AlterResourceGroupStmt.SubCommand cmd = stmt.getCmd();

        // Validate classifiers by converting them (this validates the syntax and format)
        if (cmd instanceof AlterResourceGroupStmt.AddClassifiers addClassifiers) {
            for (List<Predicate> predicates : addClassifiers.getClassifiers()) {
                convertPredicateToClassifier(predicates);
            }
        } else if (cmd instanceof AlterResourceGroupStmt.AlterProperties alterProperties) {
            // Create a temporary ResourceGroup to validate properties
            ResourceGroup tempResourceGroup = new ResourceGroup();
            analyzeProperties(tempResourceGroup, alterProperties.getProperties());
            
            // Validate that type cannot be changed
            if (tempResourceGroup.getResourceGroupType() != null) {
                throw new SemanticException("type of ResourceGroup is immutable");
            }
            
            // Validate that at least one property is specified
            if (tempResourceGroup.getRawCpuWeight() == null &&
                    tempResourceGroup.getExclusiveCpuCores() == null &&
                    tempResourceGroup.getMemLimit() == null &&
                    tempResourceGroup.getConcurrencyLimit() == null &&
                    tempResourceGroup.getMaxCpuCores() == null &&
                    tempResourceGroup.getBigQueryCpuSecondLimit() == null &&
                    tempResourceGroup.getBigQueryMemLimit() == null &&
                    tempResourceGroup.getBigQueryScanRowsLimit() == null &&
                    tempResourceGroup.getSpillMemLimitThreshold() == null) {
                throw new SemanticException("At least one of ('cpu_weight','exclusive_cpu_cores','mem_limit'," +
                        "'max_cpu_cores','concurrency_limit','big_query_mem_limit', 'big_query_scan_rows_limit'," +
                        "'big_query_cpu_second_limit','spill_mem_limit_threshold') " +
                        "should be specified");
            }
        }

        // Validate builtin resource group constraints
        if (ResourceGroup.BUILTIN_WG_NAMES.contains(name) && !(cmd instanceof AlterResourceGroupStmt.AlterProperties)) {
            throw new SemanticException(String.format("cannot alter classifiers of builtin resource group [%s]", name));
        }
    }

    public static void analyzeDropResourceGroupStmt(DropResourceGroupStmt stmt) throws SemanticException {
        String name = stmt.getName();
        if (ResourceGroup.BUILTIN_WG_NAMES.contains(name)) {
            throw new SemanticException(String.format("cannot drop builtin resource group [%s]", name));
        }
    }
}
