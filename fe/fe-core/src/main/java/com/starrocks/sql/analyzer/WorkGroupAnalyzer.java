// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.analyzer;

import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.Predicate;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.WorkGroup;
import com.starrocks.catalog.WorkGroupClassifier;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.system.BackendCoreStat;
import com.starrocks.thrift.TWorkGroupType;
import org.apache.commons.net.util.SubnetUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkGroupAnalyzer {
    // Classifier format
    // 1. user = foobar
    // 2. role = operator
    // 3. query_type in ('select', 'insert')
    // 4. source_ip = "192.168.1.1/24"
    public static WorkGroupClassifier convertPredicateToClassifier(List<Predicate> predicates) throws SemanticException {
        WorkGroupClassifier classifier = new WorkGroupClassifier();
        for (Predicate pred : predicates) {
            if (pred instanceof BinaryPredicate && ((BinaryPredicate) pred).getOp().equals(BinaryPredicate.Operator.EQ)) {
                BinaryPredicate eqPred = (BinaryPredicate) pred;
                Expr lhs = eqPred.getChild(0);
                Expr rhs = eqPred.getChild(1);
                if (!(lhs instanceof SlotRef) || !(rhs instanceof StringLiteral)) {
                    throw new SemanticException("Illegal classifier '" + eqPred.toSql() + "'");
                }
                String key = ((SlotRef) lhs).getColumnName();
                String value = ((StringLiteral) rhs).getValue();
                if (key.equalsIgnoreCase(WorkGroup.USER)) {
                    if (!WorkGroupClassifier.UseRolePattern.matcher(value).matches()) {
                        throw new SemanticException(
                                String.format("Illegal classifier specifier '%s': '%s'", WorkGroup.USER, eqPred.toSql()));
                    }
                    classifier.setUser(value);
                } else if (key.equalsIgnoreCase(WorkGroup.ROLE)) {
                    if (!WorkGroupClassifier.UseRolePattern.matcher(value).matches()) {
                        throw new SemanticException(
                                String.format("Illegal classifier specifier '%s': '%s'", WorkGroup.ROLE, eqPred.toSql()));
                    }
                    classifier.setRole(value);
                } else if (key.equalsIgnoreCase(WorkGroup.SOURCE_IP)) {
                    SubnetUtils.SubnetInfo subnetInfo = new SubnetUtils(value).getInfo();
                    classifier.setSourceIp(subnetInfo.getCidrSignature());
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
                if (!key.equalsIgnoreCase(WorkGroup.QUERY_TYPE)) {
                    throw new SemanticException(String.format("Unsupported classifier specifier: '%s'", key));
                }

                Set<String> values = rhs.stream().map(e -> ((StringLiteral) e).getValue()).collect(Collectors.toSet());
                for (String queryType : values) {
                    if (!WorkGroupClassifier.SUPPORTED_QUERY_TYPES.contains(queryType.toUpperCase())) {
                        throw new SemanticException(
                                String.format("Unsupported %s: '%s'", WorkGroup.QUERY_TYPE, queryType));
                    }
                }
                classifier.setQueryTypes(values.stream()
                        .map(String::toUpperCase).map(WorkGroupClassifier.QueryType::valueOf).collect(Collectors.toSet()));
            } else {
                throw new SemanticException(String.format("Illegal classifier specifier: '%s'", pred.toSql()));
            }
        }

        if (classifier.getUser() == null &&
                classifier.getRole() == null &&
                (classifier.getQueryTypes() == null || classifier.getQueryTypes().isEmpty())
                && classifier.getSourceIp() == null) {
            throw new SemanticException("At least one of ('user', 'role', 'query_type', 'source_ip') should be given");
        }
        return classifier;
    }

    // Property format:
    // ('cpu_core_limit'='n', 'mem_limit'='m%', 'concurrency_limit'='n', 'type'='normal|default|realtime')
    public static void analyzeProperties(WorkGroup workgroup, Map<String, String> properties) throws SemanticException {
        for (Map.Entry<String, String> e : properties.entrySet()) {
            String key = e.getKey();
            String value = e.getValue();
            if (key.equalsIgnoreCase(WorkGroup.CPU_CORE_LIMIT)) {
                int cpuCoreLimit = Integer.parseInt(value);
                int avgCoreNum = BackendCoreStat.getAvgNumOfHardwareCoresOfBe();
                if (cpuCoreLimit <= 0 || cpuCoreLimit > avgCoreNum) {
                    throw new SemanticException(String.format("cpu_core_limit should range from 1 to %d", avgCoreNum));
                }
                workgroup.setCpuCoreLimit(Integer.parseInt(value));
                continue;
            }
            if (key.equalsIgnoreCase(WorkGroup.MEM_LIMIT)) {
                double memLimit;
                if (value.endsWith("%")) {
                    value = value.substring(0, value.length() - 1);
                    memLimit = Double.parseDouble(value) / 100;
                } else {
                    memLimit = Double.parseDouble(value);
                }
                if (memLimit <= 0.0 || memLimit >= 1.0) {
                    throw new SemanticException("mem_limit should range from 0.00(exclude) to 1.00(exclude)");
                }
                workgroup.setMemLimit(memLimit);
                continue;
            }
            if (key.equalsIgnoreCase(WorkGroup.CONCURRENCY_LIMIT)) {
                int concurrencyLimit = Integer.parseInt(value);
                if (concurrencyLimit <= 0) {
                    throw new SemanticException("concurrency_limit should be greater than 0");
                }
                workgroup.setConcurrencyLimit(concurrencyLimit);
                continue;
            }
            if (key.equalsIgnoreCase(WorkGroup.WORKGROUP_TYPE)) {
                try {
                    workgroup.setWorkGroupType(TWorkGroupType.valueOf("WG_" + value.toUpperCase()));
                    if (workgroup.getWorkGroupType() != TWorkGroupType.WG_NORMAL) {
                        throw new SemanticException("Only support 'normal' type");
                    }
                } catch (Exception ignored) {
                    throw new SemanticException("Only support 'normal' type");
                }
                continue;
            }

            throw new SemanticException("Unknown property: " + key);
        }
    }
}
