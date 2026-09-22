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

package com.starrocks.planner;

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.plan.HDFSScanNodePredicates;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.type.Type;

import java.util.List;

/** Shared formatting for external scan nodes that use HDFS scan predicates. */
final class ScanNodeExplainHelper {
    private ScanNodeExplainHelper() {
    }

    static void appendPredicates(StringBuilder output, String prefix, ScanNode node,
                                 HDFSScanNodePredicates predicates) {
        appendPredicate(output, prefix, node, "PARTITION PREDICATES", predicates.getPartitionConjuncts());
        appendPredicate(output, prefix, node, "NON-PARTITION PREDICATES", predicates.getNonPartitionConjuncts());
        appendPredicate(output, prefix, node, "NO EVAL-PARTITION PREDICATES", predicates.getNoEvalPartitionConjuncts());
        appendPredicate(output, prefix, node, "MIN/MAX PREDICATES", predicates.getMinMaxConjuncts());
    }

    private static void appendPredicate(StringBuilder output, String prefix, ScanNode node,
                                        String label, List<Expr> predicates) {
        if (!predicates.isEmpty()) {
            // Preserve the node's formatter, including session-controlled EXPLAIN desensitization.
            output.append(prefix).append(label).append(": ").append(node.explainExpr(predicates)).append("\n");
        }
    }

    static void appendStatistics(StringBuilder output, String prefix, TExplainLevel detailLevel, ScanNode node) {
        if (detailLevel != TExplainLevel.VERBOSE) {
            output.append(prefix).append(String.format("cardinality=%s\n", node.getCardinality()));
        }
        output.append("\n");
        output.append(prefix).append(String.format("avgRowSize=%s\n", node.getAvgRowSize()));
    }

    static void appendPrunedTypes(StringBuilder output, String prefix, TupleDescriptor tuple) {
        for (SlotDescriptor slot : tuple.getSlots()) {
            Type type = slot.getOriginType();
            if (type.isComplexType()) {
                output.append(prefix).append(String.format("Pruned type: %d <-> [%s]\n", slot.getId().asInt(), type));
            }
        }
    }
}
