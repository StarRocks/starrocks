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

import com.google.common.collect.Lists;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.TypeSerializer;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TNormalRawValuesNode;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TRawValuesNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * RawValuesNode is a PlanNode optimized for efficiently handling large constant lists 
 * in the execution plan, specifically designed for LargeInPredicate optimization.
 * 
 * <p><b>Problem:</b>
 * Traditional UnionNode parses and stores each constant as a separate expression object,
 * which creates significant memory and serialization overhead when dealing with hundreds of thousands 
 * of constant values (e.g., WHERE id IN (1, 2, 3, ..., 100000)).
 * 
 * <p><b>Solution:</b>
 * RawValuesNode stores constant values in their raw text form and uses type-specific optimized 
 * serialization:
 * <ul>
 *   <li>For integer types (TINYINT/SMALLINT/INT/BIGINT): serialized as {@code List<Long>}</li>
 *   <li>For string types (VARCHAR/CHAR): serialized as {@code List<String>}</li>
 * </ul>
 * 
 * <p>This approach dramatically reduces:
 * <ul>
 *   <li>Memory usage during planning (no AST nodes for each constant)</li>
 *   <li>Serialization overhead when sending plan to BE</li>
 *   <li>Deserialization time in BE</li>
 * </ul>
 * 
 * <p><b>Usage Context:</b>
 * RawValuesNode is created by {@link com.starrocks.sql.optimizer.rule.transformation.LargeInPredicateToJoinRule}
 * when transforming LargeInPredicate to Left semi/anti join. It serves as the build side of the join,
 * providing the constant values for matching.
 * 
 * <p><b>Supported Types:</b>
 * <ol>
 *   <li>Integer types: TINYINT, SMALLINT, INT, BIGINT</li>
 *   <li>String types: VARCHAR, CHAR</li>
 * </ol>
 * 
 * <p><b>BE Execution:</b>
 * In BE, {@code RawValuesNode} directly constructs columns from typed arrays without 
 * expression evaluation, providing significant performance improvements.
 * 
 * @see com.starrocks.sql.optimizer.operator.logical.LogicalRawValuesOperator
 * @see com.starrocks.sql.optimizer.operator.physical.PhysicalRawValuesOperator
 * @see com.starrocks.sql.ast.expression.LargeInPredicate
 */
public class RawValuesNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(RawValuesNode.class);

    private final String rawText;
    private final Type constantType;
    private final List<Object> rawConstantList;
    private final int constantCount;

    public RawValuesNode(PlanNodeId id, TupleId tupleId, Type constantType,
                         String rawText, List<Object> rawConstantList, int constantCount) {
        super(id, Lists.newArrayList(tupleId), "RAW_VALUES");
        this.constantType = constantType;
        this.rawText = rawText;
        this.rawConstantList = rawConstantList;
        this.constantCount = constantCount;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.RAW_VALUES_NODE;
        msg.raw_values_node = new TRawValuesNode();
        msg.raw_values_node.tuple_id = tupleIds.get(0).asInt();
        msg.raw_values_node.constant_type = TypeSerializer.toThrift(constantType);
        PrimitiveType primitiveType = constantType.getPrimitiveType();

        if (primitiveType.isIntegerType()) {
            msg.raw_values_node.setLong_values((List<Long>) (List<?>) rawConstantList);
        } else if (primitiveType.isCharFamily()) {
            msg.raw_values_node.setString_values((List<String>) (List<?>) rawConstantList);;
        } else {
            throw new UnsupportedOperationException("Unsupported type for RawValuesNode: " + primitiveType);
        }
    }


    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("RAW VALUES\n");
        output.append(prefix).append("constant count: ").append(constantCount).append("\n");
        output.append(prefix).append("constant type: ").append(constantType.toString()).append("\n");

        if (detailLevel == TExplainLevel.VERBOSE) {
            String sample = rawText.length() > 100 ?
                           rawText.substring(0, 97) + "..." : rawText;
            output.append(prefix).append("sample values: ").append(sample).append("\n");
        }
        
        return output.toString();
    }

    @Override
    public boolean canUseRuntimeAdaptiveDop() {
        return true;
    }

    @Override
    protected void toNormalForm(TNormalPlanNode planNode, FragmentNormalizer normalizer) {
        TNormalRawValuesNode rawValuesNode = new TNormalRawValuesNode();
        
        rawValuesNode.setTuple_id(normalizer.remapTupleId(tupleIds.get(0)).asInt());
        rawValuesNode.setConstant_type_desc(constantType.toSql());

        PrimitiveType primitiveType = constantType.getPrimitiveType();
        if (primitiveType.isIntegerType()) {
            rawValuesNode.setLong_values((List<Long>) (List<?>) rawConstantList);
        } else if (primitiveType.isCharFamily()) {
            rawValuesNode.setString_values((List<String>) (List<?>) rawConstantList);
        } else {
            throw new UnsupportedOperationException("Unsupported type for RawValuesNode: " + primitiveType);
        }
        
        rawValuesNode.setConstant_count(constantCount);
        planNode.setNode_type(TPlanNodeType.RAW_VALUES_NODE);
        planNode.setRaw_values_node(rawValuesNode);
        normalizeConjuncts(normalizer, planNode, conjuncts);
        
        super.toNormalForm(planNode, normalizer);
    }
}