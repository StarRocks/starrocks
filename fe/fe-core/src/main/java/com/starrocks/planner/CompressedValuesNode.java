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
import com.starrocks.thrift.TCompressedValuesNode;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * PlanNode for efficiently handling large constant lists without parsing individual expressions.
 * This dramatically reduces memory usage and serialization overhead compared to UnionNode.
 * Supports type-specific serialization for better compression.
 */
public class CompressedValuesNode extends PlanNode {
    private static final Logger LOG = LogManager.getLogger(CompressedValuesNode.class);
    
    private final Type valueType;
    private final String rawConstantList;
    private final int constantCount;

    public CompressedValuesNode(PlanNodeId id, TupleId tupleId, 
                               Type valueType, String rawConstantList, int constantCount) {
        super(id, Lists.newArrayList(tupleId), "COMPRESSED_VALUES");
        this.valueType = valueType;
        this.rawConstantList = rawConstantList;
        this.constantCount = constantCount;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.node_type = TPlanNodeType.COMPRESSED_VALUES_NODE;
        msg.compressed_values_node = new TCompressedValuesNode();
        msg.compressed_values_node.tuple_id = tupleIds.get(0).asInt();
        msg.compressed_values_node.value_type = valueType.toThrift();
        msg.compressed_values_node.constant_count = constantCount;

        // Parse and set typed values based on the data type
        parseAndSetTypedValues(msg.compressed_values_node);
    }

    /**
     * Parse raw constant list into typed values for better compression.
     * @param node The TCompressedValuesNode to populate
     */
    private void parseAndSetTypedValues(TCompressedValuesNode node) {
        String[] constantStrings = rawConstantList.split(",");
        PrimitiveType primitiveType = valueType.getPrimitiveType();
        
        switch (primitiveType) {
            case BIGINT:
            case INT:
            case SMALLINT:
            case TINYINT:
                parseAndSetLongValues(node, constantStrings);
                break;
                
            case VARCHAR:
            case CHAR:
                parseAndSetStringValues(node, constantStrings);
                break;
                
            default:
                throw new IllegalArgumentException("Unsupported type for CompressedValuesNode: " + primitiveType);
        }
    }

    /**
     * Parse constants as long values for integer types.
     */
    private void parseAndSetLongValues(TCompressedValuesNode node, String[] constantStrings) {
        List<Long> longValues = new ArrayList<>(constantStrings.length);
        
        for (String constantStr : constantStrings) {
            String trimmed = constantStr.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            
            try {
                longValues.add(Long.parseLong(trimmed));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Failed to parse '" + trimmed + "' as long value: " + e.getMessage());
            }
        }
        
        node.long_values = longValues;
        LOG.debug("Successfully parsed {} long values for type {}", longValues.size(), valueType.getPrimitiveType());
    }

    /**
     * Parse constants as string values for string types.
     */
    private void parseAndSetStringValues(TCompressedValuesNode node, String[] constantStrings) {
        List<String> stringValues = new ArrayList<>(constantStrings.length);
        
        for (String constantStr : constantStrings) {
            String trimmed = constantStr.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            
            // Remove quotes if present
            String strValue = trimmed;
            if ((strValue.startsWith("'") && strValue.endsWith("'")) ||
                (strValue.startsWith("\"") && strValue.endsWith("\""))) {
                strValue = strValue.substring(1, strValue.length() - 1);
            }
            
            stringValues.add(strValue);
        }
        
        node.string_values = stringValues;
        LOG.debug("Successfully parsed {} string values for type {}", stringValues.size(), valueType.getPrimitiveType());
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        StringBuilder output = new StringBuilder();
        output.append(prefix).append("COMPRESSED VALUES\n");
        output.append(prefix).append("constant count: ").append(constantCount).append("\n");
        output.append(prefix).append("value type: ").append(valueType.toString()).append("\n");
        
        // Show compression type
        PrimitiveType primitiveType = valueType.getPrimitiveType();
        String compressionType = getCompressionType(primitiveType);
        output.append(prefix).append("compression: ").append(compressionType).append("\n");
        
        if (detailLevel == TExplainLevel.VERBOSE) {
            String sample = rawConstantList.length() > 100 ? 
                           rawConstantList.substring(0, 97) + "..." : rawConstantList;
            output.append(prefix).append("sample values: ").append(sample).append("\n");
        }
        
        return output.toString();
    }

    /**
     * Get the compression type description for explain output.
     */
    private String getCompressionType(PrimitiveType primitiveType) {
        switch (primitiveType) {
            case BIGINT:
            case INT:
            case SMALLINT:
            case TINYINT:
                return "typed_long_list";
            case VARCHAR:
            case CHAR:
                return "typed_string_list";
            default:
                return "unsupported_type";
        }
    }

    public Type getValueType() {
        return valueType;
    }

    public String getRawConstantList() {
        return rawConstantList;
    }

    public int getConstantCount() {
        return constantCount;
    }
}